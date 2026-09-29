# Rapport — Industrialisation d'une pipeline Big Data (CI/CD)

## 1. Architecture du projet

La plateforme traite des commandes de vente en temps réel :

```text
Client ──HTTP POST──▶ FastAPI (sales-api)
                          │ événement JSON
                          ▼
                     Kafka (topic sales.orders)
                          │ Structured Streaming
                          ▼
                     PySpark (spark-streaming)
                          │ JDBC
                          ▼
                     PostgreSQL (table processed_orders)
```

| Composant | Rôle | Fichier / service |
|---|---|---|
| Sales API (FastAPI) | Valide la commande (Pydantic), génère `order_id`, calcule `total_amount`, publie dans Kafka | `app/main.py`, service `sales-api` |
| Kafka | Transporte les événements de commande | topic `sales.orders`, service `kafka` |
| PySpark | Consomme Kafka, filtre les événements invalides, écrit en base | `spark/sales_stream.py`, service `spark-streaming` |
| PostgreSQL | Stocke les commandes traitées | `infra/postgres/init.sql`, table `processed_orders` |
| Jenkins | Orchestre la chaîne CI/CD | `Jenkinsfile`, service `jenkins` |
| SonarQube | Analyse la qualité du code, applique le Quality Gate | `sonar-project.properties`, service `sonarqube` |

Tous les services tournent via `docker-compose.yml` sur un réseau commun `data-platform`, ce qui permet à Jenkins de joindre Kafka, PostgreSQL, l'API et SonarQube par leur nom de service.

## 2. Stratégie de tests

Trois niveaux de tests, du plus rapide et isolé au plus complet :

| Niveau | Ce qui est testé | Dépendances | Durée | Activation |
|---|---|---|---|---|
| Unitaire | Règles métier et endpoints de l'API, Kafka simulé (mock) | Aucune | ~1 s | Toujours |
| Intégration | Deux composants réels ensemble : API → Kafka, puis Kafka → Spark → PostgreSQL | Kafka, Spark, PostgreSQL | ~5 s | `RUN_INTEGRATION_TESTS=true` |
| E2E | Toute la chaîne, vue de l'extérieur, via HTTP | Toute l'infrastructure | ~3 s | `RUN_E2E_TESTS=true` |

Pourquoi trois niveaux :
- Les tests **unitaires** localisent précisément une erreur de logique (calcul, validation) et s'exécutent sans infrastructure. Ce sont eux qui produisent la couverture de code.
- Les tests **d'intégration** vérifient les contrats entre composants (format du message Kafka, schéma Spark, écriture JDBC), que les mocks ne peuvent pas garantir.
- Le test **E2E** vérifie le comportement métier attendu par l'utilisateur : une commande envoyée à l'API finit correctement enregistrée en base.

Les tests d'intégration et E2E sont désactivés par défaut (`pytest.mark.skipif`) pour pouvoir lancer `pytest` sur un poste sans infrastructure.

## 3. Tests unitaires réalisés

Fichier : `tests/unit/test_orders.py` — **42 tests, couverture 100 % de `app/`**.

| Règle métier | Tests |
|---|---|
| Calcul du montant total | Plusieurs couples quantité × prix (paramétrés), arrondi à 2 décimales |
| Quantité | Positive acceptée (1, 5, 1000) ; nulle refusée ; négative refusée ; > 1000 refusée |
| Prix | Positif accepté (0,01 ; 49,90 ; 100 000) ; 0, négatif, > 100 000 et non numérique refusés |
| Identifiant client | Valide accepté ; vide, 1 caractère ou absent refusé |
| Identifiant produit | Valide accepté ; vide, 1 caractère ou absent refusé |
| Identifiant de commande | Format `ORD-` + 10 caractères hexadécimaux ; unicité sur 100 générations |
| Construction de l'événement | Champs attendus exactement ; données recopiées ; timestamp ISO 8601 en UTC ; événement valide selon `OrderEvent` |
| Endpoints API | `/api/health`, `/api/products`, `POST /api/orders` (201 avec Kafka mocké, 404 produit inconnu), création du producteur Kafka mockée |

Les validations invalides sont vérifiées avec `pydantic.ValidationError` (et non `Exception`), pour qu'un test ne passe pas à cause d'une erreur sans rapport.

## 4. Tests d'intégration réalisés

Fichier : `tests/integration/test_api.py`.

**Test A — API → Kafka (`test_api_kafka`)**
1. Envoi d'une commande à `POST /api/orders`.
2. Un `KafkaConsumer` lit le topic `sales.orders` jusqu'à trouver l'`order_id` renvoyé par l'API (timeout 60 s).
3. Vérification du contenu du message : client, produit, quantité, prix, `total_amount = 200.0`.

Complété par `test_api_unknown_product_does_not_produce_event` (produit inconnu → 404) et `test_health`.

**Test B — Kafka → Spark → PostgreSQL (`test_kafka_spark_postgresql`)**
1. Publication directe dans Kafka d'un événement avec un `order_id` unique (`ORD-IT…`).
2. Polling de la table `processed_orders` jusqu'à apparition de la ligne (timeout 60 s).
3. Vérification des colonnes : client, produit, quantité, prix, montant, timestamp.

Ce test isole la partie Spark : il ne dépend pas de l'API.

La configuration de connexion passe par variables d'environnement (`KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_HOST`, `POSTGRES_PORT`…) pour que le même test fonctionne depuis le poste (`localhost`) et depuis Jenkins (`kafka:29092`, `postgres:5432`).

## 5. Scénario E2E

Fichier : `tests/e2e/test_sales_pipeline.py` — `test_order_flows_from_api_to_postgresql`.

| Étape | Action |
|---|---|
| **Given** | L'infrastructure est démarrée : `GET /api/health` répond `UP` |
| **When** | `POST /api/orders` avec `{"customer_id": "C100", "product_id": "P001", "quantity": 3, "unit_price": 100}` → 201 |
| **Then** | La commande apparaît dans `processed_orders` avec les bonnes valeurs et `total_amount = 300` |

L'appel se fait en HTTP réel (`httpx`) vers le conteneur de l'API : le test traverse réellement API → Kafka → Spark → PostgreSQL.

**Attente sans `sleep` arbitraire** : la fonction `wait_until` interroge PostgreSQL toutes les 2 secondes et rend la main dès que la ligne existe. Si le délai maximal (90 s) est dépassé, le test échoue avec un message explicite (`Commande ORD-… absente de processed_orders après 90s`). La connexion a elle-même un `connect_timeout` pour ne jamais bloquer indéfiniment.

## 6. Pipeline Jenkins

Le job `Sales_new` est de type *Pipeline script from SCM* : Jenkins récupère le `Jenkinsfile` depuis le dépôt Git (branche `master`), ce qui versionne la pipeline avec le code.

| Étape | Commande | Contrôle apporté |
|---|---|---|
| Checkout | `checkout scm` | Build sur la version exacte poussée |
| Environment | `python3 --version`, `docker --version` | Outils disponibles |
| Install | `pip install -r requirements.txt` | Dépendances installables et figées |
| Unit Tests | `pytest tests/unit --cov=app` → `coverage.xml`, `test-results-unit.xml` | Pas de régression de logique métier |
| Integration Tests | `pytest tests/integration` | Contrats entre API, Kafka, Spark et PostgreSQL respectés |
| Build | `docker compose build sales-api spark-streaming` | Les images se construisent |
| E2E Tests | `pytest tests/e2e` | Le flux complet fonctionne |
| SonarQube | `sonar-scanner` via `withSonarQubeEnv('SonarQube')` | Analyse qualité et couverture |
| Quality Gate | `waitForQualityGate abortPipeline: true` (timeout 5 min) | Arrêt si la qualité est insuffisante |

Chaque étape est bloquante : un échec arrête la pipeline et les étapes suivantes sont ignorées. En fin de build, Jenkins publie les rapports JUnit (`test-results-*.xml`) et archive `coverage.xml`.

Variables d'environnement définies dans le `Jenkinsfile` : `RUN_INTEGRATION_TESTS`, `RUN_E2E_TESTS`, `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`, `POSTGRES_HOST=postgres`, `POSTGRES_PORT=5432`, `API_URL=http://sales-api:8000`.

## 7. Configuration SonarQube

`sonar-project.properties` :

```properties
sonar.projectKey=real-time-sales-devops
sonar.sources=app
sonar.tests=tests
sonar.python.version=3.11
sonar.python.coverage.reportPaths=coverage.xml
```

Mise en place :
- **Jenkins** : plugin *SonarQube Scanner* ; serveur `SonarQube` (`http://sonarqube:9000`) avec un token stocké dans les *Credentials* (`sonar-token`) ; outil `SonarScanner` installé automatiquement depuis Maven Central.
- **SonarQube** : webhook `http://jenkins:8080/sonarqube-webhook/`, qui notifie Jenkins à la fin de l'analyse (nécessaire à `waitForQualityGate`).
- **Couverture** : `.coveragerc` avec `relative_files = True`, pour que `coverage.xml` contienne des chemins relatifs (`app/…`) que le scanner sait relier aux sources.
- **Analyse manuelle** : service `sonar-scanner` (profil `tools`) dans `docker-compose.yml`, lancé avec `docker compose run --rm sonar-scanner` ; le token est lu depuis `.env` (non versionné).

## 8. Quality Gate

Quality Gate personnalisé, appliqué au projet, conditions sur le code global :

| Condition | Seuil d'échec |
|---|---|
| Coverage | < 80 % |
| Reliability issues (bugs) | > 0 |
| Security issues (vulnerabilities) | > 0 |
| Duplicated lines | > 3 |
| New issues | > 0 |

Fonctionnement dans la CI : après l'analyse, SonarQube calcule le statut du gate et l'envoie à Jenkins par webhook. `waitForQualityGate abortPipeline: true` fait échouer le build si le statut est `ERROR`.

```text
SonarQube ─▶ Quality Gate ─┬─ OK    ─▶ pipeline SUCCESS
                           └─ ERROR ─▶ pipeline FAILURE (arrêt)
```

Le seuil de 80 % de couverture impose de tester tout nouveau code ; 0 bug et 0 vulnérabilité empêchent l'introduction de défauts connus ; la limite de duplication évite le copier-coller.

## 9. Résultats obtenus

**Pipeline Jenkins** : toutes les étapes au vert, `Finished: SUCCESS`.

| Étape | Résultat |
|---|---|
| Tests unitaires | 42 / 42 passés |
| Tests d'intégration | 4 / 4 passés |
| Build | Images `sales-api` et `spark-streaming` construites |
| Test E2E | 1 / 1 passé |
| Quality Gate | **Passed** |

**SonarQube — première analyse** :

| Mesure | Valeur | Note |
|---|---|---|
| Coverage | 100 % | — |
| Bugs | 0 | A |
| Vulnerabilities | 0 | A |
| Security Hotspots | 0 | — |
| Code Smells | 2 | A |
| Duplications | 0 % | — |
| Lignes de code | 63 | — |

Code smells détectés :

| Catégorie | Fichier | Règle | Problème |
|---|---|---|---|
| Code Smell (Major) | `app/main.py` | python:S8415 | La réponse HTTP 404 de `POST /api/orders` n'est pas documentée dans `responses` |
| Code Smell (Major) | `tests/unit/test_orders.py` | python:S5958 | `pytest.raises(Exception)` est trop large |

Aucun bug, aucune vulnérabilité, aucune duplication : les corrections ont porté sur ces deux code smells (voir section 11).

## 10. Problèmes rencontrés

| Problème | Cause | Solution |
|---|---|---|
| `pytest: unrecognized arguments: --cov` | Plugin `pytest-cov` absent ; `\` de continuation bash non reconnu sous Windows (`rootdir: C:\`) | Installation de `pytest-cov` ; commande sur une ligne ou `^` / `` ` `` sous Windows |
| Couverture à 71 % | Endpoints et création du producteur Kafka non testés | Tests unitaires des endpoints avec `TestClient` et Kafka mocké → 100 % |
| `password authentication failed for user "sales"` | Un PostgreSQL Windows local occupait aussi le port 5432 | Conteneur publié sur le port hôte 5433 |
| `checkout scm is only available…` | Job Jenkins configuré en *Pipeline script* | Passage en *Pipeline script from SCM* |
| `externally-managed-environment` (pip) | Python Debian protégé (PEP 668) | `pip install --user --break-system-packages` |
| `No module named 'flask'` | Import `from flask import app` ajouté par erreur par l'IDE | Suppression de l'import |
| Erreur de syntaxe Groovy | Virgule entre deux variables du bloc `environment` | Suppression de la virgule |
| `No module named 'kafka.vendor.six.moves'` | `kafka-python` 2.0.2 incompatible avec Python 3.12+ | Passage à `kafka-python` 2.3.0 |
| `docker: 'compose' is not a docker command` | Paquet Debian `docker.io` sans plugin Compose | Installation du plugin Compose v2 dans l'image Jenkins |
| `sonar-scanner: not found` | Outil déclaré dans *SonarScanner for MSBuild* (.NET) | Déclaration dans *SonarQube Scanner installations* |
| `No SonarRunnerInstallation named SonarScanner` | Nom saisi `Sonar Scanner` (avec espace) | Nom corrigé en `SonarScanner` |
| Chemins de couverture non résolus | `coverage.xml` avec chemins Windows absolus | `.coveragerc` avec `relative_files = True` |
| Token SonarQube versionné | Token écrit en clair dans `docker-compose.yml` | Déplacé dans `.env` (ignoré par Git) ; token révoqué et régénéré |

## 11. Corrections réalisées

**Qualité du code (suite à SonarQube)** :
- `app/main.py` : ajout de `responses={404: {"description": "Unknown product"}}` sur `POST /api/orders`. La documentation OpenAPI décrit maintenant la réponse 404 (règle S8415).
- `tests/unit/test_orders.py` : `pytest.raises(Exception)` remplacé par `pytest.raises(ValidationError)`. Le test ne peut plus passer à cause d'une autre erreur (règle S5958).

**Sécurité** : suppression du token SonarQube en clair dans `docker-compose.yml`, remplacé par `${SONAR_TOKEN}` lu depuis `.env`.

**Tests et CI** : ajout des tests unitaires, d'intégration et E2E ; rapports JUnit publiés dans Jenkins ; correction des dépendances et de l'image Jenkins.

## 12. Synthèse

**Comment garantir automatiquement qu'une modification ne dégrade ni le fonctionnement ni la qualité d'une pipeline Data ?**

Chaque `git push` déclenche la pipeline Jenkins, qui applique une série de contrôles bloquants :

```text
Développer → Commit → Push → Jenkins → Tests unitaires → Tests d'intégration
          → Build → E2E → SonarQube → Quality Gate → Validation
```

- Les **tests unitaires** détectent immédiatement une erreur de logique (ex. un calcul `quantity + unit_price` au lieu de `quantity * unit_price` casse la pipeline dès cette étape).
- Les **tests d'intégration** détectent une rupture de contrat entre composants (format de message, schéma Spark, écriture en base).
- Le **test E2E** garantit que le résultat métier final reste correct sur toute la chaîne.
- **SonarQube** mesure objectivement la qualité (couverture, bugs, vulnérabilités, code smells, duplications).
- Le **Quality Gate** transforme ces mesures en décision : une baisse de couverture sous 80 % ou un nouveau bug fait échouer la pipeline, même si tous les tests passent.
- Le **dépôt Git** centralise le code et versionne la pipeline elle-même (`Jenkinsfile`). Avec des branches et des merge requests, seul du code validé par la pipeline est intégré.

Une modification n'est donc acceptée que si elle passe tous les niveaux de tests **et** respecte les critères de qualité : la validation est automatique, reproductible, et ne dépend pas d'une vérification manuelle.
