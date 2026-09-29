# TP — Industrialisation d’une Pipeline Big Data avec CI/CD

**Formation :** Master Data Engineering  
**Niveau :** Master  
**Thématique :** DevOps, Tests logiciels et CI/CD  
**Projet :** Real-Time Sales Analytics Pipeline  

**Technologies :** GitLab, Docker, Kafka, PySpark, PostgreSQL, Pytest, Jenkins, SonarQube

---

## 1. Contexte professionnel

Vous intervenez en tant qu’ingénieur Data au sein d’une équipe développant une plateforme d’analyse des ventes en temps réel.

L’application reçoit des commandes via une API REST. Les événements sont transmis à Kafka, traités avec PySpark puis stockés dans PostgreSQL.

Votre mission consiste à **industrialiser cette pipeline Data** en mettant en place une stratégie de tests automatisés et une chaîne CI avec Jenkins et SonarQube.

L’objectif n’est pas uniquement de faire fonctionner la pipeline, mais de garantir qu’une modification du code peut être **testée, analysée et validée automatiquement** avant son intégration.

---

## 2. Objectifs pédagogiques

À l’issue du TP, vous devez être capable de :

- comprendre une architecture Data orientée streaming ;
- mettre en œuvre des tests unitaires ;
- concevoir des tests d’intégration ;
- réaliser un scénario de test End-to-End ;
- automatiser les tests avec Jenkins ;
- analyser la qualité du code avec SonarQube ;
- mettre en place un Quality Gate ;
- utiliser Git et GitLab dans un workflow collaboratif ;
- intégrer les pratiques DevOps dans un projet Data Engineering.

---

## 3. Architecture du projet

L’architecture fournie est la suivante :

```text
                 ┌──────────────────┐
                 │    Sales API     │
                 │     FastAPI      │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │      Kafka       │
                 │  sales.orders    │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │     PySpark      │
                 │    Streaming     │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │   PostgreSQL     │
                 │ processed_orders │
                 └──────────────────┘


             CI / Qualité logicielle

 GitLab ───────► Jenkins ───────► SonarQube
                    │                  │
                    │                  ▼
                    │             Quality Gate
                    │
                    ├── Tests unitaires
                    ├── Tests intégration
                    └── Tests E2E
```

L’infrastructure Docker est fournie dans le repository.

Vous devez principalement travailler sur :

- les tests ;
- la pipeline Jenkins ;
- l’analyse SonarQube ;
- le Quality Gate ;
- le workflow Git/GitLab.

---

## 4. Dépôt GitLab

Le projet de départ est disponible ici :

**https://gitlab.com/santoudllo/real-time-sales-devops-tp**

Le dépôt contient notamment :

```text
app/
tests/
spark/
docker/
infra/
Jenkinsfile
docker-compose.yml
sonar-project.properties
```

---

## 5. Organisation Git

Chaque étudiant ou groupe travaille dans **son propre fork** du projet.

Le workflow attendu est :

```text
Projet enseignant
       │
       │ Fork
       ▼
Projet étudiant
       │
       ▼
     dev
       │
       ├── développement
       ├── tests
       ├── Jenkins
       └── SonarQube
       │
       ▼
Merge Request
       │
       ▼
Projet enseignant
```

### 5.1 Fork du projet

Depuis le projet GitLab, sélectionnez :

**Fork → votre espace GitLab → Fork project**

Conservez les branches du projet.

---

## 6. Clonage du projet

Après avoir créé votre fork, clonez votre repository :

```bash
git clone https://gitlab.com/VOTRE_USERNAME/real-time-sales-devops-tp.git
```

Entrez dans le projet :

```bash
cd real-time-sales-devops-tp
```

Vérifiez les branches :

```bash
git branch -a
```

Vous devez notamment retrouver :

```text
remotes/origin/main
remotes/origin/dev
```

Placez-vous sur la branche `dev` :

```bash
git checkout dev
```

Récupérez les dernières modifications :

```bash
git pull origin dev
```

Vérifiez votre branche :

```bash
git branch
```

Résultat attendu :

```text
* dev
  main
```

> **Consigne : tout le développement du TP doit être réalisé sur la branche `dev`.**

---

# 7. Démarrage de l’environnement

L’environnement technique est entièrement fourni avec Docker.

Aucune installation locale de Kafka, Spark ou PostgreSQL n’est nécessaire.

Lancez l’environnement :

```bash
docker compose up -d --build
```

Vérifiez les conteneurs :

```bash
docker compose ps
```

Les principaux services sont :

| Service | Fonction |
|---|---|
| FastAPI | API de ventes |
| Kafka | Ingestion des événements |
| PostgreSQL | Stockage des données |
| PySpark | Traitement streaming |
| Jenkins | CI/CD |
| SonarQube | Analyse de qualité |

Interfaces disponibles :

```text
API       : http://localhost:8000/docs
Spark     : http://localhost:8080
Jenkins   : http://localhost:8082
SonarQube : http://localhost:9000
```

---

# 8. Comprendre le flux fonctionnel

L’API permet de générer des événements de ventes.

Exemple :

```bash
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "C001",
    "product_id": "P001",
    "quantity": 2,
    "unit_price": 49.90
  }'
```

Le système doit réaliser le flux :

```text
POST /api/orders
       │
       ▼
     Kafka
       │
       ▼
    PySpark
       │
       ▼
  PostgreSQL
```

L’événement contient notamment :

```text
order_id
customer_id
product_id
quantity
unit_price
total_amount
timestamp
```

Avant de commencer les tests, prenez quelques minutes pour comprendre :

- `app/main.py`
- `spark/sales_stream.py`
- `infra/postgres/init.sql`
- `docker-compose.yml`
- `tests/`
- `Jenkinsfile`
- `sonar-project.properties`

---

# 9. Partie 1 — Tests unitaires

## Objectif

Vérifier le comportement des fonctions métier indépendamment de Kafka, Spark et PostgreSQL.

Les tests sont à réaliser dans :

```text
tests/unit/
```

Le projet contient déjà quelques tests de départ.

Vous devez compléter la couverture fonctionnelle.

### Cas à tester

Vous devez notamment vérifier :

- calcul du montant total ;
- quantité positive ;
- quantité nulle ;
- quantité négative ;
- prix positif ;
- prix invalide ;
- identifiant client ;
- identifiant produit ;
- génération de l’identifiant de commande ;
- construction correcte d’un événement.

Exécutez :

```bash
pytest tests/unit -v
```

Puis générez la couverture :

```bash
pytest tests/unit \
  --cov=app \
  --cov-report=term-missing \
  --cov-report=xml:coverage.xml
```

### Attendu

Les tests doivent :

- être reproductibles ;
- couvrir les règles métier principales ;
- tester les cas nominaux ;
- tester les cas limites ;
- tester les cas invalides.

---

# 10. Partie 2 — Tests d’intégration

## Objectif

Vérifier que les différents composants fonctionnent correctement ensemble.

Vous devez tester au minimum deux interactions.

### Test A — API → Kafka

Vérifier qu’une commande envoyée à l’API produit correctement un événement dans le topic :

```text
sales.orders
```

Le test doit permettre de vérifier que :

```text
API
 ↓
Kafka
```

fonctionne correctement.

### Test B — Kafka → Spark → PostgreSQL

Vérifier qu’un événement consommé par Spark est correctement traité puis enregistré dans :

```text
processed_orders
```

Le flux attendu est :

```text
Kafka
  ↓
PySpark
  ↓
PostgreSQL
```

Les tests doivent être placés dans :

```text
tests/integration/
```

Exemple d’organisation :

```text
tests/integration/
├── test_api_kafka.py
└── test_spark_postgres.py
```

### Consigne

Les tests d’intégration doivent réellement utiliser les services Docker.

Il ne s’agit plus de simuler entièrement les composants.

---

# 11. Partie 3 — Tests End-to-End

## Objectif

Valider le comportement de l’ensemble de la plateforme.

Le scénario attendu est :

```text
API
 │
 ▼
Kafka
 │
 ▼
PySpark
 │
 ▼
PostgreSQL
```

### Scénario

#### Given

L’infrastructure est démarrée.

#### When

Une commande est envoyée à l’API.

Exemple :

```json
{
  "customer_id": "C100",
  "product_id": "P001",
  "quantity": 3,
  "unit_price": 100
}
```

#### Then

La commande doit :

1. être publiée dans Kafka ;
2. être consommée par Spark ;
3. être traitée ;
4. être enregistrée dans PostgreSQL ;
5. contenir les valeurs attendues.

Le montant attendu est :

```text
300
```

Les tests sont placés dans :

```text
tests/e2e/
```

### Attention

Évitez les attentes arbitraires telles que :

```python
time.sleep(30)
```

Privilégiez un mécanisme de **polling avec timeout** permettant d’attendre réellement la disponibilité du résultat.

Le test doit échouer proprement si le délai maximal est dépassé.

---

# 12. Partie 4 — Mise en place de Jenkins

## Objectif

Automatiser l’exécution des tests et contrôler la qualité du projet.

Accédez à :

```text
http://localhost:8082
```

Le projet contient déjà :

```text
Jenkinsfile
```

Vous devez compléter et industrialiser cette pipeline.

### Pipeline attendue

```text
Checkout
    ↓
Installation
    ↓
Tests unitaires
    ↓
Tests d’intégration
    ↓
Build
    ↓
Tests E2E
    ↓
Analyse SonarQube
    ↓
Quality Gate
```

La pipeline doit échouer automatiquement lorsqu’une étape critique échoue.

Exemple :

```text
Tests unitaires
      │
      ├── PASS ──► étape suivante
      │
      └── FAIL ──► Pipeline FAILED
```

---

# 13. Partie 5 — SonarQube

## Objectif

Introduire une analyse automatisée de la qualité du code.

Accédez à :

```text
http://localhost:9000
```

Le projet contient déjà :

```text
sonar-project.properties
```

Configurez l’analyse afin d’obtenir notamment :

- Bugs ;
- Vulnerabilities ;
- Code Smells ;
- Coverage ;
- Duplications.

Vous devez comprendre les résultats produits par SonarQube et identifier les problèmes nécessitant une correction.

---

# 14. Partie 6 — Quality Gate

Le Quality Gate permet de définir des conditions minimales de qualité avant de considérer la pipeline comme valide.

Vous devez mettre en place des critères cohérents.

Exemple :

```text
Coverage          ≥ 80 %
Bugs              = 0
Vulnerabilities   = 0
Duplications      ≤ 3 %
```

Le principe attendu est :

```text
                 SonarQube
                     │
                     ▼
                Quality Gate
                     │
              ┌──────┴──────┐
              │             │
            PASS           FAIL
              │             │
              ▼             X
          Pipeline OK    Pipeline STOP
```

Le pipeline Jenkins doit exploiter le résultat du Quality Gate.

---

# 15. Partie 7 — Amélioration du code

Après la première analyse SonarQube, identifiez les problèmes détectés.

Classez-les par catégorie :

```text
Bugs
Code Smells
Vulnerabilities
Duplications
Coverage
```

Corrigez les problèmes prioritaires.

Après chaque modification :

```bash
git status
git add .
git commit -m "Correction qualité du code"
git push origin dev
```

Relancez ensuite Jenkins et comparez les résultats.

L’objectif est d’observer l’amélioration progressive de la qualité du projet.

---

# 16. Partie 8 — Industrialisation complète

À la fin du TP, votre pipeline doit être capable de réaliser automatiquement :

```text
                    GitLab
                       │
                       ▼
                    Jenkins
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
       Tests unitaires          Build
             │                   │
             ▼                   ▼
     Tests intégration       Tests E2E
             │                   │
             └─────────┬─────────┘
                       ▼
                   SonarQube
                       │
                       ▼
                 Quality Gate
                       │
                ┌──────┴──────┐
                ▼             ▼
               PASS          FAIL
                │              │
                ▼              X
           Pipeline OK      Pipeline STOP
```

La pipeline doit permettre de détecter automatiquement une régression fonctionnelle ou une dégradation de la qualité du code.

---

# 17. Gestion Git

À chaque étape importante :

```bash
git status
git add .
git commit -m "Message explicite"
git push origin dev
```

Les messages de commit doivent être explicites.

Exemples :

```text
Ajout des tests unitaires
Ajout tests intégration Kafka
Ajout tests E2E
Configuration Jenkins
Configuration SonarQube
Correction des erreurs de qualité
Amélioration de la couverture
```

Évitez les messages tels que :

```text
update
test
modif
fix
```

---

# 18. Merge Request

Lorsque votre travail est terminé, vérifiez :

```bash
git status
```

Puis :

```bash
git push origin dev
```

Depuis GitLab, créez ensuite une **Merge Request**.

La branche source est :

```text
dev
```

La branche cible dépend du workflow demandé par l’enseignant.

Dans le cadre de ce TP, la Merge Request doit être dirigée vers la branche de référence du projet fourni par l’enseignant.

La Merge Request doit contenir :

- un résumé du travail réalisé ;
- la stratégie de tests ;
- les résultats Jenkins ;
- les résultats SonarQube ;
- la couverture obtenue ;
- les difficultés rencontrées ;
- les corrections apportées.

---

# 19. Livrables

Chaque groupe doit fournir les éléments suivants.

## Code

```text
app/
tests/
spark/
Jenkinsfile
sonar-project.properties
```

## Tests

```text
tests/unit/
tests/integration/
tests/e2e/
```

Les trois niveaux de tests doivent être fonctionnels.

## CI/CD

Une pipeline Jenkins fonctionnelle comprenant au minimum :

```text
Tests unitaires
Tests d'intégration
Build
Tests E2E
Analyse SonarQube
Quality Gate
```

## Qualité

Le projet doit être analysé par SonarQube avec au minimum :

- couverture ;
- bugs ;
- vulnerabilities ;
- code smells ;
- duplications ;
- Quality Gate.

## Documentation

Créer :

```text
docs/report.md
```

Le rapport doit présenter :

1. l’architecture du projet ;
2. la stratégie de tests ;
3. les tests unitaires réalisés ;
4. les tests d’intégration réalisés ;
5. le scénario E2E ;
6. la pipeline Jenkins ;
7. la configuration SonarQube ;
8. le Quality Gate ;
9. les résultats obtenus ;
10. les problèmes rencontrés ;
11. les corrections réalisées.

---

# 20. Checklist finale

Avant de soumettre votre travail, vérifiez :

```text
[ ] Fork réalisé
[ ] Projet cloné
[ ] Branche dev utilisée
[ ] Infrastructure Docker fonctionnelle
[ ] API fonctionnelle
[ ] Kafka fonctionnel
[ ] Spark fonctionnel
[ ] PostgreSQL fonctionnel

[ ] Tests unitaires réalisés
[ ] Tests d'intégration réalisés
[ ] Tests E2E réalisés

[ ] Jenkins configuré
[ ] Pipeline automatisée
[ ] SonarQube configuré
[ ] Coverage publiée
[ ] Quality Gate fonctionnel

[ ] Code corrigé
[ ] Documentation réalisée
[ ] git push effectué
[ ] Merge Request créée
```

---

# 21. Question de synthèse

À l’issue du TP, vous devez être capable de répondre à la question suivante :

> **Comment garantir automatiquement qu’une modification apportée à une pipeline Data/Big Data ne dégrade ni son fonctionnement ni la qualité du code ?**

Votre réponse devra s’appuyer sur les mécanismes que vous avez mis en œuvre :

```text
GitLab
   +
Tests automatisés
   +
Jenkins
   +
SonarQube
   +
Quality Gate
   =
CI fiable pour une pipeline Data
```

La réussite du TP ne consiste pas uniquement à obtenir une pipeline verte.

Vous devez être capables d’expliquer :

- pourquoi chaque type de test est nécessaire ;
- ce que chaque test valide ;
- la différence entre test unitaire, intégration et E2E ;
- comment Jenkins orchestre les différentes étapes ;
- comment SonarQube mesure la qualité ;
- comment le Quality Gate intervient dans la CI ;
- comment GitLab permet de sécuriser l’intégration du code.

---

## Consigne finale

Vous devez être capables de présenter votre solution comme une **pipeline Data Engineering industrialisée**, et non comme une simple application fonctionnelle.

Le résultat attendu est une chaîne dans laquelle :

```text
Développer
    ↓
Commit
    ↓
Push
    ↓
Jenkins
    ↓
Tests
    ↓
Build
    ↓
E2E
    ↓
SonarQube
    ↓
Quality Gate
    ↓
Validation
```

Chaque étape doit apporter un contrôle permettant de réduire le risque d'introduire une régression dans la plateforme.
