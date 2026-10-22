# TP — Déployer une pipeline Data avec Kubernetes en local

**Formation :** Master Data Engineering  
**Thématique :** DevOps, conteneurisation et orchestration  
**Projet :** Real-Time Sales Analytics Pipeline  
**Environnement :** poste local, Docker et Minikube — aucun cloud requis

## 1. Contexte et mission

Lors du TP précédent (TP — Industrialisation d’une Pipeline Big Data avec CI/CD), vous avez lancé avec Docker Compose une plateforme de ventes composée de services tels que FastAPI, Kafka, PySpark et PostgreSQL, ainsi que des outils DevOps.

Dans ce TP, vous allez reprendre **le même projet** et découvrir comment Kubernetes déploie et supervise des applications conteneurisées.

**Mission :** déployer une version locale de la plateforme sur Minikube, vérifier l’état des composants, exposer l’API, mettre à l’échelle un déploiement et observer le redémarrage automatique d’un Pod.

> Le but n’est pas de convertir toute l’infrastructure en une seule fois. Concentrez-vous sur les objets Kubernetes et sur le fonctionnement de la pipeline. Jenkins et SonarQube restent hors périmètre de ce TP.

## 2. Architecture cible

```text
Navigateur / curl
       |
       v
Service Kubernetes (API)
       |
       v
Deployment API ---- Pods API
       |
       v
Kafka ---> PySpark ---> PostgreSQL
```

Les composants de la pipeline doivent communiquer à l’intérieur du cluster via des **Services Kubernetes**. Le poste local accède à l’API au moyen d’un `port-forward`.

### À retenir

- **Pod** : unité qui exécute un ou plusieurs conteneurs.
- **Deployment** : maintient le nombre de réplicas souhaité et permet les mises à jour.
- **Service** : fournit un point d’accès réseau stable vers les Pods.
- **ConfigMap** : stocke de la configuration non confidentielle.
- **kubectl** : outil en ligne de commande pour piloter Kubernetes.

## 3. Préparer le projet et le cluster local

Le fork du projet a déjà été créé, le dépôt a été cloné et la branche `dev` a été préparée lors du TP précédent. **Ne recommencez pas ces étapes** : reprenez simplement le dossier du projet déjà présent sur votre poste.

Vérifiez que Docker fonctionne, puis démarrez Minikube :

```bash
minikube start --driver=docker
kubectl get nodes
kubectl get pods -A
```

Le nœud Minikube doit être `Ready`.

> Si vous utilisez déjà Docker Compose pour cette plateforme, arrêtez ses services avant les tests afin d’éviter les conflits de ports et de ressources :
>
> ```bash
> docker compose down
> ```
>
> Ne supprimez pas les volumes de données du projet.

## 4. Examiner l’infrastructure existante

Avant de créer les manifestes Kubernetes, inspectez les fichiers du dépôt :

```bash
docker compose config
```

Repérez dans `docker-compose.yml` les services, images ou paramètres `build`, variables d’environnement, ports, dépendances et volumes. Consultez également les Dockerfiles présents dans le dépôt.

Créez le dossier de travail :

```bash
mkdir -p k8s
```

Pour ce TP, déployez au minimum **l’API et PostgreSQL**, puis intégrez Kafka et Spark en réutilisant les images et la configuration du projet lorsqu’elles sont disponibles et compatibles.

**Attention :** les noms de services Docker Compose ne deviennent pas automatiquement des noms DNS Kubernetes. Les composants doivent utiliser les noms des Services Kubernetes définis dans vos manifestes.

## 5. Créer les ressources Kubernetes

Dans le dossier `k8s/`, créez des manifestes YAML pour les ressources suivantes :

| Ressource | Rôle attendu |
|---|---|
| `Deployment` API | Démarrer l’API à partir de l’image du projet |
| `Service` API | Fournir un accès stable à l’API dans le cluster |
| `Deployment` PostgreSQL | Démarrer la base avec les paramètres requis |
| `Service` PostgreSQL | Permettre aux composants de joindre la base |
| `ConfigMap` | Centraliser les paramètres non secrets, si nécessaire |

Réutilisez les images Docker existantes. Si une image est construite localement, construisez-la ou chargez-la dans Minikube et utilisez le même nom dans le manifeste. Pour la base, prévoyez un volume persistant si vous devez conserver les données après la recréation du Pod. Ne placez pas de vrais mots de passe dans une ConfigMap ou dans un dépôt Git.

### Exemple de Deployment à adapter pour l’API

Créez `k8s/api-deployment.yaml`. Remplacez `sales-api:tp3` par le nom réel de l’image construite ou chargée dans Minikube, et adaptez le port à celui de l’application.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sales-api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: sales-api
  template:
    metadata:
      labels:
        app: sales-api
    spec:
      containers:
        - name: api
          image: sales-api:tp3
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 8000
```

Créez également `k8s/api-service.yaml` pour exposer le port de l’API **à l’intérieur du cluster**. Le sélecteur du Service doit correspondre au label `app: sales-api`, et le `targetPort` doit correspondre au port du conteneur.

Complétez les manifestes de PostgreSQL et, si le temps le permet, de Kafka et Spark en vous basant sur la configuration réelle du projet. Ne copiez pas à l’aveugle les noms de variables ou les commandes : vérifiez-les dans le dépôt.

## 6. Déployer et observer

Appliquez les manifestes :

```bash
kubectl apply -f k8s/
kubectl get deployments
kubectl get pods -o wide
kubectl get services
```

Si un Pod ne démarre pas, examinez son état et ses événements :

```bash
kubectl describe pod NOM_DU_POD
kubectl logs NOM_DU_POD
```

Attendez que les composants nécessaires soient prêts avant de tester l’application. Si un conteneur redémarre ou reste en erreur, identifiez la cause dans les événements et les logs.

## 7. Tester l’API et manipuler Kubernetes

### A. Accéder à l’API

Dans un terminal, lancez :

```bash
kubectl port-forward service/sales-api 8000:8000
```

Gardez ce terminal ouvert. Dans un autre terminal, ouvrez `http://localhost:8000/docs` ou envoyez une requête à l’API, selon les routes réellement disponibles dans le projet.

Si le `port-forward` échoue, vérifiez le nom du Service, son port et l’état des Pods.

### B. Mettre à l’échelle le Deployment

Passez l’API à trois réplicas :

```bash
kubectl scale deployment/sales-api --replicas=3
kubectl get pods -o wide
kubectl get deployment sales-api
```

Observez les Pods créés. Revenez ensuite à un seul réplica :

```bash
kubectl scale deployment/sales-api --replicas=1
```

### C. Observer la supervision

Identifiez un Pod de l’API, puis supprimez-le :

```bash
kubectl get pods
kubectl delete pod NOM_DU_POD
kubectl get pods -w
```

Observez Kubernetes créer un Pod de remplacement afin de respecter l’état souhaité du Deployment.

### D. Vérifier le flux Data

Lorsque Kafka, Spark et PostgreSQL sont déployés et prêts, envoyez une commande de test à l’API en reprenant le format du TP précédent :

```bash
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"C100","product_id":"P001","quantity":3,"unit_price":100}'
```

Vérifiez ensuite, avec les moyens déjà utilisés au TP précédent, si l’événement est traité et enregistré dans PostgreSQL. Le résultat dépend du déploiement correct de tous les composants et de leur configuration réseau.

## 8. Livrables

Dans votre fork, ajoutez le dossier `k8s/` contenant les manifestes créés. Puis :

```bash
git status
git add k8s/
git commit -m "Ajout du déploiement Kubernetes local"
git push origin dev
```

Remettez :
- les manifestes Kubernetes ;
- une courte capture ou sortie de `kubectl get pods`, `kubectl get deployments` et `kubectl get services` ;
- une preuve de test de l’API ;
- deux ou trois lignes expliquant ce qui s’est passé après la suppression d’un Pod.

## 9. Nettoyage

À la fin du TP :

```bash
kubectl delete -f k8s/
minikube stop
```

## Résultat attendu

Vous devez avoir compris comment Kubernetes déploie une application, lui fournit un accès réseau, ajuste le nombre de réplicas et recrée un Pod supprimé. Vous devez également avoir identifié les adaptations nécessaires pour faire communiquer les services de la pipeline Data dans le cluster.

**Référence du projet :** https://gitlab.com/santoudllo/real-time-sales-devops-tp
