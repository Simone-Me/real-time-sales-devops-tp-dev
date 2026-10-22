# Déploiement Kubernetes local (Minikube)

| Fichier | Ressource |
|---|---|
| `configmap.yaml` | ConfigMap `sales-config` (paramètres non secrets) |
| `postgres-init-configmap.yaml` | ConfigMap `postgres-init` (copie de `infra/postgres/init.sql`) |
| `postgres-pvc.yaml` | PersistentVolumeClaim `postgres-data` (1 Gi) |
| `postgres-deployment.yaml` | Deployment `postgres` (`postgres:16-alpine`) |
| `postgres-service.yaml` | Service ClusterIP `postgres:5432` |
| `api-deployment.yaml` | Deployment `sales-api` (image `sales-api:tp3`) |
| `api-service.yaml` | Service ClusterIP `sales-api:8000` |

## 1. Image de l'API

```bash
docker build -t sales-api:tp3 -f docker/api/Dockerfile .
minikube image load sales-api:tp3
minikube image ls | grep sales-api
```

## 2. Secret PostgreSQL (jamais commité)

Le mot de passe n'est pas stocké dans Git. Créez le Secret avant de déployer :

```bash
kubectl create secret generic postgres-secret \
  --from-literal=POSTGRES_PASSWORD='<mot-de-passe>'
```

## 3. Déploiement

```bash
kubectl apply -f k8s/
kubectl get deployments,pods,services,pvc
```

## 4. Vérifications

```bash
# API
kubectl port-forward service/sales-api 8000:8000
curl http://localhost:8000/api/health

# PostgreSQL
kubectl exec -it deploy/postgres -- psql -U sales -d sales -c '\dt'
```

## Nettoyage

```bash
kubectl delete -f k8s/
kubectl delete secret postgres-secret
```

`kubectl delete -f k8s/` supprime aussi le PVC, donc les données de la base.
