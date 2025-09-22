# Cloud-Native-Reddit-Data-Pipeline
This repository contains the backend system to a group project that built a cloud-native Reddit data pipeline. The overall system was deployed on a Kubernetes cluster using Fission serverless functions, with data harvested from Reddit using PRAW, processed with NLP, and stored in Elasticsearch for advanced analytics. This repo highlights my individual contribution within a larger group project. The full system included additional components not reflected here (handled by other team members).

## Content
My specific role focused on data ingestion, processing, and NLP analysis, which is documented in this repository.
### Repo Structure
```
fission_functions/
    ├── addReddit/
    ├── enqueue/
    ├── reddit_harvest/
    ├── redditProcessor/
    ├── specs/
    └── specs_processor/
LICENSE
README.md
redisinsight.yaml         
```
### Functions
Each function folder contains the corresponding Python file, build script and requirements.txt.
- addReddit: Store enqueued Reddit data into Elasticsearch
- enqueue: Enqueue Reddit data into Redis
- reddit_harvest: Harvest data from Reddit using PRAW
- redditProcessor: Process Reddit data with NLP workflow

### Fission specs
The spec folders contain all the required yaml files to configure the functions, including trigger events and shared parameters.
- specs: The specifications for the data streaming pipeline.
- specs_processor: The specifications for the data processing pipeline.

### Workflow
1. Harvest Reddit posts/comments with PRAW.
2. Enqueue raw data into Redis.
3. Store data into Elasticsearch with custom index mappings.
4. Run NLP analysis (VADER sentiment, LDA topic modeling).
5. Push processed results back to Elasticsearch for analytics dashboards.

### Logical Architecture Diagram
<img width="1261" height="611" alt="Logical Layered Architecture(2)" src="https://github.com/user-attachments/assets/eea68d87-58b7-4c69-a5c8-4916f38a9b4c" />

## Data streaming
The data streaming pipeline can be deployed by applying specs and specs_processor in the fission_functions directory to the Kubernetes cluster. These two spec folders contain all the YAML file to create the functions and the corresponding event triggers.
```bash
fission spec apply --specdir specs
fission spec apply --specdir specs_processor
```

The following script is used to install Redis for this project.
```bash
export REDIS_VERSION='0.19.1'
helm repo add ot-helm https://ot-container-kit.github.io/helm-charts/
helm upgrade redis-operator ot-helm/redis-operator \
    --install --namespace redis --create-namespace --version ${REDIS_VERSION}
    
kubectl create secret generic redis-secret --from-literal=password=group64 -n redis
helm upgrade redis ot-helm/redis --install --namespace redis  
```
The connection string is as follow: `redis://redis-headless.redis.svc.cluster.local:6379`

## Route for data processor functions
Unlike the harvester functions that are automated with time-trigger events, the data processor function `redditprocessor` can only be invoked manually after creating a route for it.
```bash
fission route create --url /redditprocessor --function redditprocessor --name redditprocessor --createingress
```

Then, we need to start a port forward in another shell:
```bash
kubectl port-forward service/router -n fission 9090:80
```

Finally, the function can be  invoked using the following command:
```bash
curl "http://127.0.0.1:9090/redditprocessor" | jq '.'
```

## Monitoring the functions
We can monitor the functions by using `fission fn log` to ensure the pipeline is running.
```bash
fission fn log --name rharvesteradel
fission fn log --name rharvesteraus
fission fn log --name rharvesterbris
fission fn log --name rharvestermelb
fission fn log --name rharvestersydney
fission fn log --name addreddit
fission fn log --name redditprocessor
```
>Note: The `redditprocessor` will take a few minutes to run before we can see the log due to its heavy operation.
