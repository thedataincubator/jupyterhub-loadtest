#!/bin/bash

docker build -t quay.io/zachglassman/jupyterhub-stress:v0.4.7 .
docker push quay.io/zachglassman/jupyterhub-stress:v0.4.7

#docker build -t quay.io/zachglassman/kubectl:v1.9.0 -f Dockerfile.kubectl .
#docker push quay.io/zachglassman/kubectl:v1.9.0
