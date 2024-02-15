#!/usr/bin/env bash

docker build --push --platform=linux/amd64 -f Dockerfile -t $1 .