#!/usr/bin/env bash

docker build --platform=linux/amd64 -f Dockerfile -t $1 .