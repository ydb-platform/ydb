#!/bin/sh
mkdir -p proto/google/pubsub/v1/
cp -a google/cloud/pubsub_v1/proto/. proto/google/pubsub/v1/
rm -r google/cloud/pubsub_v1/proto