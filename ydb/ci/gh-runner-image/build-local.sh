#!/usr/bin/env bash

set -e

if [[ -z "$FOLDER_ID" || -z "$ZONE_ID" || -z "$SUBNET_ID" ]]; then
  echo "FOLDER_ID, ZONE_ID and SUBNET_ID must be set"
  exit 1
fi

export YC_TOKEN=$(yc iam create-token)

packer build \
  -var "folder_id=${FOLDER_ID?}" \
  -var "zone_id=${ZONE_ID?}" \
  -var "subnet_id=${SUBNET_ID?}" \
  .
