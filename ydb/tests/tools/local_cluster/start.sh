#!/usr/bin/env bash

set -e
../../../../ya make -r .
./local_cluster --version main
