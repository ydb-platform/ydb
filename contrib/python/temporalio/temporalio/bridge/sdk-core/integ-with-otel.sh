#!/usr/bin/env bash

# Run integ tests with OTel collector export enabled
export TEMPORAL_INTEG_OTEL_URL="grpc://localhost:4317"
export TEMPORAL_TRACING_FILTER="temporal_sdk_core=DEBUG"

cargo integ-test "${@:1}"
