#!/usr/bin/env bash

export RUSTFLAGS="--cfg tokio_unstable"

cargo "$1" --features "tokio-console" "${@:2}"
