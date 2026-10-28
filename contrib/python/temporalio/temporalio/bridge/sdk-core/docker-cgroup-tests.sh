#!/usr/bin/env bash

# Runs tests in a constrained Docker cgroup by cross-compiling them to MUSL with
# `cargo zigbuild` and executing inside Ubuntu.
# Requirements:
# - Zig toolchain: https://ziglang.org/download/
# - cargo-zigbuild helper: https://github.com/rust-cross/cargo-zigbuild#installation (e.g. `cargo install cargo-zigbuild`)

# Usage:
#   ./docker-cgroup-tests.sh [cpus] [memory]
# Defaults:
#   cpus   = 0.1  (limit available CPU cores)
#   memory = 256m (limit available RAM)

# cross-compiles integration tests with cargo zigbuild, then runs them using docker
executable_path=$(cargo zigbuild --target x86_64-unknown-linux-musl --tests --features="test-utilities ephemeral-server" --message-format=json | jq -r 'select(.profile?.test == true and .target?.name == "temporalio_sdk_core" and .executable) | .executable')
relative_path="target/${executable_path#*target/}"

cpus="${1:-0.1}"
memory="${2:-256m}"

printf 'Running with cpus=%s memory=%s\n' "$cpus" "$memory"

docker run --rm -v "$PWD":/app -w /app --cpus="$cpus" --memory="$memory" --env CGROUP_TESTS_ENABLED=true ubuntu bash -c "./$relative_path cgroup_ --nocapture"
