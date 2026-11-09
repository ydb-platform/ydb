#!/bin/sh

rustup target add thumbv6m-none-eabi && \
cargo test && \
cargo test --features test_local && \
cargo test --features portable-atomic && \
cargo check --no-default-features --features alloc && \
cargo check --no-default-features && \
cargo check --target thumbv6m-none-eabi --no-default-features --features alloc,portable-atomic,portable-atomic/critical-section && \
cd async && \
cargo test && \
cargo test --no-default-features --features alloc && \
cargo check --no-default-features --features alloc && \
cargo check --no-default-features && \
cargo check --target thumbv6m-none-eabi --no-default-features --features alloc,portable-atomic,portable-atomic/critical-section && \
cd ../blocking && \
cargo test && \
cargo check --no-default-features --features alloc && \
cargo check --no-default-features && \
cargo check --target thumbv6m-none-eabi --no-default-features --features alloc,portable-atomic,portable-atomic/critical-section && \
echo "Done!"
