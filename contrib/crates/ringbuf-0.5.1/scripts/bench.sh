#!/bin/sh

cargo +nightly bench --features=bench && \
cd async && \
cargo +nightly bench --features=bench && \
echo "Done!"
