#!/bin/sh

cargo +nightly miri test && \
cargo +nightly miri test --features test_local && \
cd async && \
cargo +nightly miri test && \
echo "Done!"
