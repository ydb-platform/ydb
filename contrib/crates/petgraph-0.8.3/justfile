# Lists all available recipes
default:
    just --list

# Builds the project in development mode
build:
    cargo build

# Tests with all features enabled
test:
    cargo test --features all

# Miri with all tests (this might take very long). Consider the fast-miri recipe instead or specify individual tests
miri:
    cargo miri test

# Miri with the same configuration as in (non-thorough) CI. Uses nextest and excludes some tests that are known to be very slow
miri-fast:
    cargo miri nextest run -- \
                --skip b01_vienna_test \
                --skip b07_vienna_test \
                --skip generic_graph6_encoder_test_cases \
                --skip graph6_for_csr_test_cases \
                --skip graph6_for_graph_map_test_cases \
                --skip graph6_for_graph_test_cases \
                --skip graph6_for_matrix_graph_test_case \
                --skip graph6_for_stable_graph_test_cases

# Fmt with the same configuration as in CI
fmt:
    cargo fmt --all -- --check

# Clippy with the same configuration as in CI
clippy:
    cargo clippy --all-features --lib --bins --examples --tests -- -D warnings

# Runs all linting checks that are run in CI
lint: fmt clippy

# Runs all tests and linting that are run in CI
ci: fmt clippy test

# Checks if no-std is working same as in CI. Requires the wasm32v1-none target to be installed
check-no-std:
    cargo check --no-default-features -p petgraph --target wasm32v1-none --features graphmap,serde-1,stable_graph,matrix_graph,generate,unstable
