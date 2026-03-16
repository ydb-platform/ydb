# Contributor Guidance for `sdk-core`

This repository provides a Rust workspace for the Temporal Core SDK and related crates. Use this
document as your quick reference when submitting pull requests.

## Where Things Are

- `core/` – implementation of the core SDK
- `client/` – clients for communicating with Temporal clusters
- `core-api/` – API definitions exposed by core
- `core-c-bridge/` – C interface for core
- `sdk/` – pre-alpha Rust SDK built on top of core (used mainly for tests)
- `sdk-core-protos/` – protobuf definitions shared across crates
- `fsm/` – state machine implementation and macros
- `test-utils/` – helpers and binaries for tests
- `tests/` – integration, heavy, and manual tests
- `arch_docs/` – architectural design documents
- Contributor guide: `README.md`
- `target/` - This contains compiled files. You never need to look in here.

## Repo Specific Utilities

- `.cargo/config.toml` defines useful cargo aliases:
    - `cargo lint` – run clippy on workspace crates
    - `cargo test-lint` – run clippy on tests
    - `cargo integ-test` – run the integration test runner
- `cargo-tokio-console.sh` – run any cargo command with the `tokio-console` feature
- `integ-with-otel.sh` – run integration tests with OpenTelemetry enabled
- `.cargo/multi-worker-manual-test` – helper script for spawning multiple workers during manual
  testing

## Building and Testing

The following commands are enforced for each pull request (see `README.md`):

```bash
cargo build            # build all crates
cargo test             # run unit tests
cargo integ-test       # integration tests (starts ephemeral server by default)
cargo test --test heavy_tests  # load tests -- agents do not need to run this and should not
```

Rust compilation can take some time. Do not interrupt builds or tests unless they are taking more
than 10 minutes.

Additional checks:

```bash
cargo fmt --all        # format code
cargo clippy --all -- -D warnings  # lint
```

Documentation can be generated with `cargo doc`.

## Expectations for Pull Requests

- Format and lint your code before submitting.
- Ensure all tests pass locally. Integration tests may require a running Temporal server or the
  ephemeral server started by `cargo integ-test`.
- Keep commit messages short and in the imperative mood.
- Provide a clear PR description outlining what changed and why.
- Reviewers expect new features or fixes to include corresponding tests when applicable.

## Review Checklist

Reviewers will look for:

- All builds, tests, and lints passing in CI
    - Note that some tests cause intentional panics. That does not mean the test failed. You should
      only consider tests that have failed according to the harness to be a real problem.
- New tests covering behavior changes
- Clear and concise code following existing style (see `README.md` for error handling guidance)
- Documentation updates for any public API changes

## Notes

- Fetch workflow histories with `cargo run --bin histfetch <workflow_id> [run_id]` (binary lives in
  `test-utils`).
- Protobuf files under `sdk-core-protos/protos/api_upstream` are a git subtree; see `README.md` for
  update instructions.
