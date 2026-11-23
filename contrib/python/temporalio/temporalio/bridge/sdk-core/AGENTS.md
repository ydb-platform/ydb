# Contributor Guidance for `sdk-rust`

This repository provides a Rust workspace for the Temporal Core SDK and related crates. Use this
document as your quick reference when submitting pull requests.

## Requirements for coding agents

- Always use `cargo integ-test <test_name>` for running integration tests. Do not run them directly. If you have added `dbg!` or println statements, you must add `-- --nocapture` to see them.
- Unit tests may be run with `cargo test`. If you are about to run a test, you do not need to run
  `cargo build` separately first. Just run the test, and it will build. Running `cargo build --test`
  DOES NOT build integration tests. Use `cargo lint` for checking if integration tests compile
  without running them.
- Always use `cargo lint` for checking lints / clippy. Do not use clippy directly.
- It is EXTREMELY IMPORTANT that any added comments should explain why something needs to be done,
  rather than what it is. Comments that simply state a fact easily understood from type signatures
  or other context should NEVER be added. Always prefer to avoid a comment unless it truly is 
  clarifying something nonobvious. This does _NOT_ mean you should _remove_ any existing comments
  unless they are no longer accurate.
- Always make every attempt to avoid explicit sleeps in test code. Instead rely on synchronization
  techniques like channels, Notify, etc.
- Rust compilation can take some time. Do not interrupt builds or tests unless they are taking more
  than 5 minutes. When making changes that may break integration tests, after compiling, run
  integration tests with `timeout 180`, as it is possible to introduce test hangs.
- DO NOT put use statements in function scope. Always put them at the top of the file, unless doing
  so helps prevent ambiguous method resolution because of traits. Putting them at the top of a tests module is also acceptable.
- If you want to format, don't bother checking first. Just run formatting, and run it by using
  `cargo +nightly fmt`, because some settings require the nightly formatter.
- Do not extract functions for relatively simple helpers that are only used in one location.
- Use custom error types leveraging `thiserror` for any public facing error types
- Don't add pro-forma unit tests that simply check values are passed through some very obviously
  correct machinery, or that defaults equal the same hardcoded values in the test, etc.


## Repo Specific Utilities

- `.cargo/config.toml` defines useful cargo aliases:
  - `cargo lint` – run clippy on workspace crates and integration tests
  - `cargo test-lint` – run clippy on unit tests
  - `cargo integ-test` – run the integration test runner

## Building and Testing

The following commands are enforced for each pull request (see `README.md`):

```bash
cargo fmt --all --check # ensure code is formatted
cargo build             # build all crates
cargo lint              # run lints
cargo test-lint         # run lints on tests
cargo test              # run unit tests
cargo integ-test        # integration tests (starts ephemeral server by default)
cargo test --test heavy_tests  # load tests -- agents do not need to run this and should not
```

Documentation can be generated with `cargo doc`.

## Expectations for Pull Requests

- Format and lint your code before submitting.
- Ensure all tests pass locally. Integration tests may require a running Temporal server or the
  ephemeral server started by `cargo integ-test`.
- Keep commit messages short and in the imperative mood.
- Provide a clear PR description outlining what changed and why.
- Reviewers expect new features or fixes to include corresponding tests when applicable.
- Always add a changelog entry for each user-facing change in a PR, under the `## Unreleased`
  heading and never under a released version heading. The two changelogs are split by audience:
  the repository-root `CHANGELOG.md` serves users of the Rust SDK, and
  `crates/sdk-core/CHANGELOG.md` serves users of the other Temporal SDKs, whose workers and
  clients run on Core.
- The test for either file is whether a user of that SDK can observe the change: different
  behavior, an option they can set, a new log or metric, a different interaction with the server.
  Ask what the user sees, not which crate or which files the PR touched.
- The Rust SDK runs on Core too, so a user-observable change in Core behavior normally belongs in
  both files, worded for each audience. What belongs only in the sdk-core changelog is what only
  the other SDKs' users can see — a Core capability the Rust SDK does not surface, or a C-bridge
  change. Conversely, an internal Rust API change that a language SDK absorbs inside its own
  bridge, without its users noticing, belongs in neither file. This applies to the crates Core
  shares as well: `temporalio-client`, `temporalio-common`, `temporalio-common-wasm`,
  `temporalio-macros`, and `temporalio-protos`.
- Write each entry from the perspective of the file's audience, and word a change that goes in
  both files separately for each rather than copying it verbatim. The root changelog should not
  describe Core internals or bridge-only APIs, and the sdk-core changelog should not name Rust
  identifiers its readers cannot call. The comment at the top of either file lists the available
  headings.

## Review Checklist

Reviewers will look for:

- All builds, tests, and lints passing in CI
    - Note that some tests cause intentional panics. That does not mean the test failed. You should
      only consider tests that have failed according to the harness to be a real problem.
- New tests covering behavior changes
- Clear and concise code following existing style (see `README.md` for error handling guidance)
- Documentation updates for any public API changes

## Where Things Are

- `crates` - All crates in the workspace.
  - `crates/client/` – clients for communicating with Temporal clusters
  - `crates/common/` – Common functionality & protobuf definitions
  - `crates/macros/` – procedural macro implementations
  - `crates/sdk/` – Rust SDK built on top of core
  - `crates/sdk-core/` – implementation of the core SDK
      - `crates/sdk-core/tests/` – integration, heavy, and manual tests
  - `crates/sdk-core-c-bridge/` – C interface for core
- `arch_docs/` – architectural design documents
- Contributor guide: `README.md`
- `target/` - This contains compiled files. You never need to look in here.

## Notes

- Fetch workflow histories with `cargo run --bin histfetch <workflow_id> [run_id]` (binary lives in
  `crates/core/src/histfetch.rs`).
- Protobuf files under `crates/protos/protos/api_upstream` are a git subtree; see
  `README.md` for update instructions.
