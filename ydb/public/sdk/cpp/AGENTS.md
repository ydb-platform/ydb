# YDB C++ SDK

These instructions apply to `ydb/public/sdk/cpp`. The repository-root `AGENTS.md` also applies; this file adds SDK-specific guidance. Run commands from the repository root.

## Repository map

| Path | Purpose |
| --- | --- |
| `include/ydb-cpp-sdk/` | Consumer-facing headers. Treat APIs here as compatibility-sensitive unless explicitly marked draft/private. |
| `src/client/` | Client implementations, normally paired with a public header and a local `ya.make`. |
| `src/client/impl/internal/` | Guarded implementation details. Never expose these headers through the public API. |
| `src/library/` | SDK support libraries shared by clients. |
| `adapters/`, `plugins/` | Optional integration layers; keep their dependencies out of the core SDK. |
| `examples/` | Compilable, user-oriented usage examples. Keep them simple and on public APIs. |
| `tests/unit/` | Focused client/library/plugin tests. |
| `tests/integration/` | Tests using the local YDB recipe and public service behavior. |
| `src/client/*/ut/` | Co-located component tests, especially topic, federated topic, and PersQueue. |

Service protobufs and gRPC definitions live under `ydb/public/api`; do not duplicate wire types in the SDK.

## Development rules

- Use C++20 or earlier and follow the surrounding style: four spaces, no tabs, attached braces, `#pragma once`, and `NYdb::inline Dev`. Use `./ya style <changed-files>` for formatting; avoid unrelated reformatting and include churn.
- Treat compiler warnings and `.clang-tidy` findings as errors. Fix the cause; do not add broad suppressions or weaken `.clang-tidy` to make a change pass. A narrow suppression requires a comment explaining why the diagnostic is a false positive.
- Put stable API declarations under `include/ydb-cpp-sdk/` and implementation under the matching `src/client/` or `src/library/` module. Update every affected `ya.make` explicitly.
- Preserve public source compatibility by default. Do not silently remove or rename public symbols, change defaults or ownership, or reorder enum values. Prefer additive overloads and deprecation.
- Public headers must compile for consumers without private `src/` headers or transitive-include accidents. 
- Follow existing fluent-setting macros and naming. When adding a setting, update its default, copy/converting constructors, request serialization, and tests; a field that is accepted but not propagated is a bug.
- Keep protobuf conversion at the SDK boundary. Preserve optional/presence semantics, unknown/unsupported enum handling, operation status, issues, endpoint, response metadata, and server compatibility. Use the established request builders and proto accessors instead of exposing mutable protos ad hoc.
- Async code must complete each promise exactly once on success, error, cancellation, and null-response paths. Audit callback captures, weak/shared ownership, teardown, driver/client/session lifetime, deadlines, and exceptions from continuations.
- Never invoke user callbacks or fulfill promises while holding a lock. Avoid blocking network/executor threads. For retries, preserve the operation's deadline and retry only when status and idempotency allow it.
- Keep optional adapter/plugin dependencies isolated. Production `PEERDIR`s in `src`, `include`, `plugins`, and `examples` must remain within `allowed_peerdirs.txt`; the subtree is mirrored into the standalone SDK.
- Add or update `CHANGELOG.md` for notable user-visible API or behavior changes, using the repository's current release process rather than inventing a version heading.

## Build and test

Choose the narrowest affected module first. Tests already include compilation.

```bash
# Build
./ya make --build relwithdebinfo -DUSER_CXXFLAGS=-Werror <folder>

# Run all tests in a target/subtree
./ya make --build relwithdebinfo -DUSER_CXXFLAGS=-Werror -tA <folder> 2>&1 | tail

# Run one suite/test (quote the glob)
./ya make --build relwithdebinfo -DUSER_CXXFLAGS=-Werror -tA <folder> -F '*test-filter*' 2>&1 | tail

# Repeat a suspected flake
./ya make --build relwithdebinfo -DUSER_CXXFLAGS=-Werror -tA <folder> -F '*test-filter*' --test-retries N 2>&1 | tail

# Dump production compile commands; `ya dump` does not build or run targets
mkdir -p ydb/public/sdk/cpp/build/clang-tidy
./ya dump compile-commands --build relwithdebinfo --no-generated \
  --files-in=<folder> \
  --cmd-build-root="$PWD/ydb/public/sdk/cpp/build/clang-tidy/generated" \
  --output-file=ydb/public/sdk/cpp/build/clang-tidy/compile_commands.json \
  <folder>

# Analyze each changed translation unit directly
clang-tidy -p ydb/public/sdk/cpp/build/clang-tidy \
  --config-file=ydb/public/sdk/cpp/.clang-tidy \
  --extra-arg=-Werror <changed.cpp>

# Only if clang-tidy reports a missing generated header, build the narrow target
./ya make --build relwithdebinfo -DUSER_CXXFLAGS=-Werror \
  --replace-result --add-result=.h \
  --add-protobuf-result --add-flatbuf-result \
  -o ydb/public/sdk/cpp/build/clang-tidy/generated <folder>

# Validate standalone-SDK production dependencies
python3 ydb/public/sdk/cpp/scripts/check_peerdirs.py
```

- Always pass `-DUSER_CXXFLAGS=-Werror`; `ya make` does not accept a standalone `-werror` option. Do not pass `-j` and do not force rebuilds.
- Lint production translation units only. Run clang-tidy directly; never pass `-A`, lint test paths, or invoke it through `ya make`, `-tA`, or a test recipe. Compile commands are sufficient, so build only the narrow changed target if a generated header is actually missing. Broaden analysis to affected production consumers for shared headers or common code. The SDK `.clang-tidy` deliberately enables expensive analyzer and bug-finding checks, so do not run it over the entire SDK unless the change is cross-cutting.
- Unit-test pure mapping, validation, settings, result parsing, retry decisions, and lifecycle behavior. Follow the target's existing GoogleTest or `Y_UNIT_TEST` framework.
- Use integration tests when correctness depends on a real YDB service, discovery, sessions, transactions, streaming, auth, or topic behavior. Keep recipe resource/timeout declarations in `ya.make` consistent with neighboring tests.
- For concurrency or shutdown fixes, add a deterministic regression test and use `--test-retries` to probe flakiness. Do not replace synchronization with sleeps.
- Before handoff, run the nearest affected test target; broaden to the affected client/subtree when shared headers, common client code, retry logic, driver state, or build dependencies change.

## Review checklist

Review the behavior, not just compilation:

- **API:** Is the public surface necessary, documented, self-contained, consistently named, and source-compatible? Are defaults and lifetime/ownership semantics clear?
- **Wire mapping:** Are all settings serialized correctly, including optional values and operation timeouts? Are all status, error, metadata, and response variants preserved?
- **Async safety:** Can callbacks race with cancellation or destruction, outlive captured references, complete twice, hang a future, deadlock during teardown, or run user code under a lock?
- **Retries and sessions:** Is retrying safe for this operation? Are idempotency, deadline propagation, backoff, session invalidation, endpoint selection, and streaming end-of-stream behavior correct?
- **Dependencies:** Does the change respect public/private and core/plugin boundaries, list direct `PEERDIR`s/SRCS, pass `check_peerdirs.py`, and remain exportable to the standalone SDK?
- **Static analysis:** Was the affected module built with warnings treated as errors (`-DUSER_CXXFLAGS=-Werror`) and checked with the SDK `.clang-tidy`? Were findings fixed rather than hidden by broad suppression?
- **Tests:** Do tests cover success, transport/server failure, invalid input, boundary values, cancellation/teardown, and the original regression without timing assumptions? Is an integration test needed?
- **User impact:** Do examples, comments, and `CHANGELOG.md` match the actual behavior, with no credentials, endpoints, generated output, or large artifacts accidentally committed?
