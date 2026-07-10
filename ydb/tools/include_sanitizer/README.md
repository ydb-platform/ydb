# ydb include sanitizer (report-only v1)

A Python orchestrator built on top of **clang-include-cleaner** that identifies
`#include` directives in `ydb/` headers that are either fully unused, or are
used only by some `.cpp` translation units (TUs) that include the header.

It produces:

- a machine-readable include dependency graph,
- a ranked list of "hot" headers,
- per-file unified-diff previews of the recommended cleanups.

**No source files are modified.** The tool is strictly report-only in this
version.

## Status

This is v1. Scope:

- analyzer engine: `clang-include-cleaner` (LLVM)
- input: `compile_commands.json` covering TUs under `ydb/`
- output: JSON, CSV, Markdown, unified-diff patches, DOT graph
- coverage: `ydb/**`; `contrib/`, `util/`, `library/cpp/`, generated files are
  filtered out of the verdict layer
- no auto-apply; suggestions only

## Layout

```
ydb/tools/include_sanitizer/
  compdb/         compile_commands.json generation from `ya make`
  analyze/        per-TU runner around clang-include-cleaner
  aggregate/      cross-TU aggregator + dependency graph builder
  report/         verdict + diff-preview emission
  bin/            CLI entry point
  pilot.py        end-to-end pilot harness without clang/build
  tests/          unit + integration tests
```

## Tests

```bash
python3 -m unittest discover -s ydb/tools/include_sanitizer/tests -v
```

## Pipeline

```
ya make --> [compdb wrapper records clang invocations] --> compile_commands.json
                                                                    |
                                                                    v
                                            per-TU runner (clang-include-cleaner)
                                                                    |
                                                                    v
                                            per-TU JSON: used symbols + provenance
                                                                    |
                                                                    v
                                            cross-TU aggregator (keep/remove/move)
                                                                    |
                                                                    v
                            graph.json + DOT + hot_headers.csv + diffs/*.patch + summary.md
```

## Quick start — pilot (no clang, no build)

To exercise the full aggregate→report pipeline on real `ydb/` files
without running `ya make` or `clang-include-cleaner` (useful for tool
development and CI):

```bash
ydb/tools/include_sanitizer/bin/sanitize_includes pilot \
    --subdir ydb/core/base --report -v
```

This will scan real include directives, synthesize a fake verdict per
header, run the aggregator and report layers, and write outputs under
`ydb/tools/include_sanitizer/reports/`.

## Quick start — real run

```bash
# 1. Generate compile_commands.json AND analyze in-line during the build.
#
#    Wrapper mode (preferred). The tool:
#      - patches build/scripts/retry_cc.py and build/scripts/clang_wrapper.py
#        with a small recorder shim,
#      - auto-injects -DRETRY=yes / RETRY=yes (so retry_cc.py wraps every
#        C++ compile in OPENSOURCE builds),
#      - runs the ya make command,
#      - for every compile, BEFORE invoking clang, records the command and
#        runs clang-include-cleaner with the live build_root (so generated
#        *.pb.h files are still available),
#      - restores both build/scripts/*.py files to byte-identical originals
#        and aggregates the recorded entries.
#
#    Why during the build? ya's per-TU build_root (which holds the
#    generated *.pb.h, *.fbs.h, ... files) is ephemeral. A separate
#    analyze step run AFTER the build typically fails with "file not
#    found" on those headers.
ydb/tools/include_sanitizer/bin/sanitize_includes compdb --mode wrapper -- \
    ./ya make --build relwithdebinfo ydb/core/base

#    Pass --no-inline-analyze to skip in-line analysis (faster build, but
#    you must arrange to keep generated headers around for a later
#    analyze step).
#    Pass --cleaner-bin /path/to/clang-include-cleaner if autodiscovery
#    misses your install.

#    iwyu mode: aggregate entries previously written to .compdb_entries/
#    (e.g. from an earlier wrapper-mode run or an IWYU=yes build patched
#    to dump compile commands).
ydb/tools/include_sanitizer/bin/sanitize_includes compdb --mode iwyu

# 2. Run clang-include-cleaner per TU (parallel, cached)
ydb/tools/include_sanitizer/bin/sanitize_includes analyze \
    --subdir ydb/core/base --jobs $(nproc)

# 3. Cross-TU aggregate into verdicts and a graph
ydb/tools/include_sanitizer/bin/sanitize_includes aggregate

# 4. Produce the report (graphs, CSV, summary, per-file diff previews)
ydb/tools/include_sanitizer/bin/sanitize_includes report --top 100

# Or all at once:
ydb/tools/include_sanitizer/bin/sanitize_includes all --subdir ydb/core/base
```

Output goes under `ydb/tools/include_sanitizer/reports/`.

## How verdicts are computed

For every (header `H`, `#include I` in `H`) pair:

- `S(H, I)` = symbols that `I` (transitively) provides to consumers of `H`.
- `H_uses(I)` = `H` itself uses any symbol in `S(H, I)` (inline functions,
  templates, member types, constexpr inits, etc.).
- `consumers(H)` = TUs that include `H` directly or transitively.
- For each consumer `C` that is a `.cpp` file: does `C` use a symbol in
  `S(H, I)` that no other include of `C` already provides?

Verdict:

| Condition                                                              | Verdict      |
|------------------------------------------------------------------------|--------------|
| `H_uses(I)`                                                            | `keep`       |
| No consumer (header or TU) needs `I`                                   | `remove`     |
| Only `.cpp` consumers need it; none of them already include `I`        | `move-to-cpp`|
| Some `.h` consumer needs `I` through `H`                               | `keep` (deferred) |

`IWYU pragma: keep` / `IWYU pragma: export` / `IWYU pragma: private` are
respected. Generated headers (`*.pb.h`, anything under `$BUILD_ROOT/`) are
never flagged for removal.

## Requirements

- Python 3.8+
- `clang-include-cleaner` available on `PATH` (LLVM 17+; older versions
  understand fewer flags). If `ya` has bundled it under
  `~/.ya/tools/.../bin/clang-include-cleaner`, the tool will pick that up.
- For wrapper-mode compdb generation: the ability to run `./ya make`.

## Out of scope for v1

- Auto-applying patches.
- Adding forward-declaration suggestions.
- Touching anything outside `ydb/`.
- Modifying `ya.make` `PEERDIR`s.

These are planned for follow-up phases once we have confidence in v1's
verdicts.
