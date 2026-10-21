# PersQueue library helpers

Shared Topics helpers used across core and services (topic path/name parsing,
counters helpers, small utilities).
Core: [`ydb/core/persqueue/AGENTS.md`](../../core/persqueue/AGENTS.md).
Shared rules: [`RULES.md`](../../core/persqueue/RULES.md).

## Layout

* Root — shared constants.
* `topic_parser/` — topic/consumer name converters and PQ labels/counters helpers.
* `counter_time_keeper/`, `obfuscate/`, `tests/` — small utilities and test helpers.
* `deprecated/` — legacy helpers (e.g. read batch converter).

Public SDK parsing lives in
`ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public`.

Tests: `./ya make --build relwithdebinfo -tA ydb/library/persqueue/topic_parser`
