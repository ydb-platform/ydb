# No abort

Do **not** use `Y_ABORT`, `Y_ABORT_UNLESS`, `Y_VERIFY`, `Y_FAIL` — they kill the process.

Use `AFL_ENSURE(cond)("key", value)...` (throws; recoverable) or reply with an error.

Always attach localization context for the relevant objects: tablet id, path/name, partition/shard, table, datashard, switch value — whatever identifies the failure (same data as nearby logs).

`Y_VERIFY_DEBUG` / `AFL_VERIFY_DEBUG` are OK (debug-only). Prefer `AFL_ENSURE` over `Y_ENSURE`.

**Review:** reject new abort/verify; require diagnostic keys on every `AFL_ENSURE`.
