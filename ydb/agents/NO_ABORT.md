# No abort

Never `Y_ABORT` / `Y_ABORT_UNLESS` / `Y_VERIFY` / `Y_FAIL` (kills process).

Use `AFL_ENSURE(cond)("key", value)...` or reply with an error. Always add localization keys (tablet, path, partition/shard, table, switch value, …).

`Y_VERIFY_DEBUG` / `AFL_VERIFY_DEBUG` OK. Prefer `AFL_ENSURE` over `Y_ENSURE`.

Actors/tablets: inherit `IActorExceptionHandler` (log + recover). `AFL_ENSURE` only if the calling actor has it — otherwise the throw crashes the server.

**Review:** no new abort/verify; keys on every `AFL_ENSURE`; `IActorExceptionHandler` on new actors/tablets and anywhere `AFL_ENSURE` is used.
