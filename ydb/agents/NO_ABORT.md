# No abort

Never `Y_ABORT` / `Y_ABORT_UNLESS` / `Y_VERIFY` / `Y_FAIL` (kills process).

Use `AFL_ENSURE(cond)("key", value)...` or reply with an error. Always add localization keys (tablet, path, partition/shard, table, switch value, …).

`Y_VERIFY_DEBUG` / `AFL_VERIFY_DEBUG` OK. Prefer `AFL_ENSURE` over `Y_ENSURE`.

Actors: wrap the actor base in `TActorWithExceptionHandler<TBase>` and implement `OnUnhandledException` (log + recover). Tablets use `TTabletActor<TBase>` to preserve the tablet restart policy. `AFL_ENSURE` only if the calling actor handles the exception — otherwise the throw crashes the server.

**Review:** no new abort/verify; keys on every `AFL_ENSURE`; an explicit exception-handling base on new actors/tablets and anywhere `AFL_ENSURE` is used.
