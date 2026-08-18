# ADR: WASM UDF objects (static object_framework + ui64 handles)

## Status

Accepted (MVP).

## Context

Нужны stateful WASM UDF в стиле LogFeller `ParseTskv`:

```sql
$fn = YQL::Udf(AsAtom("Mod.Parse"), Void(), Void(), AsAtom($blob));
SELECT $fn(row) ...
```

Требования:

- много объектов на один query;
- host передаёт в методы только **ui64** handle;
- TypeConfig blob **opaque** для host;
- реестр объектов не должен быть отдельным wasm в `required_libraries`.

## Decision

1. **Static lib** `ydb/services/udf_store/wasm/object_framework/` с C API  
   `ObjectFrameworkCreate/Get/Destroy`. UDF-модуль линкует через `PEERDIR`.
2. Манифест: секция **`objects[]`** с `create_export` / `destroy_export` / `methods[]`.  
   Methods с `yql_binding=type_config_callable` разворачиваются в `functions` с create/call/destroy.
3. Host: **`TWasmConfiguredCallable`** — pin blob на generation, create → ui64, call(handle, args).
4. Per-query compartment несёт **`Generation`**; при смене generation object recreates.
5. `required_libraries` остаётся для sdk / пользовательских libs; **не** для framework.

## Consequences

- У каждого module image свой реестр — handle валиден только с exports этого модуля.
- Нужен sdk (или иной malloc) для аллокации instance’ов в wasm.
- Zero-copy host `TString` → durable wasm view без pin/copy в linear memory невозможен.
- Phase 2 (не сделано): Resource/`New`/`Call`; schema-driven YQL types.
