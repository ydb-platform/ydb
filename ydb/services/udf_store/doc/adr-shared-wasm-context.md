# ADR: Shared WASM context + SELECT via Snapshot

## Status

Accepted (MVP).

## Context

Нужен общий mutable context на запрос: несколько фильтров / объектов / модулей пишут статистику в одно место; затем результат виден в `SELECT`.

Два отдельных вопроса:

1. **Share** — где живёт state и как его передают между вызовами.
2. **Observe** — как YQL видит накопленное (linear memory сама по себе не SELECT-ится).

## Decision

### Share

- Context — **объект в per-query compartment** с identity **`ui64` handle** (id в реестре `object_framework`).
- **MVP example** (`examples/ctx/`): один module image — `Ctx::New` / `CountRow` / `CountPositive` / `Snapshot` линкуют static `object_framework` через `PEERDIR`. Filters и snapshot шарят один реестр.
- Cross-module (опционально, не в examples): реестр в **shared wasm library** (`required_libraries`); static `object_framework` внутри одного module image **не** шарит handles с другим.
- В вызовы фильтров передаётся **`uint64` ctx** первым (или явным) аргументом.
- `TypeConfigCallable` остаётся для per-object config; context — отдельный handle.

### Observe (SELECT)

- Явный export/метод **`Snapshot(ctx) → string`** (MVP: текстовый/JSON dump).
- Host возвращает обычный `TUnboxedValue`; без Snapshot колонки со stats не будет.
- SQL должен **сначала форсировать** прогон фильтров, потом Snapshot (lazy MiniKQL).

Рекомендуемый паттерн:

```sql
$ctx = Ctx::New();
$mapped = ListMap($vals, ($x) -> { RETURN Ctx::CountRow($ctx, $x) });
SELECT $mapped AS rows, Ctx::Snapshot($ctx) AS stats;
```

Антипаттерн для итоговой статистики запроса:

```sql
SELECT Ctx::CountRow($ctx, x), Ctx::Snapshot($ctx) FROM Input;
```

### Manifest

- Object methods могут быть `yql_binding: "plain"` (не только `type_config_callable`).
- Plain method: YQL-имя = `methods[].name`, wasm export = `methods[].export`.
- При наличии `create_export` хост синтезирует **`New` → ui64`** (plain), если имя свободно.
- Optional `"export"` на `functions[]` — wasm-имя, если отличается от YQL `name`.

## Consequences

- Нужен sdk/`malloc` для аллокации instance’ов context в wasm.
- Handle валиден только в текущем compartment/`Generation`.
- Позже: YQL Resource вместо голого ui64; typed Snapshot; aggregate-обёртка.
