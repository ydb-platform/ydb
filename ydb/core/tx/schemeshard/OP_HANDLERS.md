# Per-op handlers

## Problem

Adding a schemeshard operation today touches 5+ central files: the proto enum, `MakeOperationParts` factory switch (~144 cases), `ExtractChangingPaths` audit switch (~130 cases), `GetOperationClass` (~131 cases), and others. Every op's logic is scattered across the schemeshard tree.

## End-state design (where we're going)

**Each op owns its handlers in its own `.cpp`. The codegen emits the central dispatch directly from the proto enum. There is no central switch, no manifest, no wrapper.**

What an op author writes — full surface, forever:

```cpp
// In schemeshard__operation_<your_op>.cpp:

YDB_DEFINE_OP_FACTORY(ESchemeOpFoo, op, tx, ctx) {
    return {CreateNewFoo(op.NextPartId(), tx)};
}

YDB_DEFINE_OP_AUDIT(ESchemeOpFoo, tx, paths) {
    paths.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateFoo().GetName()}));
}
```

That's it. Nothing else to edit. Compile-time errors catch typos and signature drift. Linker errors catch missing definitions.

What the central dispatch looks like at end state:

```cpp
TVector<TString> ExtractChangingPaths(const NKikimrSchemeOp::TModifyScheme& tx) {
    return NGenerated::NOpHandlers::CollectChangingPaths(tx);
}
```

One line. The codegen-emitted `CollectChangingPaths` is the dispatch.

### Audit log example end-to-end

```cpp
// A CREATE TABLE request arrives:
NKikimrSchemeOp::TModifyScheme tx;
tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
tx.SetWorkingDir("/Root/Db");
tx.MutableCreateTable()->SetName("Users");

// Audit subsystem calls:
const auto fragment = MakeAuditLogFragment(tx);

// fragment.Paths == {"/Root/Db/Users"}
```

What happened:

1. `MakeAuditLogFragment` calls `ExtractChangingPaths(tx)`.
2. End state: `ExtractChangingPaths` is one-line delegation to `NGenerated::NOpHandlers::CollectChangingPaths(tx)`.
3. Codegen-emitted switch routes by `tx.GetOperationType()` to `CollectChangingPaths_ESchemeOpCreateTable`.
4. That handler (defined in `schemeshard__operation_create_table.cpp` via `YDB_DEFINE_OP_AUDIT`) runs:
   ```cpp
   paths.emplace_back(NKikimr::JoinPath({tx.GetWorkingDir(), tx.GetCreateTable().GetName()}));
   ```
5. Audit fragment's `Paths` field carries `["/Root/Db/Users"]`.

The path from the request to the audit fragment is fully type-checked at compile time and resolved at link time. No virtual dispatch, no `if constexpr` chains, no runtime registry.

## Migration-time scaffolding (temporary)

Until every op is migrated we need a way to incrementally move ops without breaking everything else. **The pieces below are scaffolding — they all get deleted at the end.**

| Scaffolding | Purpose | Removed when |
|---|---|---|
| `op_handler_overrides.yaml` | Tells codegen which ops have migrated; gates the macro `static_assert` so unmigrated ops can't accidentally use the macros. | Every op is migrated. |
| `Try*Dispatch` returning `std::optional` | Lets the codegen handle migrated ops while un-migrated ops fall through to legacy. | Codegen flips to exhaustive `Dispatch`. |
| Legacy switches in `schemeshard__operation.cpp` and `schemeshard_audit_log_fragment.cpp` | Handle un-migrated ops. Each migration deletes one case. | Empty switch — file deleted or function collapsed to the codegen call. |
| The `if (auto handled = TryDispatch(...)) return *handled;` prelude in central functions | Routes migrated ops through codegen, falls through to legacy switch otherwise. | Replaced by direct call to the exhaustive codegen function. |

So the **mid-migration** version of the op author flow has three steps:

1. Write the handler in your op's `.cpp` (same as end state).
2. Add the op name to `op_handler_overrides.yaml` (one line).
3. Delete the matching case from the central legacy switch.

After full migration only step 1 remains.

## Comparison

| Concern | Today (pre-refactor) | Mid-migration | End state |
|---|---|---|---|
| Files to touch when adding a new op | 5+ central files + op's .cpp | YAML + central switch + op's .cpp | op's .cpp only |
| Where is `Foo`'s dispatch logic? | Search across factory switch, audit switch, class switch, ... | Same, but for un-migrated ops only | One file: `schemeshard__operation_foo.cpp` |
| Catch a missing handler | Manual review | Linker error (extern unresolved if op in YAML) | Linker error always |
| Catch wrong signature | Compile error at the call site | Compile error in macro `static_assert` | Same |
| Central audit switch length | ~600 lines | shrinks each migration | one line |

## Where we are

- `ESchemeOpCreateTable` migrated as the pilot (1 of ~130).
- All scaffolding in place: YAML, codegen, macros, central preludes.

## End-state cleanup (the "we're done" PR)

When the YAML lists every `EOperationType`:

1. Flip the codegen template's `default: return std::nullopt` branch to `default: Y_UNREACHABLE()` (one-line config). `TryDispatch` becomes exhaustive `Dispatch`.
2. Replace the central function bodies with `return NGenerated::NOpHandlers::Dispatch(...);`.
3. Delete the legacy switches (~1000 LOC removed across `schemeshard__operation.cpp` and `schemeshard_audit_log_fragment.cpp`).
4. Delete `op_handler_overrides.yaml` — strict mode doesn't read it.
5. Drop the `static_assert` predicate from the `YDB_DEFINE_OP_*` macros (no YAML to gate against).

After this PR: missing handlers are linker errors permanently. Adding a new op = write the function. Done.

## Failure modes (mid-migration AND after)

| Mistake | Where caught |
|---|---|
| Used macro, op not in YAML (mid-migration only) | Compile (`static_assert` with file-pointing message) |
| Wrong signature | Compile (macro bakes in the signature) |
| Typo in op name | Compile (`NKikimrSchemeOp::Typo` undeclared) |
| YAML lists nonexistent op (mid-migration only) | Codegen (validates against proto enum) |
| In YAML, function not defined | Linker (extern unresolved) |
| End-state: missing handler for a new op | Linker |

No silent failures. Every mistake is caught before the change reaches users.

## Testing

The per-op pattern unlocks a real test pyramid. Today every schemeshard op test boots the full actor runtime; only 4 of 28 test targets are SMALL. The handlers exposed by this PR are pure functions over protos and synthetic operation context, so they can be exercised at function granularity.

### Tier 1 — pure handlers, no fixtures (SMALL, ms-scale)

Audit handlers take only `(const TModifyScheme&, TVector<TString>&)` — no operation context. Call them directly:

```cpp
NSO::TModifyScheme tx;
tx.SetOperationType(NSO::ESchemeOpCreateTable);
tx.SetWorkingDir("/Root/Db");
tx.MutableCreateTable()->SetName("Users");

TVector<TString> paths;
NHandlers::CollectChangingPaths_ESchemeOpCreateTable(tx, paths);
// paths == ["/Root/Db/Users"]
```

`ut_op_handlers/parity_helpers.h` provides `AssertOpAuditPaths(opType, populate, expected)` that exercises the handler AND the public `MakeAuditLogFragment` surface, asserting both produce the same paths.

### Tier 2 — factory handlers with a fake context (SMALL, ms-scale)

Factory handlers take `TOperationContext&`. For ops whose body doesn't dereference `ctx.SS` / `ctx.GetTxc()` (e.g. CreateTable's non-copy branch), `ut_op_handlers/fake_operation_context.h` provides `TFakeOperationContextHarness`:

```cpp
NTesting::TFakeOperationContextHarness fakeCtx;
TOperation op(TTxId(42));
NSO::TModifyScheme tx;
tx.SetOperationType(NSO::ESchemeOpCreateTable);
tx.SetWorkingDir("/Root/Db");
tx.MutableCreateTable()->SetName("Orders");

const auto parts = NHandlers::MakeOperationParts_ESchemeOpCreateTable(op, tx, fakeCtx.Get());
// parts.size() == 1, parts[0] is the constructed sub-op
```

The fake's `SS` is `nullptr` and `Ctx`/`Txc` reference uninitialized storage — handlers that touch them crash loudly (segfault or sanitizer hit). This is a feature: it forces ops with deep schema dependencies to use Tier 3.

### Tier 3 — full runtime (MEDIUM/LARGE, existing tests)

State machine progress, persistence, multi-op transactions, reboot recovery — same `TTestEnv` harness as today. Unchanged.

### Cross-op invariants

The codegen-emitted `IsOpRegistered` makes it cheap to assert properties across every migrated op:

```cpp
for (auto opType : AllRegisteredOps()) {
    auto paths = NHandlers::TryCollectChangingPaths(MinimalTx(opType));
    UNIT_ASSERT_C(paths.has_value() && !paths->empty(),
                  EOperationType_Name(opType) << " produced no audit paths");
}
```

Add invariants here as the migration progresses (e.g., "every Create-class op produces ≥1 path", "every audit handler is idempotent").

### Migration parity

Before deleting an op's case from the legacy switch, write an `AssertOpAuditPaths` golden test with the expected paths derived from the legacy code. The test stays as the regression guard after the legacy case is gone.

## Tooling

`ss_tool` (`ydb/tools/ss_tool/`) is the by-aspect view of what the schemeshard supports — the cross-cutting counterpart to the per-op `.cpp` view. Permanent surface, useful long after migration finishes.

```
ss_tool ops list                  # every op the schemeshard knows about
ss_tool ops show <OpName>         # everything about one op
ss_tool ops migration-status      # progress dashboard (mid-migration only)
```

During migration the rows include `IsRegistered` so the tool doubles as a progress dashboard. The fields the tool exposes will grow as the codegen learns more about each op (e.g. inferred audit field, factory signature class, related sub-ops); the tool framework stays the same.

Only `migration-status` is migration-scoped — it returns "100% — done" once the YAML is empty and can be removed in the cleanup PR. `list` and `show` stay.
