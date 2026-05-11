#pragma once

#include <ydb/core/tx/schemeshard/schemeshard__operation_db_changes.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_memory_changes.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_side_effects.h>

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <ydb/library/actors/core/actor.h>

#include <optional>

namespace NKikimr::NSchemeShard::NTesting {

// Minimal TOperationContext for unit tests of per-op handlers whose body
// does NOT dereference SS, Ctx, or Txc.
//
//   * SS is set to nullptr — segfault if a handler dereferences it.
//   * Ctx and Txc reference uninitialized aligned storage — UB if accessed,
//     normally manifests as a sanitizer hit or null-deref crash.
//   * SideEffects, MemChanges, DbChanges are real default-constructed
//     instances; tests can inspect them after the handler runs.
//
// Use case: tier-2 tests of factory handlers that produce sub-ops via pure
// proto arithmetic (e.g. ESchemeOpCreateTable's non-copy branch). For
// handlers that touch the schema or DB, fall back to the full TTestEnv
// harness in ut_helpers/.
class TFakeOperationContextHarness {
public:
    TFakeOperationContextHarness();

    TOperationContext& Get() { return *Context_; }

    const TSideEffects& OnComplete() const { return *SideEffects_; }
    const TMemoryChanges& MemChanges() const { return *MemChanges_; }
    const TStorageChanges& DbChanges() const { return *DbChanges_; }

private:
    alignas(NTabletFlatExecutor::TTransactionContext)
        char TxcStorage_[sizeof(NTabletFlatExecutor::TTransactionContext)];
    alignas(NActors::TActorContext)
        char CtxStorage_[sizeof(NActors::TActorContext)];

    TIntrusivePtr<TSideEffects> SideEffects_;
    TIntrusivePtr<TMemoryChanges> MemChanges_;
    TIntrusivePtr<TStorageChanges> DbChanges_;
    std::optional<TOperationContext> Context_;
};

inline TFakeOperationContextHarness::TFakeOperationContextHarness()
    : SideEffects_(MakeIntrusive<TSideEffects>())
    , MemChanges_(MakeIntrusive<TMemoryChanges>())
    , DbChanges_(MakeIntrusive<TStorageChanges>())
{
    auto& fakeTxc = *reinterpret_cast<NTabletFlatExecutor::TTransactionContext*>(TxcStorage_);
    auto& fakeCtx = *reinterpret_cast<const NActors::TActorContext*>(CtxStorage_);
    Context_.emplace(
        /* SS = */ nullptr,
        fakeTxc,
        fakeCtx,
        *SideEffects_,
        *MemChanges_,
        *DbChanges_);
}

} // namespace NKikimr::NSchemeShard::NTesting
