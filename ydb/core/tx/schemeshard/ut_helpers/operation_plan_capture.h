#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_operation_factory.h>
#include <ydb/core/tx/schemeshard/schemeshard_operation_plan.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NSchemeShardUT_Private {

// Observes operation plans through the factory SchemeShard already consults, installed in the
// node's AppData for the lifetime of the object. A sealed plan is captured before any part of
// its operation is constructed, which is before the first propose by construction.
class TOperationPlanCapture : public NKikimr::NSchemeShard::IOperationFactory {
    using TSealedOperationPlan = NKikimr::NSchemeShard::TSealedOperationPlan;

public:
    struct TCaptured {
        ui64 TxId;
        TSealedOperationPlan Plan;
    };

    explicit TOperationPlanCapture(NActors::TTestActorRuntime& runtime, ui32 nodeIndex = 0)
        : AppData(runtime.GetAppData(nodeIndex))
        , Inner(AppData.SchemeOperationFactory)
    {
        Y_ABORT_UNLESS(Inner);
        AppData.SchemeOperationFactory = this;
    }

    ~TOperationPlanCapture() {
        AppData.SchemeOperationFactory = Inner;
    }

    TVector<TIntrusivePtr<NKikimr::NSchemeShard::ISubOperation>> MakeOperationParts(
            const NKikimr::NSchemeShard::TOperation& op,
            const TTxTransaction& tx,
            NKikimr::NSchemeShard::TOperationContext& ctx) const override
    {
        return Inner->MakeOperationParts(op, tx, ctx);
    }

    TIntrusivePtr<NKikimr::NSchemeShard::ISubOperation> MakePlannedPart(
            const NKikimr::NSchemeShard::TOperationId& id,
            const TSealedOperationPlan& plan,
            const NKikimr::NSchemeShard::TPartBlueprint& blueprint,
            NKikimr::NSchemeShard::TOperationContext& ctx) const override
    {
        return Inner->MakePlannedPart(id, plan, blueprint, ctx);
    }

    void OnPlanSealed(ui64 txId, const TSealedOperationPlan& plan) const override {
        Sealed.push_back(TCaptured{txId, plan});
    }

    const TCaptured& LastSealed() const {
        UNIT_ASSERT_C(!Sealed.empty(), "no plan was sealed");
        return Sealed.back();
    }

    const TSealedOperationPlan* FindSealed(ui64 txId) const {
        for (const auto& captured : Sealed) {
            if (captured.TxId == txId) {
                return &captured.Plan;
            }
        }
        return nullptr;
    }

    mutable TVector<TCaptured> Sealed;

private:
    NKikimr::TAppData& AppData;
    const NKikimr::NSchemeShard::IOperationFactory* Inner;
};

} // namespace NSchemeShardUT_Private
