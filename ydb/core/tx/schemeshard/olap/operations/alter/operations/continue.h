#pragma once
#include <ydb/core/tx/schemeshard/schemeshard_tx_infly.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TEvoluteAlterColumnTable: public TSubOperation {
    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override;

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override;

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterColumnTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override;
};

}
