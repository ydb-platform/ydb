#pragma once
#include <ydb/core/tx/schemeshard/schemeshard_tx_infly.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_common.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

using namespace NKikimr;
using namespace NSchemeShard;

class TStartAlterColumnTable: public TSubOperation {
private:
    TTxState::ETxState NextState(TTxState::ETxState /*state*/) const override {
        return TTxState::Done;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState /*state*/) override {
        return MakeHolder<TDone>(OperationId);
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override;

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterColumnTable");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override;
};

}
