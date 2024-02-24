#pragma once
#include "tx_controller.h"

namespace NKikimr::NColumnShard {

class TColumnShard;

class TProposeTransactionBase : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
public:
    TProposeTransactionBase(TColumnShard* self)
        : TBase(self)
    {}

protected:
    void ProposeTransaction(const TTxController::TBasicTxInfo& txInfo, const TString& txBody, const TActorId source, const ui64 cookie, TTransactionContext& txc);

    virtual void OnProposeResult(TTxController::TProposeResult& proposeResult, const TTxController::TTxInfo& txInfo) = 0;
    virtual void OnProposeError(TTxController::TProposeResult& proposeResult, const TTxController::TBasicTxInfo& txInfo) = 0;
};


}
