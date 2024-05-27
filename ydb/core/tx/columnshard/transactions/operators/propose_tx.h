#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/transactions/tx_controller.h>

namespace NKikimr::NColumnShard {

class IProposeTxOperator: public TTxController::ITransactionOperator {
private:
    using TBase = TTxController::ITransactionOperator;
protected:
    virtual bool DoCheckTxInfoForReply(const TFullTxInfo& originalTxInfo) const override {
        return GetTxInfo() == originalTxInfo;
    }
    virtual void DoSendReply(TColumnShard& owner, const TActorContext& ctx) override;
    virtual bool DoCheckAllowUpdate(const TFullTxInfo& currentTxInfo) const override {
        if (!currentTxInfo.SeqNo || !GetTxInfo().SeqNo) {
            return true;
        }
        if (currentTxInfo.SeqNo->Generation > GetTxInfo().SeqNo->Generation) {
            return false;
        }
        if (currentTxInfo.SeqNo->Generation < GetTxInfo().SeqNo->Generation) {
            return true;
        }
        return currentTxInfo.SeqNo->Round < GetTxInfo().SeqNo->Round;
    }
public:
    using TBase::TBase;

    bool TxWithDeadline() const override {
        return false;
    }
};

}
