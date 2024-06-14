#pragma once

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/transactions/tx_controller.h>

namespace NKikimr::NColumnShard {

class ISSTransactionOperator: public TTxController::ITransactionOperator {
private:
    using TBase = TTxController::ITransactionOperator;
protected:
    virtual void DoSendReply(TColumnShard& owner, const TActorContext& ctx) override;
public:
    using TBase::TBase;
};

}
