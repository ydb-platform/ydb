#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard::NSubscriber {

//TODO rename to to TEventTxCompleted
class TEventTransactionCompleted: public ISubscriptionEvent {
private:
    using TBase = ISubscriptionEvent;
    YDB_READONLY_DEF(ui64, TxId);
    TString DoDebugString() const override {
         return "tx_id=" + std::to_string(TxId);
    }
public:
    TEventTransactionCompleted(const ui64 txId)
        : TBase(EEventType::TransactionCompleted)
        , TxId(txId)
    {
    }
};

} //namespace NKikimr::NColumnShard::NSubscriber