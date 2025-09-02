#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>

namespace NKikimr::NColumnShard::NSubscriber {

class TEventTxCompleted: public ISubscriptionEvent {
private:
    using TBase = ISubscriptionEvent;
    YDB_READONLY_DEF(ui64, TxId);
    virtual TString DoDebugString() const override;
public:
    TEventTxCompleted(const ui64 txId)
        : TBase(EEventType::TxCompleted)
        , TxId(txId)
    {
    }
};

} //namespace NKikimr::NColumnShard::NSubscriber