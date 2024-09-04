#pragma once
#include <ydb/core/tx/columnshard/subscriber/events/tables_erased/event.h>
#include <ydb/core/tx/columnshard/subscriber/events/transaction_completed/event.h>
#include <ydb/core/tx/columnshard/subscriber/events/writes_completed/event.h>
#include <memory>
#include <set>

namespace NKikimr::NColumnShard::NSubscriber {

class ISubscriber {
public:
    virtual void OnEvent(const std::shared_ptr<ISubscriptionEvent>& ev) = 0;

    virtual std::set<EEventType> GetEventTypes() const = 0;
    virtual bool IsFinished() const = 0;

    virtual ~ISubscriber() = default;
};

class TSubscriberBase: public ISubscriber {
protected:
    virtual void DoOnEvent(const NSubscriber::TEventTablesErased&) {};
    virtual void DoOnEvent(const NSubscriber::TEventTransactionCompleted&) {};
    virtual void DoOnEvent(const NSubscriber::TEventWritesCompleted&) {};
public:
    void OnEvent(const std::shared_ptr<ISubscriptionEvent>& ev) override {
        switch(ev->GetType()) {
            case EEventType::Undefined:
                break;//AFL_VERIFY(false);
            case EEventType::TablesErased:
                DoOnEvent(static_cast<const NSubscriber::TEventTablesErased&>(*ev.get()));
                break;
            case EEventType::TransactionCompleted:
                DoOnEvent(static_cast<const NSubscriber::TEventTransactionCompleted&>(*ev.get()));
                break;
            case EEventType::WritesCompleted:
                DoOnEvent(static_cast<const NSubscriber::TEventWritesCompleted&>(*ev.get()));
                break;
        }
    }
};

} //namespace NKikimr::NColumnShard::NSubscriber