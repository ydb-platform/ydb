#pragma once
#include <ydb/core/tx/columnshard/subscriber/events/tables_erased/event.h>
#include <ydb/core/tx/columnshard/subscriber/events/transaction_completed/event.h>
#include <ydb/core/tx/columnshard/subscriber/events/writes_completed/event.h>
#include <memory>
#include <set>

namespace NKikimr::NColumnShard::NSubscriber {

class ISubscriber {
protected:
public:
    virtual void OnEvent(const std::shared_ptr<ISubscriptionEvent>& ev);

    virtual std::set<EEventType> GetEventTypes() const = 0;
    virtual bool IsFinished() const = 0;

    virtual ~ISubscriber() = default;
};

template<typename TDerived>
class TSubscriberBase: public ISubscriber {
protected:
    template<typename TEvent> void DoOnEvent(const TEvent&) {
    }
public:
    void OnEvent(const std::shared_ptr<ISubscriptionEvent>& ev) override {
        auto* derived = static_cast<TDerived*>(this);
        switch(ev->GetType()) {
            case EEventType::Undefined:
                break;//AFL_VERIFY(false);
            case EEventType::TablesErased:
                derived->DoOnEvent(static_cast<const NSubscriber::TEventTablesErased&>(*ev.get()));
                break;
            case EEventType::TransactionCompleted:
                derived->DoOnEvent(static_cast<const NSubscriber::TEventTransactionCompleted&>(*ev.get()));
                break;
            case EEventType::WritesCompleted:
                derived->DoOnEvent(static_cast<const NSubscriber::TEventWritesCompleted&>(*ev.get()));
                break;
        }
    }
};

} //namespace NKikimr::NColumnShard::NSubscriber