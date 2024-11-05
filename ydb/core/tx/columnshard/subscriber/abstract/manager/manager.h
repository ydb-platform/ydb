#pragma once
#include <ydb/core/tx/columnshard/subscriber/abstract/subscriber/subscriber.h>
#include <ydb/core/tx/columnshard/subscriber/abstract/events/event.h>

#include <ydb/library/actors/core/log.h>
#include <util/generic/hash.h>
#include <vector>

namespace NKikimr::NColumnShard::NSubscriber {

class TManager {
private:
    class TSharedPtrHashContainer {
    private:
        std::shared_ptr<ISubscriber> Object;
    public:
        TSharedPtrHashContainer(const std::shared_ptr<ISubscriber>& obj)
            : Object(obj)
        {

        }

        TSharedPtrHashContainer() {
            AFL_VERIFY(!!Object);
        }

        ISubscriber* operator->() const {
            return Object.get();
        }

        explicit operator size_t() const {
            return (size_t)Object.get();
        }

        bool operator==(const TSharedPtrHashContainer& object) const {
            return Object == object.Object;
        }
    };
    TColumnShard& Owner;
    THashMap<EEventType, THashSet<TSharedPtrHashContainer>> Subscribers;
public:
    TManager(TColumnShard& owner)
        : Owner(owner)
    {

    }

    void RegisterSubscriber(const std::shared_ptr<ISubscriber>& s) {
        for (auto&& et : s->GetEventTypes()) {
            AFL_VERIFY(Subscribers[et].emplace(s).second);
        }
    }

    void OnEvent(const std::shared_ptr<ISubscriptionEvent>& ev) {
        auto it = Subscribers.find(ev->GetType());
        if (it == Subscribers.end()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "on_event_subscriber_skipped")("event", ev->GetType())("details", ev->DebugString());
            return;
        } else {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "on_event_subscriber")("event", ev->GetType())("details", ev->DebugString());
        }
        std::vector<TSharedPtrHashContainer> toRemove;
        for (auto&& i : it->second) {
            i->OnEvent(ev, Owner);
            if (i->IsFinished()) {
                toRemove.emplace_back(i);
            }
        }
        for (auto&& i : toRemove) {
            for (auto&& evType : i->GetEventTypes()) {
                auto it = Subscribers.find(evType);
                AFL_VERIFY(it != Subscribers.end());
                it->second.erase(i);
                if (it->second.empty()) {
                    Subscribers.erase(it);
                }
            }
        }
    }
};
}