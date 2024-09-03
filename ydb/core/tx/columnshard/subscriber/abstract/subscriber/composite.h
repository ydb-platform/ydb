#pragma once
#include "subscriber.h"


namespace NKikimr::NColumnShard::NSubscriber {

class TCompositeSubscriber: public TSubscriberBase {
    std::vector<std::shared_ptr<NSubscriber::ISubscriber>> Subscribers;
public:
    TCompositeSubscriber(std::initializer_list<std::shared_ptr< NSubscriber::ISubscriber>> subscribers)
        : Subscribers(subscribers) 
    {
    }
    bool IsFinished() const override {
        return std::all_of(cbegin(Subscribers), cend(Subscribers), [](auto s){ return s->IsFinished();});
    }
    std::set<NSubscriber::EEventType> GetEventTypes() const override {
        return std::accumulate(cbegin(Subscribers), cend(Subscribers), std::set<EEventType>{}, [](auto a, const auto& s){ 
            const auto& events = s->GetEventTypes();
            a.insert(cbegin(events), cend(events)); 
            return a;
        });
    }
    void OnEvent(const std::shared_ptr<NSubscriber::ISubscriptionEvent>& ev) override {
        //For the sake of simplicity, we don't filter events consumed by each particular subscriber. All subscribers are called with every incoming event
        //Can be improved in case of poor perfomance 
        for (auto s: Subscribers) {
            s->OnEvent(ev);
        }
    }
};

} //namespace NKikimr::NColumnShard::NSubscriber