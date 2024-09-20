#include "counters.h"

#include <util/system/guard.h>

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {


std::shared_ptr<TSubscriberTypeCounters> TSubscriberCounters::GetTypeCounters(const TString& resourceType) {
    TGuard lock(Mutex);
    auto it = ResourceTypeCounters.find(resourceType);
    if (it == ResourceTypeCounters.end()) {
        it = ResourceTypeCounters.emplace(resourceType, std::make_shared<TSubscriberTypeCounters>(*this, resourceType)).first;
    }
    return it->second;
}

TSubscriberTypeCounters::TSubscriberTypeCounters(const TSubscriberCounters& owner, const TString& resourceType)
    : TBase(owner)
{
    DeepSubGroup("ResourceType", resourceType);

    RequestBytes = TBase::GetDeriviative("Requests/Bytes");
    RequestsCount = TBase::GetDeriviative("Requests/Count");

    BytesRequested = TBase::GetValueAutoAggregationsClient("Requested/Bytes");
    CountRequested = TBase::GetValueAutoAggregationsClient("Requested/Count");

    RepliesCount = TBase::GetDeriviative("Replies/Count");
    ReplyBytes = TBase::GetDeriviative("Replies/Bytes");

    BytesAllocated = TBase::GetValueAutoAggregationsClient("Allocated/Bytes");
    CountAllocated = TBase::GetValueAutoAggregationsClient("Allocated/Count");
}

}
