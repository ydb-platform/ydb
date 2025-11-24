#pragma once

#include "pq_impl.h"

namespace NKikimr::NPQ {

struct TPartitionInfo {
    TPartitionInfo(const TActorId& actor,
                   TMaybe<TPartitionKeyRange>&& keyRange)
        : Actor(actor)
        , KeyRange(std::move(keyRange))
        , InitDone(false)
    {
    }

    TPartitionInfo(const TPartitionInfo& info)
        : Actor(info.Actor)
        , KeyRange(info.KeyRange)
        , InitDone(info.InitDone)
        , PendingRequests(info.PendingRequests)
    {
    }

    TActorId Actor;
    TMaybe<TPartitionKeyRange> KeyRange;
    bool InitDone;
    THashMap<TString, TTabletLabeledCountersBase> LabeledCounters;
    size_t ReservedBytes = 0;

    struct TPendingRequest {
        TPendingRequest(ui64 cookie,
                        std::shared_ptr<TEvPersQueue::TEvRequest> event,
                        const TActorId& sender) :
            Cookie(cookie),
            Event(std::move(event)),
            Sender(sender)
        {
        }

        TPendingRequest(const TPendingRequest& rhs) = default;
        TPendingRequest(TPendingRequest&& rhs) = default;

        ui64 Cookie;
        std::shared_ptr<TEvPersQueue::TEvRequest> Event;
        TActorId Sender;
    };

    TDeque<TPendingRequest> PendingRequests;
};

}
