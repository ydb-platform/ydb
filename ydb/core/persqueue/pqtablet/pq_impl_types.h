#pragma once

#include "pq_impl.h"

namespace NKikimr::NPQ {

struct TPartitionInfo {
    explicit TPartitionInfo(const TActorId& actor)
        : Actor(actor)
        , InitDone(false)
    {
    }

    TPartitionInfo(const TPartitionInfo& info) = delete;
    TPartitionInfo(TPartitionInfo&& info) = default;

    TActorId Actor;
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
