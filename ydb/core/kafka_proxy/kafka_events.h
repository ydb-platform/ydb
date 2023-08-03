#pragma once

#include <library/cpp/actors/core/event_local.h>
#include <ydb/core/base/events.h>

#include "kafka_messages.h"

using namespace NActors;

namespace NKafka {

struct TEvKafka {
    enum EEv {
        EvRequest = EventSpaceBegin(NKikimr::TKikimrEvents::TKikimrEvents::ES_KAFKA),
        EvProduceRequest,
        EvWakeup,
        EvResponse = EvRequest + 256,
        EvInternalEvents = EvResponse + 256,
        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::TKikimrEvents::ES_KAFKA),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_KAFKA)");


    struct TEvProduceRequest : public TEventLocal<TEvProduceRequest, EvProduceRequest> {
        TEvProduceRequest(const ui64 cookie, const TProduceRequestData* request)
        : Cookie(cookie)
        , Request(request)
        {}

        ui64 Cookie;
        const TProduceRequestData* Request;
    };

    struct TEvResponse : public TEventLocal<TEvResponse, EvResponse> {
        TEvResponse(const ui64 cookie, const TApiMessage::TPtr response)
            : Cookie(cookie)
            , Response(std::move(response)) {
        }

        ui64 Cookie;
        const TApiMessage::TPtr Response;
    };

    struct TEvWakeup : public TEventLocal<TEvWakeup, EvWakeup> {
    };
};

} // namespace NKafka
