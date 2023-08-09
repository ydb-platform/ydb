#pragma once

#include <library/cpp/actors/core/event_local.h>
#include <ydb/core/base/events.h>

#include "kafka_messages.h"
#include "ydb/library/aclib/aclib.h"

using namespace NActors;

namespace NKafka {

struct TEvKafka {
    enum EEv {
        EvRequest = EventSpaceBegin(NKikimr::TKikimrEvents::TKikimrEvents::ES_KAFKA),
        EvProduceRequest,
        EvAuthSuccess,
        EvWakeup,
        EvResponse = EvRequest + 256,
        EvInternalEvents = EvResponse + 256,
        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::TKikimrEvents::ES_KAFKA),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_KAFKA)");


    struct TEvProduceRequest : public TEventLocal<TEvProduceRequest, EvProduceRequest> {
        TEvProduceRequest(const ui64 correlationId, const TProduceRequestData* request)
        : CorrelationId(correlationId)
        , Request(request)
        {}

        ui64 CorrelationId;
        const TProduceRequestData* Request;
    };

    struct TEvAuthSuccess : public TEventLocal<TEvAuthSuccess, EvAuthSuccess> {
        TEvAuthSuccess(TIntrusiveConstPtr<NACLib::TUserToken> token)
        : UserToken(token)
        {}

        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    };

    struct TEvResponse : public TEventLocal<TEvResponse, EvResponse> {
        TEvResponse(const ui64 correlationId, const TApiMessage::TPtr response)
            : CorrelationId(correlationId)
            , Response(std::move(response)) {
        }

        const ui64 CorrelationId;
        const TApiMessage::TPtr Response;
    };

    struct TEvWakeup : public TEventLocal<TEvWakeup, EvWakeup> {
    };
};

} // namespace NKafka
