#pragma once

#include <library/cpp/actors/core/event_local.h>
#include <ydb/core/base/events.h>

#include "kafka_messages.h"
#include "ydb/library/aclib/aclib.h"
#include "actors/actors.h"

using namespace NActors;

namespace NKafka {

struct TEvKafka {
    enum EEv {
        EvRequest = EventSpaceBegin(NKikimr::TKikimrEvents::TKikimrEvents::ES_KAFKA),
        EvProduceRequest,
        EvAuthResult,
        EvHandshakeResult,
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

    struct TEvResponse : public TEventLocal<TEvResponse, EvResponse> {
        TEvResponse(const ui64 correlationId, const TApiMessage::TPtr response)
            : CorrelationId(correlationId)
            , Response(std::move(response)) {
        }

        const ui64 CorrelationId;
        const TApiMessage::TPtr Response;
    };

    struct TEvAuthResult : public TEventLocal<TEvAuthResult, EvAuthResult> {
        TEvAuthResult(EAuthSteps authStep, std::shared_ptr<TEvKafka::TEvResponse> clientResponse, TIntrusiveConstPtr<NACLib::TUserToken> token, TString database, TString error = "")
        : AuthStep(authStep),
          UserToken(token),
          Database(database),
          Error(error),
          ClientResponse(std::move(clientResponse))
        {}

        EAuthSteps AuthStep;
        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        TString Database;
        TString Error;
        TString SaslMechanism;
        std::shared_ptr<TEvKafka::TEvResponse> ClientResponse;
    };

    struct TEvHandshakeResult : public TEventLocal<TEvHandshakeResult, EvHandshakeResult> {
        TEvHandshakeResult(EAuthSteps authStep, std::shared_ptr<TEvKafka::TEvResponse> clientResponse, TString saslMechanism, TString error = "")
        : AuthStep(authStep),
          Error(error),
          SaslMechanism(saslMechanism),
          ClientResponse(std::move(clientResponse))
        {}
        
        EAuthSteps AuthStep;
        TString Error;
        TString SaslMechanism;
        std::shared_ptr<TEvKafka::TEvResponse> ClientResponse;
    };

    struct TEvWakeup : public TEventLocal<TEvWakeup, EvWakeup> {
    };
};

} // namespace NKafka
