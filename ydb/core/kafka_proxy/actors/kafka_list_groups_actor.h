#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include "ydb/library/aclib/aclib.h"
#include <ydb/core/kafka_proxy/kqp_helper.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include "actors.h"

namespace NKafka {

class TKafkaListGroupsActor: public NActors::TActorBootstrapped<TKafkaListGroupsActor> {

public:
    TKafkaListGroupsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TListGroupsRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , ListGroupsRequestData(message)
        , ListGroupsResponseData(new TListGroupsResponseData()) {
    }

    enum EKafkaTxnKqpRequests : ui8 {
            NO_REQUEST = 0,
            SELECT
        };

void Bootstrap(const NActors::TActorContext& ctx);


private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
        }
    }

    void StartKqpSession(const TActorContext& ctx);
    void SendToKqpConsumerGroupsRequest(const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    TString GetYqlWithTablesNames(const TString& templateStr);
    TListGroupsResponseData ParseGroupsMetadata(const NKqp::TEvKqp::TEvQueryResponse& response);
    void HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse& response, const TActorContext& ctx);
    NYdb::TParams BuildSelectParams();
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    template<class ErrorResponseType, class EventType>
    void SendFailResponse(TAutoPtr<TEventHandle<EventType>>& evHandle, EKafkaErrors errorCode, const TString& errorMessage);

    TMaybe<TString> GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev);
    void Die(const TActorContext &ctx);
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TListGroupsRequestData> ListGroupsRequestData;
    const TListGroupsResponseData::TPtr ListGroupsResponseData;

    // EKafkaErrors ErrorCode = EKafkaErrors::NONE_ERROR;

    std::unique_ptr<TKqpTxHelper> Kqp;

    const TString DatabasePath="/Root";
    TAutoPtr<TEventHandle<TEvKafka::TEvEndTxnRequest>> EndTxnRequestPtr;

    TString KqpSessionId;
    ui64 KqpCookie = 0;
};

} // namespace NKafka
