#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include "ydb/library/aclib/aclib.h"
#include <ydb/core/kafka_proxy/kqp_helper.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include "actors.h"

namespace NKafka {

static const TString SELECT_GROUPS_NO_FILTER = R"sql(
    DECLARE $Database AS Utf8;
    SELECT * FROM (
        SELECT
            `<consumer_state_table_name>`.*,
            ROW_NUMBER() OVER (PARTITION BY consumer_group ORDER BY generation DESC) AS row_num
        FROM `<consumer_state_table_name>`
        WHERE database = $Database
    )
    WHERE row_num = 1;
)sql";

static const TString SELECT_GROUPS_WITH_FILTER = R"sql(
    DECLARE $Database AS Utf8;
    DECLARE $StatesFilter AS List<Uint32>;

    SELECT * FROM (
        SELECT
            `<consumer_state_table_name>`.*,
            ROW_NUMBER() OVER (PARTITION BY consumer_group ORDER BY generation DESC) AS row_num
        FROM `<consumer_state_table_name>`
        WHERE database = $Database
    )
    WHERE row_num = 1 AND state in $StatesFilter;
)sql";

class TKafkaListGroupsActor: public NActors::TActorBootstrapped<TKafkaListGroupsActor> {


using TBase = TActorBootstrapped<TKafkaListGroupsActor>;

public:
    TKafkaListGroupsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TListGroupsRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , ListGroupsRequestData(message)
        , DatabasePath(context->DatabasePath) {
    }


void Bootstrap(const NActors::TActorContext& ctx);


TStringBuilder LogPrefix() const {
    return TStringBuilder() << "KafkaListGroupsActor{DatabasePath=" << DatabasePath << "}: ";
}

private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            HFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
        }
    }

    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse& response, const TActorContext& ctx);

    void Die(const TActorContext &ctx);

    void StartKqpSession(const TActorContext& ctx);
    TListGroupsResponseData ParseGroupsMetadata(const NKqp::TEvKqp::TEvQueryResponse& response);

    TString GetYqlWithTableNames(const TString& templateStr);
    TMaybe<TString> GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev);
    NYdb::TParams BuildSelectParams();
    std::shared_ptr<TListGroupsResponseData> BuildResponse(TListGroupsResponseData responseData);
    void SendToKqpConsumerGroupsRequest(const TActorContext& ctx);
    void SendFailResponse(EKafkaErrors errorCode, const std::optional<TString>& errorMessage);

    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TListGroupsRequestData> ListGroupsRequestData;

    std::unique_ptr<TKqpTxHelper> Kqp;

    const TString DatabasePath;

    TString KqpSessionId;
    ui64 KqpCookie = 0;
};

} // namespace NKafka
