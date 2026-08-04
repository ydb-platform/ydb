#pragma once

#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/library/actors/core/log.h>

namespace NKafka {

class TTopicOffsetsActor : public NKikimr::NGRpcProxy::V1::TPQInternalSchemaActor<TTopicOffsetsActor,
                                                               TEvKafka::TGetOffsetsRequest,
                                                               TEvKafka::TEvTopicOffsetsResponse>
                               , public NKikimr::NGRpcProxy::V1::TDescribeTopicActorImpl
                               , public NKikimr::NGRpcProxy::V1::TCdcStreamCompatible
                               , public TKafkaExceptionHandler<TTopicOffsetsActor> {

using TBase = TPQInternalSchemaActor<TTopicOffsetsActor,
                                                               TEvKafka::TGetOffsetsRequest,
                                                               TEvKafka::TEvTopicOffsetsResponse>;

public:
    TTopicOffsetsActor(const TEvKafka::TGetOffsetsRequest& request, const TActorId& requester);

    ~TTopicOffsetsActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx) override;

    void StateWork(TAutoPtr<IEventHandle>& ev);

    void HandleCacheNavigateResponse(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) override;

    virtual void ApplyResponse(TTabletInfo&, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr&,
                               const TActorContext&) override {
        AFL_ENSURE(false)("reason", "TTopicOffsetsActor: unexpected TEvReadSessionsInfoResponse")("database", Database)("path", TopicPath);
    }

    bool ApplyResponse(NKikimr::TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr&, const TActorContext&) override {
        AFL_ENSURE(false)("reason", "TTopicOffsetsActor: unexpected TEvGetPartitionsLocationResponse")("database", Database)("path", TopicPath);
    }

    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) override;

    void Reply(const TActorContext&) override;

    void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext&) override;
};

}// namespace NKafka
