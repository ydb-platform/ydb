#pragma once

#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>

namespace NKafka {

class TTopicOffsetsActor : public NKikimr::NGRpcProxy::V1::TPQInternalSchemaActor<TTopicOffsetsActor,
                                                               TEvKafka::TGetOffsetsRequest,
                                                               TEvKafka::TEvTopicOffsetsResponse>
                               , public NKikimr::NGRpcProxy::V1::TDescribeTopicActorImpl
                               , public NKikimr::NGRpcProxy::V1::TCdcStreamCompatible {

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
        Y_ABORT();
    }

    bool ApplyResponse(NKikimr::TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr&, const TActorContext&) override {
        Y_ABORT();
    }

    void ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) override;

    void Reply(const TActorContext&) override;

    void RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext&) override;
};

}// namespace NKafka
