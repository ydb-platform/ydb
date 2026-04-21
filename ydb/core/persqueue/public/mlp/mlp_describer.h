#pragma once

#include "mlp.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TDescriberActor : public TBaseActor<TDescriberActor>
                      , public TConstantLogPrefix {

public:
    TDescriberActor(const TActorId& parentId, const TDescribeSettings& settings);

    void Bootstrap();
    void PassAway() override;

private:
    void DoDescribe();
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr&);
    STFUNC(DescribeState);

    void DoRuntimeAttributes();
    void Handle(TEvPQ::TEvMLPGetRuntimeAttributesResponse::TPtr&);
    void Handle(TEvPQ::TEvMLPErrorResponse::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    STFUNC(RuntimeAttributesState);

    void SendToTablet(ui64 tabletId, IEventBase *ev);
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage);

    bool OnUnhandledException(const std::exception&) override;

private:
    const TActorId ParentId;
    const TDescribeSettings Settings;

    TActorId ChildActorId;
    NDescriber::TTopicInfo TopicInfo;

    TBackoff Backoff = TBackoff(5);
    ui64 Cookie = 1;
};

} // namespace NKikimr::NPQ::NMLP
