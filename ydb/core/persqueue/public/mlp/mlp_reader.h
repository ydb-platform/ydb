#pragma once

#include "mlp.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TReaderActor : public TBaseActor<TReaderActor>
                   , public TConstantLogPrefix {

public:
    TReaderActor(const TActorId& parentId, const TReaderSettings& settings);

    void Bootstrap();
    void PassAway() override;

private:

    void DoDescribe();
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr&);
    STFUNC(DescribeState);

    void DoSelectPartition();
    void Handle(TEvPQ::TEvMLPGetPartitionResponse::TPtr&);
    void HandleOnSelectPartition(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    STFUNC(SelectPartitionState);

    void DoRead();
    void Handle(TEvPQ::TEvMLPReadResponse::TPtr&);
    void Handle(TEvPQ::TEvMLPErrorResponse::TPtr&);
    void HandleOnRead(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    STFUNC(ReadState);

    void SendToTablet(ui64 tabletId, IEventBase *ev);
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage);

    bool OnUnhandledException(const std::exception&) override;

private:
    const TActorId ParentId;
    const TReaderSettings Settings;

    TActorId ChildActorId;
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> Info;
    const NKikimrPQ::TPQTabletConfig::TConsumer* ConsumerConfig;
    ui32 PartitionId;
    ui64 PQTabletId;

    TBackoff Backoff = TBackoff(5);
    ui64 Cookie = 1;
};

} // namespace NKikimr::NPQ::NMLP
