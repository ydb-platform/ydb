#pragma once

#include "mlp.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TReaderActor : public TBaseActor<TReaderActor>
                   , public TConstantLogPrefix {

public:
    TReaderActor(const TActorId& parentId, const TReaderSettings& settings);

    void Bootstrap();
    void PassAway() override;
    TString BuildLogPrefix() const override;

private:

    void DoDescribe();
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr&);
    STFUNC(DescribeState);

    void DoSelectPartition();
    void Handle(TEvPersQueue::TEvMLPGetPartitionResponse::TPtr&);
    void HandleOnSelectPartition(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    STFUNC(SelectPartitionState);

    void DoRead();
    void Handle(TEvPersQueue::TEvMLPReadResponse::TPtr&);
    void Handle(TEvPersQueue::TEvMLPErrorResponse::TPtr&);
    void HandleOnRead(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    STFUNC(ReadState);

    void SendToTablet(ui64 tabletId, IEventBase *ev);
    void ReplyErrorAndDie(NPersQueue::NErrorCode::EErrorCode errorCode, TString&& errorMessage);

    bool OnUnhandledException(const std::exception&) override;

private:
    const TActorId ParentId;
    const TReaderSettings Settings;

    TActorId ChildActorId;
    ui64 ReadBalancerTabletId;
    ui32 PartitionId;
    ui64 PQTabletId;

    TBackoff Backoff = TBackoff(5); // TODO retries
    ui64 Cookie = 1;
};

} // namespace NKikimr::NPQ::NMLPUSetting&