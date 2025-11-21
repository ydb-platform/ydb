#pragma once

#include "mlp.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TWriterActor : public TBaseActor<TWriterActor>
                   , public TConstantLogPrefix {

public:
    TWriterActor(const TActorId& parentId, const TWriterSettings& settings);

    void Bootstrap();
    void PassAway() override;

private:

    void DoDescribe();
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr&);
    STFUNC(DescribeState);

    void DoWrite();
    void Handle(TEvPersQueue::TEvResponse::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    STFUNC(WriteState);

    void SendToTablet(ui64 tabletId, IEventBase *ev);
    void ReplyErrorAndDie();

    bool OnUnhandledException(const std::exception&) override;
    bool IsSuccess(const NKikimrClient::TResponse& ev);
    void ReplyIfPossible();

private:
    const TActorId ParentId;
    TWriterSettings Settings;

    TActorId ChildActorId;

    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> TopicInfo;

    size_t PendingRequests = 0;

    struct TPendingMessage {
        size_t Index;
        ui64 TabletId;
        ui32 PartitionId;

        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::INTERNAL_ERROR;
        bool ResultReceived = false;
        ui64 Offset = 0;
    };
    std::vector<TPendingMessage> PendingMessages;
    NDescriber::EStatus DescribeStatus;
};

} // namespace NKikimr::NPQ::NMLP
