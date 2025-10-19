#pragma once

#include "mlp.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

struct TChangerSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;

    // TODO check access

    std::function<IEventBase* (const TString& topicName, const TString& consumer, ui32 partitionId, const std::vector<ui64>& offsets)> RequestCreator;
};

class TChangerActor : public TBaseActor<TChangerActor>
                    , public TConstantLogPrefix {

public:
    TChangerActor(const TActorId& parentId, TChangerSettings&& settings);

    void Bootstrap();
    void PassAway() override;
    TString BuildLogPrefix() const override;

private:

    void DoDescribe();
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr&);
    STFUNC(DescribeState);

    void DoCommit();
    void Handle(TEvPersQueue::TEvMLPCommitResponse::TPtr&);
    void Handle(TEvPersQueue::TEvMLPErrorResponse::TPtr&);
    void HandleOnRead(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    STFUNC(CommitState);

    void SendToTablet(ui64 tabletId, IEventBase *ev);
    void ReplyErrorAndDie(NPersQueue::NErrorCode::EErrorCode errorCode, TString&& errorMessage);

    bool OnUnhandledException(const std::exception&) override;

private:
    const TActorId ParentId;
    const TChangerSettings Settings;

    TActorId ChildActorId;

    std::unordered_set<ui32> PendingPartitions;
};

} // namespace NKikimr::NPQ::NMLP