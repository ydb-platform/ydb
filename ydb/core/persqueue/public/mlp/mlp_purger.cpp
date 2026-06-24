#include "mlp_purger.h"

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ::NMLP {

TPurgerActor::TPurgerActor(const TActorId& parentId, const TPurgerSettings& settings)
    : TBaseActor(NKikimrServices::EServiceKikimr::PQ_MLP_PURGER)
    , ParentId(parentId)
    , Settings(settings)
{
}

void TPurgerActor::Bootstrap() {
    DoDescribe();
}

void TPurgerActor::DoDescribe() {
    YDB_LOG_DEBUG("Start describe",
        {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TPurgerActor::DescribeState);

    NDescriber::TDescribeSettings settings = {
        .UserToken = Settings.UserToken,
        .AccessRights = NACLib::EAccessRights::UpdateRow
    };
    ChildActorId = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(SelfId(), Settings.DatabasePath, { Settings.TopicName }, settings));
}

void TPurgerActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    YDB_LOG_DEBUG("Handle NDescriber::TEvDescribeTopicsResponse",
        {"logPrefix", NPQ_LOG_PREFIX});

    ChildActorId = {};

    auto& topics = ev->Get()->Topics;
    AFL_ENSURE(topics.size() == 1)("s", topics.size());

    auto& topic = topics.begin()->second;
    switch(topic.Status) {
        case NDescriber::EStatus::SUCCESS: {
            TopicInfo = topic;
            return DoPurge();
        }
        default: {
            ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                NDescriber::Description(Settings.TopicName, topic.Status));
        }
    }
}

STFUNC(TPurgerActor::DescribeState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TPurgerActor::DoPurge() {
    YDB_LOG_DEBUG("Start purge",
        {"logPrefix", NPQ_LOG_PREFIX});
    Become(&TPurgerActor::PurgeState);

    for (auto& partition : TopicInfo.Info->Description.GetPartitions()) {
        auto partitionId = partition.GetPartitionId();
        auto& partitionStatus = Partitions[partitionId] = {
            .TabletId = partition.GetTabletId()
        };
        RequestPartitionIfNeeded(partitionId, partitionStatus);
    }

    ReplyIfPossible();
}

void TPurgerActor::Handle(TEvPQ::TEvMLPPurgeResponse::TPtr& ev)
{
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPPurgeResponse",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});

    auto partitionId = ev->Get()->GetPartitionId();
    auto& partitionStatus = Partitions[partitionId];
    if (partitionStatus.Status == EPartitionStatus::InProgress) {
        --PendingPartitions;
    }
    if (partitionStatus.WaitRetry) {
        partitionStatus.WaitRetry = false;
        --PendingRetries;
    }
    partitionStatus.Status = EPartitionStatus::Success;

    ReplyIfPossible();
}

void TPurgerActor::RetryIfPossible(ui32 partitionId, TPartitionStatus& partitionStatus) {
    if (partitionStatus.Status == EPartitionStatus::InProgress && !partitionStatus.WaitRetry) {
        --PendingPartitions;
        if (partitionStatus.Backoff.HasMore()) {
            ++PendingRetries;
            partitionStatus.WaitRetry = true;
            Schedule(partitionStatus.Backoff.Next(), new TEvents::TEvWakeup(partitionId));
        } else {
            partitionStatus.Status = EPartitionStatus::Error;
        }
    }
}

void TPurgerActor::Handle(TEvPQ::TEvMLPErrorResponse::TPtr& ev)
{
    YDB_LOG_DEBUG("Handle TEvPQ::TEvMLPErrorResponse",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record});

    auto partitionId = ev->Get()->GetPartitionId();
    auto& partitionStatus = Partitions[partitionId];
    if (partitionStatus.Cookie == ev->Cookie) {
        if (ev->Get()->GetStatus() == Ydb::StatusIds::SCHEME_ERROR) {
            ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR, std::move(ev->Get()->GetErrorMessage()));
            return;
        }

        RetryIfPossible(partitionId, partitionStatus);
    }

    ReplyIfPossible();
}

void TPurgerActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev)
{
    YDB_LOG_DEBUG("Handle TEvPipeCache::TEvDeliveryProblem",
        {"logPrefix", NPQ_LOG_PREFIX});

    auto tabletId = ev->Get()->TabletId;
    ++TabletCookies[tabletId];

    for (auto& [partitionId, partitionStatus] : Partitions) {
        if (partitionStatus.TabletId == tabletId) {
            RetryIfPossible(partitionId, partitionStatus);
        }
    }

    ReplyIfPossible();
}

void TPurgerActor::Handle(TEvents::TEvWakeup::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvents::TEvWakeup",
        {"logPrefix", NPQ_LOG_PREFIX});

    auto partitionId = ev->Get()->Tag;
    auto& partitionStatus = Partitions[partitionId];
    if (partitionStatus.Status == EPartitionStatus::InProgress) {
        --PendingRetries;
        RequestPartitionIfNeeded(partitionId, partitionStatus);
    }

    ReplyIfPossible();
}

STFUNC(TPurgerActor::PurgeState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvPQ::TEvMLPPurgeResponse, Handle);
        hFunc(TEvPQ::TEvMLPErrorResponse, Handle);
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void TPurgerActor::RequestPartitionIfNeeded(ui32 partitionId,TPartitionStatus& status) {
    if (status.Status == EPartitionStatus::Success || status.Status == EPartitionStatus::Error) {
        return;
    }

    ++PendingPartitions;
    status.Status = EPartitionStatus::InProgress;
    status.Cookie = ++NextCookie;
    status.WaitRetry = false;
    SendToTablet(status.TabletId, new TEvPQ::TEvMLPPurgeRequest(Settings.TopicName, Settings.Consumer, partitionId), status.Cookie);
}

void TPurgerActor::ReplyIfPossible() {
    YDB_LOG_DEBUG("ReplyIfPossible: PendingPartitions PendingRetries",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"pendingPartitions", PendingPartitions},
        {"pendingRetries", PendingRetries});
    if (PendingPartitions > 0 || PendingRetries > 0) {
        return;
    }

    auto allSuccess = std::all_of(Partitions.begin(), Partitions.end(), [](const auto& partition) {
        return partition.second.Status == EPartitionStatus::Success;
    });

    auto response = std::make_unique<TEvPurgeResponse>();
    response->Status = allSuccess ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::INTERNAL_ERROR;
    Send(ParentId, std::move(response));

    PassAway();
}

void TPurgerActor::SendToTablet(ui64 tabletId, IEventBase *ev, ui64 cookie) {
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, true, TabletCookies[tabletId]);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery, cookie);
}

void TPurgerActor::ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
    YDB_LOG_INFO("Reply error",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"#_Ydb::StatusIds::StatusCode_Name(errorCode)", Ydb::StatusIds::StatusCode_Name(errorCode)});
    Send(ParentId, new TEvPurgeResponse(errorCode, std::move(errorMessage)));
    PassAway();
}

void TPurgerActor::PassAway() {
    if (ChildActorId) {
        Send(ChildActorId, new TEvents::TEvPoison());
    }
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
    TBaseActor::PassAway();
}

bool TPurgerActor::OnUnhandledException(const std::exception& exc) {
    ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR,
        TStringBuilder() <<"Unhandled exception: " << exc.what());
    return TBaseActor::OnUnhandledException(exc);
}

IActor* CreatePurger(const NActors::TActorId& parentId, TPurgerSettings&& settings) {
    return new TPurgerActor(parentId, std::move(settings));
}

} // namespace NKikimr::NPQ::NMLP
