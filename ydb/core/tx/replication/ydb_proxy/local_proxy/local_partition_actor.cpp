#include "local_partition_actor.h"
#include "logging.h"

namespace NKikimr::NReplication {

TBaseLocalTopicPartitionActor::TBaseLocalTopicPartitionActor(const std::string& database, const std::string&& topicPath, const ui32 partitionId)
    : Database(database)
    , TopicPath(std::move(topicPath))
    , PartitionId(partitionId)
{
}

void TBaseLocalTopicPartitionActor::Bootstrap() {
    LogPrefix = MakeLogPrefix();
    DoDescribe(TopicPath);
}

void TBaseLocalTopicPartitionActor::DoDescribe(const TString& topicPath) {
    auto path = TStringBuilder() << "/" << Database << topicPath;
    LOG_D("Describe topic '" << path << "'");
    auto request = MakeHolder<TNavigate>();
    request->ResultSet.emplace_back(MakeNavigateEntry(path, TNavigate::OpPath));
    Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
    Become(&TThis::StateDescribe);
}

void TBaseLocalTopicPartitionActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    static const TString errorMarket = "LocalYdbProxy";

    LOG_T("Handle " << ev->Get()->ToString());

    auto& result = ev->Get()->Request;

    if (!CheckNotEmpty(errorMarket, result, LeaveOnError())) {
        return;
    }

    if (!CheckEntriesCount(errorMarket, result, 1, LeaveOnError())) {
        return;
    }

    const auto& entry = result->ResultSet.at(0);

    if (entry.Status == TNavigate::EStatus::PathErrorUnknown) {
        return OnFatalError(TStringBuilder() << "Discovery for all topics failed. The last error was: no path '" << Database << TopicPath << "'");
    }

    if (!CheckEntrySucceeded(errorMarket, entry, DoRetryDescribe())) {
        return;
    }

    if (entry.Kind == TNavigate::EKind::KindCdcStream) {
        return DoDescribe(TStringBuilder() << TopicPath << "/streamImpl");
    }

    if (!CheckEntryKind(errorMarket, entry, TNavigate::EKind::KindTopic, LeaveOnError())) {
        return;
    }

    auto* node = entry.PQGroupInfo->PartitionGraph->GetPartition(PartitionId);
    if (!node) {
        return OnError(TStringBuilder() << "The partition " << PartitionId << " of the topic '" << TopicPath << "' not found");
    }
    PartitionTabletId = node->TabletId;
    DoCreatePipe();
}

void TBaseLocalTopicPartitionActor::HandleOnDescribe(TEvents::TEvWakeup::TPtr& ev) {
    if (static_cast<ui64>(EWakeupType::Describe) == ev->Get()->Tag) {
        DoDescribe(TopicPath);
    }
}

TSchemeCacheHelpers::TCheckFailFunc TBaseLocalTopicPartitionActor::DoRetryDescribe() {
    return [this](const TString& error) {
        if (Attempt == MaxAttempts) {
            OnError(error);
        } else {
            Schedule(TDuration::Seconds(1 << Attempt++), new TEvents::TEvWakeup(static_cast<ui64>(EWakeupType::Describe)));
        }
    };
}

TSchemeCacheHelpers::TCheckFailFunc TBaseLocalTopicPartitionActor::LeaveOnError() {
    return [this](const TString& error) {
        OnFatalError(error);
    };
}

STATEFN(TBaseLocalTopicPartitionActor::StateDescribe) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        hFunc(TEvents::TEvWakeup, HandleOnDescribe);

        sFunc(TEvents::TEvPoison, PassAway);
    default:
        OnInitEvent(ev);
    }
}

void TBaseLocalTopicPartitionActor::DoCreatePipe() {
    LOG_T("Create pipe to " << PartitionTabletId);

    Attempt = 0;
    CreatePipe();
    Become(&TBaseLocalTopicPartitionActor::StateCreatePipe);
}

void TBaseLocalTopicPartitionActor::CreatePipe() {
    NTabletPipe::TClientConfig config;
    config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
    PartitionPipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(TThis::SelfId(), PartitionTabletId, config));
}

void TBaseLocalTopicPartitionActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());

    auto& msg = *ev->Get();
    if (msg.Status != NKikimrProto::OK) {
        if (Attempt++ == MaxAttempts) {
            return OnError("Pipe creation error");
        }
        return CreatePipe();
    }

    LOG_T("Pipe has been connected");

    OnDescribeFinished();
}

void TBaseLocalTopicPartitionActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
    LOG_T("Handle " << ev->Get()->ToString());
    OnError("Pipe destroyed");
}

STATEFN(TBaseLocalTopicPartitionActor::StateCreatePipe) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        sFunc(TEvents::TEvPoison, PassAway);
    default:
        OnInitEvent(ev);
    }
}

void TBaseLocalTopicPartitionActor::PassAway() {
    if (PartitionPipeClient) {
        NTabletPipe::CloseAndForgetClient(SelfId(), PartitionPipeClient);
    }
    IActor::PassAway();
}

}
