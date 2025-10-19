#pragma once

#include "mlp.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/util/backoff.h>

#define Service TBase::Service
#define LogBuilder TBase::LogBuilder

namespace NKikimr::NPQ::NMLP {

template<typename TRequest, typename TResponse, typename TSettings>
class TChangerActor : public TBaseActor<TChangerActor<TRequest, TResponse, TSettings>>
                    , public TConstantLogPrefix {

    using TBase = TBaseActor<TChangerActor<TRequest, TResponse, TSettings>>;
    using TThis = TChangerActor<TRequest, TResponse, TSettings>;

public:
    TChangerActor(const TActorId& parentId, TSettings&& settings, NKikimrServices::EServiceKikimr service)
        : TBase(service)
        , ParentId(parentId)
        , Settings(std::move(settings))
    {
    }

    void Bootstrap() {
        DoDescribe();
    }

    void PassAway() override;

private:

    void DoDescribe() {
        LOG_D("Start describe");
        TBase::Become(&TThis::DescribeState);
        ChildActorId = TBase::RegisterWithSameMailbox(NDescriber::CreateDescriberActor(TBase::SelfId(), Settings.DatabasePath, { Settings.TopicName }));
    }

    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
        LOG_D("Handle NDescriber::TEvDescribeTopicsResponse");

        ChildActorId = {};

        auto& topics = ev->Get()->Topics;
        AFL_ENSURE(topics.size() == 1)("s", topics.size());

        auto& topic = topics.begin()->second;
        switch(topic.Status) {
            case NDescriber::EStatus::SUCCESS: {
                TopicInfo = topic.Info;
                return DoChanges();
            }
            default: {
                ReplyErrorAndDie(Ydb::StatusIds::SCHEME_ERROR,
                    NDescriber::Description(Settings.TopicName, topic.Status));
            }
        }
    }

    STFUNC(DescribeState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    void DoChanges() {
        LOG_D("Start DoChanges");
        TBase::Become(&TThis::ChangesState);

        for (const TMessageId& messageId: Settings.Messages) {
            auto [it, inserted] = PendingPartitions.try_emplace(messageId.PartitionId, TRequestInfo{});
            auto& partitionInfo = it->second;
            if (inserted) {
                auto* node = TopicInfo->PartitionGraph->GetPartition(messageId.PartitionId);
                if (node) {
                    partitionInfo.TabletId = node->TabletId;
                } else {
                    partitionInfo.Error = true;
                }
            }
            partitionInfo.Offsets.push_back(messageId.Offset);
        }

        for (auto& [partitionId, partitionInfo]: PendingPartitions) {
            auto* ev = CreateRequest(partitionId, partitionInfo.Offsets);
            SendToTablet(partitionInfo.TabletId, ev, partitionId);
        }

        ReplyIfPossible();
    }

    void Handle(typename TResponse::TPtr& ev) {
        LOG_D("Handle response " << ev->Get()->Record.ShortDebugString());
        auto partitionId = ev->Cookie;

        auto it = PendingPartitions.find(partitionId);
        if (it == PendingPartitions.end()) {
            LOG_D("Received response fron unexpected partition " << partitionId);
            return;
        }

        auto& partitionInfo = it->second;

        partitionInfo.Success = true;

        --PendingRequests;
        ReplyIfPossible();
    }

    void Handle(TEvPersQueue::TEvMLPErrorResponse::TPtr& ev) {
        LOG_D("Handle TEvPersQueue::TEvMLPErrorResponse " << ev->Get()->Record.ShortDebugString());

        auto partitionId = ev->Cookie;

        auto it = PendingPartitions.find(partitionId);
        if (it == PendingPartitions.end()) {
            LOG_D("Received response from unexpected partition " << partitionId);
            return;
        }

        auto& partitionInfo = it->second;

        partitionInfo.Error = true;

        --PendingRequests;
        ReplyIfPossible();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        LOG_D("Handle TEvPipeCache::TEvDeliveryProblem " << ev->Get()->TabletId);

        auto it = Pipes.find(ev->Get()->TabletId);
        if (it == Pipes.end()) {
            LOG_D("Received pipe error for unexpected tablet " << ev->Get()->TabletId);
            return;
        }

        it->second.Subscribed = false;

        for (auto& [partitionId, partitionInfo]: PendingPartitions) {
            if (partitionInfo.Success || partitionInfo.Error) {
                continue;
            }

            --PendingRequests;
        }

        ReplyIfPossible();
    }

    STFUNC(ChangesState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TResponse, Handle);
            hFunc(TEvPersQueue::TEvMLPErrorResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    TRequest* CreateRequest(ui32 partitionId, const std::vector<ui64>& offsets);
    void SendToTablet(ui64 tabletId, IEventBase *ev, ui32 partitionId) {
        auto& pipe = Pipes[tabletId];
        if (!pipe.Subscribed) {
            ++pipe.Cookie;
        }

        auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, !pipe.Subscribed, pipe.Cookie);
        TBase::Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery, partitionId);

        pipe.Subscribed = true;
        ++PendingRequests;
    }

    void ReplyIfPossible() {
        if (PendingRequests > 0) {
            return;
        }

        auto response = std::make_unique<TEvChangeResponse>();
        for (auto& [partitionId, partitionInfo]: PendingPartitions) {
            for (auto offset : partitionInfo.Offsets) {
                response->Messages.emplace_back(TMessageId(partitionId, offset), partitionInfo.Success);
            }
        }

        TBase::Send(ParentId, std::move(response));

        PassAway();
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
        LOG_I("Reply error " << Ydb::StatusIds::StatusCode_Name(errorCode));
        TBase::Send(ParentId, new TEvChangeResponse(errorCode, std::move(errorMessage)));
        PassAway();
    }

private:
    const TActorId ParentId;
    const TSettings Settings;

    TActorId ChildActorId;
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> TopicInfo;

    struct TRequestInfo {
        bool Error = false;
        bool Success = false;
        ui64 TabletId = 0;
        std::vector<ui64> Offsets;
    };

    // partitionId -> request info
    std::unordered_map<ui32, TRequestInfo> PendingPartitions;

    struct TPipeInfo {
        ui64 Cookie = 0;
        bool Subscribed = false;
    };
    // tabletId -> cookie
    std::unordered_map<ui64, TPipeInfo> Pipes;

    size_t PendingRequests = 0;
};

} // namespace NKikimr::NPQ::NMLP