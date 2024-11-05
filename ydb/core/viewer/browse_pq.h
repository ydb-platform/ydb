#pragma once
#include "browse.h"
#include "viewer.h"
#include "wb_aggregate.h"
#include <ydb/core/base/tablet.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NViewerPQ {

using namespace NViewer;
using namespace NActors;

class TBrowseRoot : public TActorBootstrapped<TBrowseRoot> {
    using TBase = TActorBootstrapped<TBrowseRoot>;
    TActorId Owner;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TBrowseRoot(const TActorId& owner, const IViewer::TBrowseContext&)
        : Owner(owner)
    {}

    void Bootstrap(const TActorContext& ctx) {
        NKikimrViewer::TBrowseInfo browseInfo;
        NKikimrViewer::TMetaInfo metaInfo;
        NKikimrViewer::TBrowseInfo& pbConsumers = *browseInfo.AddChildren();
        pbConsumers.SetType(NKikimrViewer::EObjectType::Consumers);
        pbConsumers.SetName("Consumers");
        ctx.Send(Owner, new NViewerEvents::TEvBrowseResponse(std::move(browseInfo), std::move(metaInfo)));
        Die(ctx);
    }
};

class TBrowseCommon : public TBrowseTabletsCommon {
protected:
    using TThis = TBrowseCommon;
    using TBase = TBrowseTabletsCommon;
    const TString PQ_ROOT_PATH;
    TSet<TString> Consumers;
    struct TTopicInfo : NKikimrViewer::TMetaTopicInfo {
        TMap<i32, THolder<TEvPersQueue::TEvPartitionClientInfoResponse>> PartitionResults;
        TMap<i32, NKikimrViewer::TMetaTopicPartitionInfo> PartitionInfo;
    };
    TMap<TString, TTopicInfo> Topics;
    TMultiMap<TTabletId, i32> TabletPartitions;
    TMap<TTabletId, TTopicInfo*> TabletTopic;
    THashMap<TString, NKikimrViewer::TMetaTopicConsumerInfo> ConsumerInfo;
    TString FilterTopic;
    TString FilterConsumer;
    TActorId TxProxy = MakeTxProxyID();

public:
    TBrowseCommon(const TActorId& owner, const IViewer::TBrowseContext& browseContext)
        : TBrowseTabletsCommon(owner, browseContext)
        , PQ_ROOT_PATH("/Root/PQ") // TODO(xenoxeno)
    {}

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr &ev, const TActorContext &ctx) {
        DescribeResult.Reset(ev->Release());
        ++Responses;
        ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestCompleted(TxProxy, TEvTxUserProxy::EvNavigate));
        const auto& pbRecord(DescribeResult->GetRecord());
        if (pbRecord.GetStatus() == NKikimrScheme::EStatus::StatusSuccess) {
            if (pbRecord.HasPathDescription()) {
                const auto& pbPathDescription(pbRecord.GetPathDescription());
                TVector<ui64> tablets;
                if (pbPathDescription.HasPersQueueGroup()) {
                    const auto& pbPersQueueGroup = pbPathDescription.GetPersQueueGroup();
                    TTopicInfo* topicInfo = &Topics[GetTopicName(pbRecord.GetPath())];
                    tablets.reserve(pbPersQueueGroup.PartitionsSize());
                    for (const auto& partition : pbPersQueueGroup.GetPartitions()) {
                        TTabletId tabletId = partition.GetTabletId();
                        tablets.emplace_back(tabletId);
                        TabletTopic.emplace(tabletId, topicInfo);
                        TabletPartitions.emplace(tabletId, partition.GetPartitionId());
                    }
                }
                if (!tablets.empty()) {
                    Sort(tablets);
                    tablets.erase(std::unique(tablets.begin(), tablets.end()), tablets.end());
                    SendTabletRequests(tablets, ctx);
                    std::copy(tablets.begin(), tablets.end(), std::back_inserter(Tablets));
                }
            }
        } else {
            switch (DescribeResult->GetRecord().GetStatus()) {
                case NKikimrScheme::EStatus::StatusPathDoesNotExist:
                    return HandleBadRequest(ctx, "The path is not found");
                default:
                    return HandleBadRequest(ctx, "Error getting schema information");
            }
        }
        const auto& pbDescribeResult(DescribeResult->GetRecord());
        if (pbDescribeResult.HasPathDescription()) {
            const auto& pbPathDescription(pbDescribeResult.GetPathDescription());
            if (pbPathDescription.HasSelf()) {
                const auto& pbChildren(pbPathDescription.GetChildren());
                for (const auto& pbChild : pbChildren) {
                    if (pbChild.GetPathType() == NKikimrSchemeOp::EPathType::EPathTypePersQueueGroup) {
                        THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
                        if (!BrowseContext.UserToken.empty()) {
                            request->Record.SetUserToken(BrowseContext.UserToken);
                        }
                        NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
                        record->SetPath(pbDescribeResult.GetPath() + "/" + pbChild.GetName());
                        ctx.Send(TxProxy, request.Release());
                        ++Requests;
                        ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestSent(TxProxy, TEvTxUserProxy::EvNavigate));
                    }
                }
            }
        }
        if (Responses == Requests) {
            ReplyAndDie(ctx);
        }
    }

    void SendTabletRequests(const TVector<TTabletId>& tablets, const TActorContext& ctx) {
        TDomainsInfo* domainsInfo = AppData(ctx)->DomainsInfo.Get();
        for (auto tabletId : tablets) {
            TActorId pipeClient = GetTabletPipe(tabletId, ctx);
            const auto& range = TabletPartitions.equal_range(tabletId);
            if (range.first != range.second) {
                TAutoPtr<TEvPersQueue::TEvPartitionClientInfo> request = new TEvPersQueue::TEvPartitionClientInfo();
                for (auto it = range.first; it != range.second; ++it) {
                    request->Record.AddPartitions(it->second);
                    ++Requests; // because we are waiting for individual partition responses
                }
                NTabletPipe::SendData(ctx, pipeClient, request.Release(), tabletId);
                ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestSent(pipeClient, tabletId, TEvPersQueue::EvPartitionClientInfo));
            }
            pipeClient = GetTabletPipe(tabletId, ctx);
            NTabletPipe::SendData(ctx, pipeClient, new TEvTablet::TEvGetCounters(), tabletId);
            ++Requests;
            ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestSent(pipeClient, tabletId, TEvTablet::EvGetCounters));
            ui64 hiveTabletId = domainsInfo->GetHive();
            pipeClient = GetTabletPipe(hiveTabletId, ctx);
            NTabletPipe::SendData(ctx, pipeClient, new TEvHive::TEvLookupChannelInfo(tabletId), tabletId);
            ++Requests;
            ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestSent(pipeClient, hiveTabletId, TEvHive::EvLookupChannelInfo));
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            HFunc(TEvTablet::TEvGetCountersResponse, TBase::Handle);
            HFunc(TEvPersQueue::TEvPartitionClientInfoResponse, Handle);
            HFunc(TEvHive::TEvChannelInfo, TBase::Handle);
            HFunc(TEvBlobStorage::TEvResponseControllerInfo, TBase::Handle);
            CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill);
        }
    }

    TString GetTopicName(const TString& name) {
        if (name.StartsWith(PQ_ROOT_PATH)) {
            return name.substr(PQ_ROOT_PATH.size() + 1);
        }
        return name;
    }

    void Handle(TEvPersQueue::TEvPartitionClientInfoResponse::TPtr &ev, const TActorContext &ctx) {
        auto it = TabletTopic.find(ev->Cookie); // by tablet id
        if (it != TabletTopic.end()) {
            it->second->PartitionResults.emplace(ev->Get()->Record.GetPartition(), ev->Release());
        }
        if (++Responses == Requests) {
            ReplyAndDie(ctx);
        }
    }

    static void UpdateMaximum(NKikimrViewer::TMetaTopicLagInfo& destination, const NKikimrViewer::TMetaTopicLagInfo& source) {
        if (source.GetCreateTime() != 0
                && (destination.GetCreateTime() == 0 || destination.GetCreateTime() < source.GetCreateTime())) {
            destination.SetCreateTime(source.GetCreateTime());
        }
        if (source.GetWriteTime() != 0
                && (destination.GetWriteTime() != 0 || destination.GetWriteTime() < source.GetWriteTime())) {
            destination.SetWriteTime(source.GetWriteTime());
        }
        if (source.GetMessages() != 0
                && (destination.GetMessages() != 0 || destination.GetMessages() < source.GetMessages())) {
            destination.SetMessages(source.GetMessages());
        }
        if (source.GetSize() != 0
                && (destination.GetSize() == 0 || destination.GetSize() < source.GetSize())) {
            destination.SetSize(source.GetSize());
        }
    }

    void AggregatePartitionResults() {
        for (auto& it : Topics) {
            if (!FilterTopic.empty() && FilterTopic != it.first) {
                continue;
            }
            TTopicInfo& topic(it.second);
            for (const auto& pr : topic.PartitionResults) {
                const NKikimrPQ::TClientInfoResponse& pbPartitionInfo(pr.second->Record);
                auto partition = pbPartitionInfo.GetPartition();
                if (!pbPartitionInfo.HasStartOffset() || !pbPartitionInfo.HasEndOffset()) {
                    // partition wasn't found ?
                    continue;
                }
                for (const NKikimrPQ::TClientInfo& pbClientInfo : pbPartitionInfo.GetClientInfo()) {
                    const TString& client = pbClientInfo.GetClientId();
                    if (!FilterConsumer.empty() && FilterConsumer != client) {
                        continue;
                    }
                    NKikimrViewer::TMetaTopicLagInfo committedLags;
                    NKikimrViewer::TMetaTopicLagInfo readLags;
                    const auto& writePos = pbClientInfo.GetWritePosition();
                    ui64 writeLag = pbPartitionInfo.GetEndOffset() - writePos.GetOffset();
                    committedLags.SetMessages(writeLag);
                    if (writeLag != 0) {
                        committedLags.SetSize(writePos.GetSize());
                        if (writePos.HasCreateTimestamp()) {
                            committedLags.SetCreateTime(pbPartitionInfo.GetResponseTimestamp() - writePos.GetCreateTimestamp());
                        }
                        committedLags.SetWriteTime(pbPartitionInfo.GetResponseTimestamp() - writePos.GetWriteTimestamp());
                    }
                    const auto& readPos = pbClientInfo.GetReadPosition();
                    ui64 readLag = pbPartitionInfo.GetEndOffset() - readPos.GetOffset();
                    readLags.SetMessages(readLag);
                    if (readLag != 0) {
                        readLags.SetSize(readPos.GetSize());
                        if (readPos.HasCreateTimestamp()) {
                            readLags.SetCreateTime(pbPartitionInfo.GetResponseTimestamp() - readPos.GetCreateTimestamp());
                        }
                        readLags.SetWriteTime(pbPartitionInfo.GetResponseTimestamp() - readPos.GetWriteTimestamp());
                    }
                    auto itConsumerInfo = ConsumerInfo.find(client);
                    if (itConsumerInfo == ConsumerInfo.end()) {
                        itConsumerInfo = ConsumerInfo.emplace(client, NKikimrViewer::TMetaTopicConsumerInfo()).first;
                        itConsumerInfo->second.SetName(client);
                    }
                    NKikimrViewer::TMetaTopicPartitionInfo& pbPartition = topic.PartitionInfo[partition];
                    pbPartition.SetPartition(partition);
                    UpdateMaximum(*itConsumerInfo->second.MutableCommittedLags(), committedLags);
                    UpdateMaximum(*itConsumerInfo->second.MutableLastReadLags(), readLags);
                    UpdateMaximum(*pbPartition.MutableCommittedLags(), committedLags);
                    UpdateMaximum(*pbPartition.MutableLastReadLags(), readLags);
                    UpdateMaximum(*topic.MutableCommittedLags(), committedLags);
                    UpdateMaximum(*topic.MutableLastReadLags(), readLags);
                }
            }
        }
    }

    void Bootstrap(const TActorContext& ctx) override {
        // TODO: select correct TX proxy
        THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
        if (!BrowseContext.UserToken.empty()) {
            request->Record.SetUserToken(BrowseContext.UserToken);
        }
        NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
        TString path(PQ_ROOT_PATH);
        if (!FilterTopic.empty()) {
            path += '/';
            path += FilterTopic;
        }
        record->SetPath(path);
        ctx.Send(TxProxy, request.Release());
        ++Requests;
        ctx.Send(BrowseContext.Owner, new NViewerEvents::TEvBrowseRequestSent(TxProxy, TEvTxUserProxy::EvNavigate));
        UnsafeBecome(&TThis::StateWork);
    }

    void ReplyAndDie(const TActorContext &ctx) override = 0;
};

class TBrowseConsumers : public TBrowseCommon {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TBrowseConsumers(const TActorId& owner, const IViewer::TBrowseContext& browseContext)
        : TBrowseCommon(owner, browseContext)
    {}

    void ReplyAndDie(const TActorContext &ctx) {
        NKikimrViewer::TBrowseInfo browseInfo;
        NKikimrViewer::TMetaInfo metaInfo;
        FillCommonData(browseInfo, metaInfo);
        AggregatePartitionResults();
        for (const auto& pr : ConsumerInfo) {
            NKikimrViewer::TBrowseInfo& pbConsumer = *browseInfo.AddChildren();
            pbConsumer.SetType(NKikimrViewer::EObjectType::Consumer);
            pbConsumer.SetName(pr.first);
        }
        ctx.Send(Owner, new NViewerEvents::TEvBrowseResponse(std::move(browseInfo), std::move(metaInfo)));
        Die(ctx);
    }
};

class TBrowseConsumer : public TBrowseCommon {
public:
    TBrowseConsumer(const TActorId& owner, const IViewer::TBrowseContext& browseContext)
        : TBrowseCommon(owner, browseContext)
    {
        FilterConsumer = BrowseContext.GetMyName();
        if (BrowseContext.GetParentType() == NKikimrViewer::EObjectType::Topic) {
            FilterTopic = BrowseContext.GetParentName();
        }
    }

    void ReplyAndDie(const TActorContext &ctx) {
        NKikimrViewer::TBrowseInfo browseInfo;
        NKikimrViewer::TMetaInfo metaInfo;
        FillCommonData(browseInfo, metaInfo);
        AggregatePartitionResults();
        for (const auto& pr : Topics) {
            NKikimrViewer::TBrowseInfo& pbTopic = *browseInfo.AddChildren();
            pbTopic.SetType(NKikimrViewer::EObjectType::Topic);
            pbTopic.SetName(pr.first);
            pbTopic.SetFinal(true);
        }
        if (ConsumerInfo.size() == 1) {
            const NKikimrViewer::TMetaTopicConsumerInfo& consumer = ConsumerInfo.begin()->second;
            NKikimrViewer::TMetaTopicConsumerInfo& pbMetaConsumer = *metaInfo.MutableConsumer();
            pbMetaConsumer.MutableCommittedLags()->MergeFrom(consumer.GetCommittedLags());
            pbMetaConsumer.MutableLastReadLags()->MergeFrom(consumer.GetLastReadLags());
            for (const auto& pr : Topics) {
                auto& pbTopic = *pbMetaConsumer.AddTopics();
                pbTopic.SetName(pr.first);
                pbTopic.MergeFrom(pr.second);
            }
            if (!FilterTopic.empty()) {
                if (!Topics.empty() && pbMetaConsumer.TopicsSize() > 0) {
                    TTopicInfo& topic = Topics.begin()->second;
                    auto& pbMetaTopic = *pbMetaConsumer.MutableTopics(0);
                    for (const auto& pr : topic.PartitionInfo) {
                        auto& pbPartition = *pbMetaTopic.AddPartitions();
                        pbPartition.MergeFrom(pr.second);
                    }
                }
            }
        }
        ctx.Send(Owner, new NViewerEvents::TEvBrowseResponse(std::move(browseInfo), std::move(metaInfo)));
        Die(ctx);
    }
};

class TBrowseTopic : public TBrowseCommon {
public:
    TBrowseTopic(const TActorId& owner, const IViewer::TBrowseContext& browseContext)
        : TBrowseCommon(owner, browseContext)
    {
        FilterTopic = BrowseContext.GetMyName();
        if (BrowseContext.GetParentType() == NKikimrViewer::EObjectType::Consumer) {
            FilterConsumer = BrowseContext.GetParentName();
        }
    }

    void ReplyAndDie(const TActorContext &ctx) {
        NKikimrViewer::TBrowseInfo browseInfo;
        NKikimrViewer::TMetaInfo metaInfo;
        FillCommonData(browseInfo, metaInfo);
        AggregatePartitionResults();
        for (const auto& pr : ConsumerInfo) {
            NKikimrViewer::TBrowseInfo& pbConsumer = *browseInfo.AddChildren();
            pbConsumer.SetType(NKikimrViewer::EObjectType::Consumer);
            pbConsumer.SetName(pr.first);
            pbConsumer.SetFinal(true);
        }
        if (DescribeResult != nullptr) {
            const auto& pbRecord(DescribeResult->GetRecord());
            if (pbRecord.HasPathDescription()) {
                const auto& pbPathDescription(pbRecord.GetPathDescription());
                if (pbPathDescription.HasPersQueueGroup()) {
                    const auto& pbPersQueueGroup(pbPathDescription.GetPersQueueGroup());
                    NKikimrViewer::TMetaCommonInfo& pbCommon = *metaInfo.MutableCommon();
                    if (pbPersQueueGroup.PartitionsSize()) {
                        pbCommon.SetPartitions(pbPersQueueGroup.PartitionsSize());
                    }
                    if (pbPersQueueGroup.HasPQTabletConfig()) {
                        const auto& pbPQTabletConfig(pbPersQueueGroup.GetPQTabletConfig());
                        if (pbPQTabletConfig.HasPartitionConfig()) {
                            const auto& pbPartitionConfig(pbPQTabletConfig.GetPartitionConfig());
                            NKikimrViewer::TMetaTopicInfo& pbMetaTopic = *metaInfo.MutableTopic();
                            pbMetaTopic.MutablePartitionConfig()->SetMaxCountInPartition(pbPartitionConfig.GetMaxCountInPartition());
                            pbMetaTopic.MutablePartitionConfig()->SetMaxSizeInPartition(pbPartitionConfig.GetMaxSizeInPartition());
                            pbMetaTopic.MutablePartitionConfig()->SetLifetimeSeconds(pbPartitionConfig.GetLifetimeSeconds());
                        }
                    }
                }
            }
        }
        if (Topics.size() == 1) {
            TTopicInfo& topic = Topics.begin()->second;
            NKikimrViewer::TMetaTopicInfo& pbMetaTopic = *metaInfo.MutableTopic();
            pbMetaTopic.MutableCommittedLags()->MergeFrom(topic.GetCommittedLags());
            pbMetaTopic.MutableLastReadLags()->MergeFrom(topic.GetLastReadLags());
            for (const auto& pr : ConsumerInfo) {
                auto& pbClient = *pbMetaTopic.AddConsumers();
                pbClient.MergeFrom(pr.second);
            }
            for (const auto& pr : topic.PartitionInfo) {
                auto& pbPartition = *pbMetaTopic.AddPartitions();
                pbPartition.MergeFrom(pr.second);
            }
        }

        ctx.Send(Owner, new NViewerEvents::TEvBrowseResponse(std::move(browseInfo), std::move(metaInfo)));
        Die(ctx);
    }
};

}
