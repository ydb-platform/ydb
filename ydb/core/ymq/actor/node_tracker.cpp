#include "node_tracker.h"

#include <ydb/core/base/path.h>
#include <ydb/core/ymq/actor/cfg.h>
#include <ydb/core/ymq/actor/serviceid.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>

#include <util/string/vector.h>

namespace {
    ui64 GetKeyCellValue(const NKikimr::TCell& cell) {
        return cell.IsNull() ? Max<ui64>() : cell.AsValue<ui64>();
    }

    std::tuple<ui64, ui64> GetKeysPrefix(const  TConstArrayRef<NKikimr::TCell>& cells) {
        if (cells.empty()) {
            return {Max<ui64>(), Max<ui64>()};
        }
        return {GetKeyCellValue(cells[0]), GetKeyCellValue(cells[1])};
    }

    constexpr ui64 STD_PATH_SUBSCRIPTION_KEY = 1;
    constexpr ui64 FIFO_PATH_SUBSCRIPTION_KEY = 2;
}

namespace NKikimr::NSQS {
    TNodeTrackerActor::TSubscriberInfo::TSubscriberInfo(
        ui64 queueIdNumber,
        bool isFifo,
        std::optional<ui64> specifiedLeaderTabletId,
        std::optional<ui32> nodeId
    )
        : QueueIdNumber(queueIdNumber)
        , IsFifo(isFifo)
        , SpecifiedLeaderTabletId(specifiedLeaderTabletId)
        , NodeId(nodeId)
    {}

    const char* TNodeTrackerActor::GetLogPrefix() {
        return "[Node tracker] ";
    }

    TNodeTrackerActor::TNodeTrackerActor(NActors::TActorId schemeCacheActor)
        : SchemeCacheActor(schemeCacheActor)
        , TablePathSTD(NKikimr::SplitPath(Cfg().GetRoot() + "/.STD/Messages"))
        , TablePathFIFO(NKikimr::SplitPath(Cfg().GetRoot() + "/.FIFO/Messages"))
    {
    }

    void TNodeTrackerActor::Bootstrap(const NActors::TActorContext& ctx) {
        ParentActor = MakeSqsServiceID(SelfId().NodeId());
        Become(&TNodeTrackerActor::WorkFunc);
        ScheduleDescribeTables(TDuration::Zero(), ctx);
        Schedule(CLEANUP_UNUSED_TABLETS_PERIOD, new TEvents::TEvWakeup());
        LOG_SQS_DEBUG(GetLogPrefix() << "bootstrap on node=" << SelfId().NodeId());
    }

    void TNodeTrackerActor::HandleWakeup(TEvWakeup::TPtr&, const NActors::TActorContext& ctx) {
        // removed unused tablets
        for (auto it = LastAccessOfTabletWithoutSubscribers.begin(); it != LastAccessOfTabletWithoutSubscribers.end();) {
            ui64 tabletId = it->first;
            TInstant lastAccess = it->second;
            auto currentIt = it++;
            if (ctx.Now() - lastAccess >= CLEANUP_UNUSED_TABLETS_PERIOD) {
                auto infoIt = TabletsInfo.find(tabletId);
                if (infoIt != TabletsInfo.end()) {
                    ClosePipeToTablet(infoIt->second);
                    TabletsInfo.erase(infoIt);
                    LastAccessOfTabletWithoutSubscribers.erase(currentIt);
                } else {
                    LOG_SQS_ERROR(GetLogPrefix() << "unknown tabletId=" << tabletId << " with last access at " << lastAccess << " in unused tablets cleanup");
                }
            }
        }

        Schedule(CLEANUP_UNUSED_TABLETS_PERIOD, new TEvents::TEvWakeup());
    }

    void TNodeTrackerActor::ScheduleDescribeTables(TDuration runAfter, const NActors::TActorContext& ctx) {
        LOG_SQS_NOTICE(GetLogPrefix() << "schedule describe tables after " << runAfter);
        auto navigateRequest = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        //navigateRequest->DatabaseName = Cfg().GetRoot();

        navigateRequest->ResultSet.resize(2);
        navigateRequest->ResultSet.front().Path = TablePathSTD;
        navigateRequest->ResultSet.back().Path = TablePathFIFO;
        ctx.ExecutorThread.ActorSystem->Schedule(
            runAfter,
            new IEventHandle(
                SchemeCacheActor,
                SelfId(),
                new TEvTxProxySchemeCache::TEvNavigateKeySet(navigateRequest.release())
            )
        );
    }

    void TNodeTrackerActor::DescribeTablesFailed(const TString& error, const NActors::TActorContext& ctx) {
        LOG_SQS_ERROR(GetLogPrefix() << "describe tables failed: " << error);
        DescribeTablesRetyPeriod = Min(
            DESCRIBE_TABLES_PERIOD_MAX,
            2 * Max(DescribeTablesRetyPeriod, TDuration::MilliSeconds(100))
        );
        ScheduleDescribeTables(DescribeTablesRetyPeriod, ctx);
    }

    void TNodeTrackerActor::HandlePipeClientConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx) {
        ui64 tabletId = ev->Get()->TabletId;
        auto it = TabletsInfo.find(tabletId);
        if (it == TabletsInfo.end()) {
            LOG_SQS_WARN(GetLogPrefix() << "connected to unrequired tablet. Tablet id: [" << tabletId << "]. Client pipe actor: " << ev->Get()->ClientId << ". Server pipe actor: " << ev->Get()->ServerId);
            return;
        }

        auto& info = it->second;
        if (ev->Get()->Status != NKikimrProto::OK) {
            LOG_SQS_WARN(GetLogPrefix() << "failed to connect to tablet " << tabletId << " dead=" << ev->Get()->Dead);

            if (ev->Get()->Dead) {
                MoveSubscribersFromTablet(tabletId, info, ctx);
                return;
            }
            ReconnectToTablet(tabletId);
            return;
        }

        LOG_SQS_DEBUG(GetLogPrefix() << "connected to tabletId [" << tabletId << "]. Client pipe actor: " << ev->Get()->ClientId << ". Server pipe actor: " << ev->Get()->ServerId);
        info.PipeServer = ev->Get()->ServerId;
        ui32 nodeId = info.PipeServer.NodeId();
        for (auto& [id, subscriber] : info.Subscribers) {
            if (!subscriber->NodeId || subscriber->NodeId.value() != nodeId) {
                subscriber->NodeId = nodeId;
                AnswerForSubscriber(id, nodeId);
            }
        }
    }

    void TNodeTrackerActor::AnswerForSubscriber(ui64 subscriptionId, ui32 nodeId, bool disconnected) {
        Send(ParentActor, new TSqsEvents::TEvNodeTrackerSubscriptionStatus(subscriptionId, nodeId, disconnected));
    }

    void TNodeTrackerActor::HandlePipeClientDisconnected(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext&) {
        ui64 tabletId = ev->Get()->TabletId;
        auto it = TabletsInfo.find(tabletId);
        if (it != TabletsInfo.end()) {
            LOG_SQS_DEBUG(GetLogPrefix() << "tablet pipe " << tabletId << " disconnected");

            auto& info = it->second;
            if (info.PipeServer) {
                for (auto& [id, subscriber] : info.Subscribers) {
                    if (subscriber->NodeId) {
                        AnswerForSubscriber(id, subscriber->NodeId.value(), true);
                    }
                }
            }
            ReconnectToTablet(tabletId);
        } else {
            LOG_SQS_WARN(GetLogPrefix() << " disconnected from unrequired tablet id: [" << ev->Get()->TabletId << "]. Client pipe actor: " << ev->Get()->ClientId << ". Server pipe actor: " << ev->Get()->ServerId);
        }
    }

    void TNodeTrackerActor::ClosePipeToTablet(TTabletInfo& info) {
        if (info.PipeClient) {
            NTabletPipe::CloseClient(SelfId(), info.PipeClient);
            info.PipeClient = info.PipeServer = TActorId();
        }
    }

    TNodeTrackerActor::TTabletInfo& TNodeTrackerActor::ConnectToTablet(ui64 tabletId, bool isReconnect) {
        LOG_SQS_DEBUG(GetLogPrefix() << "connect to tablet " << tabletId << " is_reconnect=" << isReconnect);
        NTabletPipe::TClientConfig cfg;
        cfg.AllowFollower = false;
        cfg.CheckAliveness = true;
        cfg.RetryPolicy = {.RetryLimitCount = 3, .MinRetryTime = TDuration::MilliSeconds(100), .DoFirstRetryInstantly = !isReconnect};

        auto& info = TabletsInfo[tabletId];
        ClosePipeToTablet(info);
        info.PipeClient = Register(NTabletPipe::CreateClient(SelfId(), tabletId, cfg));
        return info;
    }

    TNodeTrackerActor::TTabletInfo& TNodeTrackerActor::ReconnectToTablet(ui64 tabletId) {
        return ConnectToTablet(tabletId, true);
    }

    TNodeTrackerActor::TTabletInfo& TNodeTrackerActor::GetTabletInfo(ui64 tabletId) {
        auto it = TabletsInfo.find(tabletId);
        if (it == TabletsInfo.end()) {
            return ConnectToTablet(tabletId);
        }
        return it->second;
    }

    void TNodeTrackerActor::RemoveSubscriber(TSqsEvents::TEvNodeTrackerUnsubscribeRequest::TPtr& request, const NActors::TActorContext& ctx) {
        ui64 subscriptionId = request->Get()->SubscriptionId;
        LOG_SQS_DEBUG(GetLogPrefix() << "remove subscriber with id=" << subscriptionId);
        auto it = TabletPerSubscriptionId.find(subscriptionId);
        if (it != TabletPerSubscriptionId.end()) {
            auto tabletIt = TabletsInfo.find(it->second);
            if (tabletIt != TabletsInfo.end()) {
                auto& info = tabletIt->second;
                info.Subscribers.erase(subscriptionId);
                if (info.Subscribers.empty()) {
                    LastAccessOfTabletWithoutSubscribers[tabletIt->first] = ctx.Now();
                }
            } else {
                LOG_SQS_WARN("Node tracker removing subscription " << subscriptionId << "that is not found in the tablet information " << tabletIt->first);
            }
            TabletPerSubscriptionId.erase(it);
        } else {
            auto subscriptionIt = SubscriptionsAwaitingPartitionsUpdates.find(subscriptionId);
            if (subscriptionIt != SubscriptionsAwaitingPartitionsUpdates.end()) {
                SubscriptionsAwaitingPartitionsUpdates.erase(subscriptionIt);
            } else {
                LOG_SQS_WARN("Node tracker removing unknown subscription " << subscriptionId);
            }
        }
    }

    bool TNodeTrackerActor::SubscriberMustWait(const TSubscriberInfo& subscriber) const {
        if (subscriber.SpecifiedLeaderTabletId) {
            return false;
        }
        // partitions of the common table have not yet been received
        if (subscriber.IsFifo) {
            return TabletsPerEndKeyRangeFIFO.empty();
        }
        return TabletsPerEndKeyRangeSTD.empty();
    }

    void TNodeTrackerActor::AddSubscriber(TSqsEvents::TEvNodeTrackerSubscribeRequest::TPtr& request, const NActors::TActorContext& ctx) {
        auto& req = *request->Get();
        LOG_SQS_DEBUG(GetLogPrefix() << "add subscriber on init with id=" << req.SubscriptionId
            << " queue_id_number=" << req.QueueIdNumber << " is_fifo=" << req.IsFifo
            << " tablet_id=" << req.TabletId.value_or(0)
        );
        auto subscriber = std::make_unique<TSubscriberInfo>(req.QueueIdNumber, req.IsFifo, req.TabletId);

        if (SubscriberMustWait(*subscriber)) {
            SubscriptionsAwaitingPartitionsUpdates[req.SubscriptionId] = std::move(subscriber);
            return;
        }

        AddSubscriber(req.SubscriptionId, std::move(subscriber), ctx);
    }

    void TNodeTrackerActor::AddSubscriber(ui64 subscriptionId, std::unique_ptr<TSubscriberInfo> subscriber, const NActors::TActorContext&) {
        ui64 tabletId = GetTabletId(*subscriber);
        TabletPerSubscriptionId[subscriptionId] = tabletId;
        TTabletInfo& info = GetTabletInfo(tabletId);

        if (info.PipeServer) {
            ui32 nodeId = info.PipeServer.NodeId();
            subscriber->NodeId = nodeId;
            AnswerForSubscriber(subscriptionId, nodeId);
        }
        LOG_SQS_DEBUG(GetLogPrefix() << "add subscriber queue_id_number=" << subscriber->QueueIdNumber
            << " leader_tablet_specified=" << subscriber->SpecifiedLeaderTabletId.has_value()
            << " tablet_id=" << tabletId
            << " node_resolved=" << subscriber->NodeId.has_value() << "/" << subscriber->NodeId.value_or(0));
        info.Subscribers[subscriptionId] = std::move(subscriber);
        if (info.Subscribers.size() == 1) {
            LastAccessOfTabletWithoutSubscribers.erase(tabletId);
        }
    }

    ui64 TNodeTrackerActor::GetTabletId(const TMap<TKeyPrefix, ui64>& tabletsPerEndKeyRange, TKeyPrefix keyPrefix) const {
        auto it = tabletsPerEndKeyRange.lower_bound(keyPrefix);
        Y_ABORT_UNLESS(it != tabletsPerEndKeyRange.end());
        return it->second;
    }

    ui64 TNodeTrackerActor::GetTabletId(const TSubscriberInfo& subscriber) const {
        if (subscriber.SpecifiedLeaderTabletId) {
            return subscriber.SpecifiedLeaderTabletId.value();
        }

        TKeyPrefix keyPrefix;
        if (subscriber.IsFifo) {
            keyPrefix = {GetKeysHash(subscriber.QueueIdNumber), subscriber.QueueIdNumber};
            return GetTabletId(TabletsPerEndKeyRangeFIFO, keyPrefix);
        }

        keyPrefix = {GetKeysHash(subscriber.QueueIdNumber, 0 /*shard*/), subscriber.QueueIdNumber};
        return GetTabletId(TabletsPerEndKeyRangeSTD, keyPrefix);
    }

    void TNodeTrackerActor::MoveSubscribersFromTablet(ui64 tabletId, const NActors::TActorContext& ctx) {
        auto it = TabletsInfo.find(tabletId);
        if (it != TabletsInfo.end()) {
            MoveSubscribersFromTablet(tabletId, it->second, ctx);
        }
    }

    void TNodeTrackerActor::MoveSubscribersFromTablet(ui64 tabletId, TTabletInfo& info, const NActors::TActorContext& ctx) {
        LOG_SQS_DEBUG(GetLogPrefix() << "move subscribers from " << tabletId);
        if (!info.Subscribers.empty()) {
            for (auto& [id, subscriber] : info.Subscribers) {
                SubscriptionsAwaitingPartitionsUpdates[id] = std::move(subscriber);
                TabletPerSubscriptionId.erase(id);
            }
            info.Subscribers.clear();
            LastAccessOfTabletWithoutSubscribers[tabletId] = ctx.Now();
        }
    }

    void TNodeTrackerActor::MoveSubscribersAfterKeyRangeChanged(
        const TMap<TKeyPrefix, ui64>& current,
        const TMap<TKeyPrefix, ui64>& actual,
        const NActors::TActorContext& ctx
    ) {
        TKeyPrefix lastCurrent{0, 0};
        TKeyPrefix lastActual{0, 0};

        auto currentIt = current.begin();
        auto actualIt = actual.begin();
        while (currentIt != current.end() && actualIt != actual.end()) {
            std::pair<TKeyPrefix, TKeyPrefix> intervalC(lastCurrent, currentIt->first);
            std::pair<TKeyPrefix, TKeyPrefix> intervalA(lastActual, actualIt->first);
            if (intervalA == intervalC) {
                if (currentIt->second != actualIt->second) {
                    MoveSubscribersFromTablet(currentIt->second, ctx);
                }
                ++currentIt;
                ++actualIt;
            } else if (intervalC.second < intervalA.first) { // don't intersect (CCCCC]...(AAAAA]
                MoveSubscribersFromTablet(currentIt->second, ctx);
                ++currentIt;
            } else if (intervalA.second < intervalC.first) { // don't intersect (AAAAA]...(CCCCC]
                ++actualIt;
            } else { // intersect
                MoveSubscribersFromTablet(currentIt->second, ctx);
                if (intervalC.second <= intervalA.second) {
                    ++currentIt;
                } else {
                    ++actualIt;
                }
            }
            lastCurrent = intervalC.second;
            lastActual = intervalA.second;
        }
        Y_ABORT_UNLESS(currentIt == current.end());
    }

    void TNodeTrackerActor::UpdateKeyRanges(
        TMap<TKeyPrefix, ui64>& currentTabletsPerEndKeyRange,
        const NKikimrSchemeOp::TPathDescription& description,
        const NActors::TActorContext& ctx
    ) {
        TMap<TKeyPrefix, ui64> tabletsPerEndKeyRange;
        for (auto part : description.GetTablePartitions()) {
            TSerializedCellVec endKeyPrefix(part.GetEndOfRangeKeyPrefix());
            auto cells = endKeyPrefix.GetCells();
            auto endKeyRange = GetKeysPrefix(cells);
            tabletsPerEndKeyRange[endKeyRange] = part.GetDatashardId();
        }

        MoveSubscribersAfterKeyRangeChanged(currentTabletsPerEndKeyRange, tabletsPerEndKeyRange, ctx);
        currentTabletsPerEndKeyRange = std::move(tabletsPerEndKeyRange);
    }

    void TNodeTrackerActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const NActors::TActorContext& ctx) {
        LOG_SQS_DEBUG(GetLogPrefix() << "got tables description.");
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        Y_ABORT_UNLESS(result->ResultSet.size() == 2);
        for (auto result : result->ResultSet) {
            if (result.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                DescribeTablesFailed(TStringBuilder() << "describe tables failed : " << result.ToString(), ctx);
                return;
            }
            LOG_SQS_INFO(GetLogPrefix() << "got table description: " << result.ToString());
            bool isFifo = (result.Path == TablePathFIFO);
            ui64 pathSubscriptonKey = isFifo ? FIFO_PATH_SUBSCRIPTION_KEY : STD_PATH_SUBSCRIPTION_KEY;
            Send(SchemeCacheActor, new TEvTxProxySchemeCache::TEvWatchPathId(result.TableId.PathId, pathSubscriptonKey));
        }
        DescribeTablesRetyPeriod = DESCRIBE_TABLES_PERIOD_MIN;
    }

    void TNodeTrackerActor::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const NActors::TActorContext& ctx) {
        const auto& describeResult = *ev->Get()->Result;
        const auto& pathDescription = describeResult.GetPathDescription();
        LOG_SQS_INFO(GetLogPrefix() << "got actual description for ["
            << ev->Get()->PathId << " / " << describeResult.GetPath() << "] partitions=" << pathDescription.GetTablePartitions().size());

        bool isFifo = ev->Get()->Key == FIFO_PATH_SUBSCRIPTION_KEY;
        if (isFifo) {
            UpdateKeyRanges(TabletsPerEndKeyRangeFIFO, pathDescription, ctx);
        } else {
            UpdateKeyRanges(TabletsPerEndKeyRangeSTD, pathDescription, ctx);
        }

        auto it = SubscriptionsAwaitingPartitionsUpdates.begin();
        while (it != SubscriptionsAwaitingPartitionsUpdates.end()) {
            if (SubscriberMustWait(*it->second)) {
                ++it;
            } else {
                AddSubscriber(it->first, std::move(it->second), ctx);
                auto itToRemove = it;
                ++it;
                SubscriptionsAwaitingPartitionsUpdates.erase(itToRemove);
            }
        }
    }

} // namespace NKikimr::NSQS
