#include "deduplication_write_queue.h"

#include <ydb/core/persqueue/pqtablet/common/logging.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/library/actors/core/event_local.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <util/generic/overloaded.h>
#include <util/string/join.h>
#include <ranges>

namespace NKikimr::NPQ {


using EWriteExternalDeduplicationStatus = TEvPQ::TEvWrite::EWriteExternalDeduplicationStatus;
using EMessageExternalDeduplicationStatus = TEvPQ::TEvWrite::EMessageExternalDeduplicationStatus;

class TDeduplicationQueueActor: public TBaseTabletActor<TDeduplicationQueueActor>
                              , public TConstantLogPrefix {
public:
    TDeduplicationQueueActor(
        ui64 tabletId,
        TActorId tabletActorId,
        TActorId partitionActorId,
        TString topicName,
        ui32 partitionId,
        TVector<NKikimrPQ::TPQTabletConfig::TPartition> parentPartitions)
        : TBaseTabletActor(tabletId, tabletActorId, NKikimrServices::EServiceKikimr::PERSQUEUE)
        , PartitionActorId(partitionActorId)
        , TopicName(std::move(topicName))
        , PartitionId(partitionId)
    {
        DisableTimestamp = TInstant::Zero();
        ParentPartitions.reserve(parentPartitions.size());
        for (const auto& partition : parentPartitions) {
            TParentPartitionInfo info{
                .CreationTime = TInstant::Max(), // TODO: Get from partition graph/config
                .TabletId = partition.GetTabletId(),
                .PartitionId = partition.GetPartitionId(),
            };
            DisableTimestamp = Max(DisableTimestamp, info.CreationTime + MaxDeduplicationTimeInterval);
            ParentPartitions.push_back(std::move(info));
            TabletInfo[info.TabletId].Partitions.push_back(partition.GetPartitionId());
        }
        std::ranges::sort(ParentPartitions.begin(), ParentPartitions.end(), std::greater<>{}, &TParentPartitionInfo::PartitionId); // oldest partitions at end
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[DeduplicationQueue][" << PartitionId << "][" << TopicName << "] ";
    }

private:
    struct TParentPartitionInfo {
        TInstant CreationTime;
        ui64 TabletId;
        ui32 PartitionId;
    };

    struct TTabletInfo {
        TVector<ui32> Partitions;
        bool Subscribed = false;
        ui32 Generation = 0;
    };

    struct TWriteRequest {
        explicit TWriteRequest(TEvPQ::TEvWrite::TPtr event, absl::flat_hash_set<TString> unresolvedDeduplicationIds)
            : Event(std::move(event))
            , UnresolvedDeduplicationIds(std::move(unresolvedDeduplicationIds))
        {
        }

        TEvPQ::TEvWrite::TPtr Event;
        absl::flat_hash_set<TString> UnresolvedDeduplicationIds;
    };

    struct TReserveBytesRequest {
        explicit TReserveBytesRequest(TEvPQ::TEvReserveBytes::TPtr event)
            : Event(std::move(event))
        {
        }
        TEvPQ::TEvReserveBytes::TPtr Event;
    };

    using TQueueRequest = std::variant<TWriteRequest, TReserveBytesRequest>;

    struct TWriteRequestMessageIndex {
        TWriteRequest* RequestPtr;
        size_t MessageIndex = -1;
    };

    struct TDeduplicationInfo {
        TVector<TWriteRequestMessageIndex> RequestsInQueue;

        absl::flat_hash_map<ui32, ui32> RemainsPartitionWithGeneration;

        ui32 PartitionErrors = 0;

        bool IsReady() const {
            return RemainsPartitionWithGeneration.empty() || PartitionErrors;
        }

        void CancelIfNotReady() {
            PartitionErrors += RemainsPartitionWithGeneration.size();
            RemainsPartitionWithGeneration.clear();
        }

        bool IsDuplicate = false;
        ui64 Offset = 0;
        ui32 PartitionId = 0;
    };

private:
    const TActorId PartitionActorId;
    const TString TopicName;
    const ui32 PartitionId;
    TInstant DisableTimestamp = TInstant::Zero();
    TVector<TParentPartitionInfo> ParentPartitions;
    absl::flat_hash_map<ui64, TTabletInfo> TabletInfo;
    const TDuration MaxDeduplicationTimeInterval = TDuration::Minutes(5);

    TDeque<TQueueRequest> Queue;
    using TDeduplicationInfoMap = absl::flat_hash_map<TString, TDeduplicationInfo>;
    TDeduplicationInfoMap DeduplicationInfo;

    using EBypassMode = NPrivate::EBypassMode;
    EBypassMode BypassMode = EBypassMode::Disabled;

private:

    static bool IsUncheckedMsg(const TEvPQ::TEvWrite::TMsg& msg) {
        if (msg.ExternalDeduplicationInfo.Status != TEvPQ::TEvWrite::EMessageExternalDeduplicationStatus::Unchecked) {
            return false;
        }
        if (!msg.MessageDeduplicationId.has_value()) {
            return false;
        }
        return true;
    }

    bool TryBypass(auto& ev) {
        TryToSwitchToBypassMode();
        if (BypassMode == EBypassMode::Enabled) {
            SendEvent(std::move(ev));
            return true;
        }
        return false;
    }

    void Handle(TEvPQ::TEvWrite::TPtr& ev) {
        LOG_D("Handle TEvWrite: " << LabeledOutput(BypassMode));
        if (TryBypass(ev)) {
            return;
        }
        absl::flat_hash_map<TString, TVector<size_t>> messageIndices;
        if (BypassMode == EBypassMode::Disabled) {
            TEvPQ::TEvWrite& write = *ev->Get();
            if (write.ExternalDeduplicationStatus == EWriteExternalDeduplicationStatus::Unchecked) {
                for (size_t i = 0; i < write.Msgs.size(); ++i) {
                    const auto& msg = write.Msgs[i];
                    if (IsUncheckedMsg(msg)) {
                        messageIndices[msg.MessageDeduplicationId.value()].push_back(i);
                    }
                }
            }
        } else {
            // Skip checking parent partitions,
            // but still eunqueu to enforce proper order.
        }
        absl::flat_hash_set<TString> unresolvedDeduplicationIds;
        unresolvedDeduplicationIds.reserve(messageIndices.size());
        for (const auto& [messageDeduplicationId, _] : messageIndices) {
            unresolvedDeduplicationIds.insert(messageDeduplicationId);
        }
        Queue.emplace_back(std::in_place_type<TWriteRequest>, std::move(ev), std::move(unresolvedDeduplicationIds));
        TWriteRequest& record = std::get<TWriteRequest>(Queue.back());
        for (const auto& [messageDeduplicationId, indices] : messageIndices) {
            auto [deduplicationInfoIt, newDeduplicationId] = DeduplicationInfo.try_emplace(messageDeduplicationId);
            auto& deduplicationInfo = deduplicationInfoIt->second;
            if (newDeduplicationId) {
                SendRequests(deduplicationInfoIt);
            }
            for (auto index : indices) {
                deduplicationInfo.RequestsInQueue.push_back(TWriteRequestMessageIndex{
                    .RequestPtr = &record,
                    .MessageIndex = index,
                });
            }
        }
        ProcessQueue();
    }

    void Handle(TEvPQ::TEvReserveBytes::TPtr& ev) {
        LOG_D("Handle TEvReserveBytes: " << LabeledOutput(BypassMode));
        if (TryBypass(ev)) {
            return;
        }
        Queue.emplace_back(std::in_place_type<TReserveBytesRequest>, std::move(ev));
        ProcessQueue();
    }


    void SendRequests(const TDeduplicationInfoMap::iterator it) {
        auto& [messageDeduplicationId, info] = *it;

        for (auto& parentPartition : ParentPartitions) {
            const ui64 tabletId = parentPartition.TabletId;
            auto& tabletInfo = TabletInfo[tabletId];
            auto ev = std::make_unique<NKikimr::TEvPersQueue::TEvCheckMessageDeduplicationRequest>(
                parentPartition.PartitionId,
                tabletInfo.Generation,
                TConstArrayRef(&messageDeduplicationId, 1));
            LOG_D("Send TEvCheckMessageDeduplicationRequest: partition=" << parentPartition.PartitionId << "; tabletId=" << tabletId << "; messageDeduplicationId=" << messageDeduplicationId);
            auto forward = std::make_unique<TEvPipeCache::TEvForward>(
                ev.release(),
                tabletId,
                !tabletInfo.Subscribed,
                tabletId);
            tabletInfo.Subscribed = true;
            info.RemainsPartitionWithGeneration[parentPartition.PartitionId] = tabletInfo.Generation;
            Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
        }
    }

    void Handle(NKikimr::TEvPersQueue::TEvCheckMessageDeduplicationResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        LOG_D("Handle TEvCheckMessageDeduplicationResponse: " << record.ShortUtf8DebugString());
        for (const auto& [messageDeduplicationId, result] : record.GetResult()) {
            auto deduplicationInfoIt = DeduplicationInfo.find(messageDeduplicationId);
            if (deduplicationInfoIt == DeduplicationInfo.end()) {
                LOG_D("Got unknown messageDeduplicationId=" << messageDeduplicationId << " in TEvCheckMessageDeduplicationResponse");
                continue;
            }
            auto& deduplicationInfo = deduplicationInfoIt->second;
            if (auto it = deduplicationInfo.RemainsPartitionWithGeneration.find(record.GetPartitionId());
                it == deduplicationInfo.RemainsPartitionWithGeneration.end()) {
                LOG_D("Got unknown partition for messageDeduplicationId=" << messageDeduplicationId << " in TEvCheckMessageDeduplicationResponse");
                continue;
            } else if (it->second > record.GetGeneration()) {
                LOG_D("Got wrong generation for messageDeduplicationId=" << messageDeduplicationId << " in TEvCheckMessageDeduplicationResponse");
                continue;
            } else {
                deduplicationInfo.RemainsPartitionWithGeneration.erase(it);
            }
            if (record.GetStatus() != NKikimrPQ::EStatus::OK) {
                deduplicationInfo.PartitionErrors += 1;
            } else {
                if (result.GetIsDuplicate()) {
                    deduplicationInfo.IsDuplicate = true;
                    deduplicationInfo.Offset = result.GetOffset();
                    deduplicationInfo.PartitionId = record.GetPartitionId();
                }
            }
            TryFinalizeDeduplicationInfo(deduplicationInfoIt);
        }
        ProcessQueue();
    }

    void TryFinalizeDeduplicationInfo(const TDeduplicationInfoMap::iterator it) {
        auto& [deduplicateMessageId, info] = *it;
        if (!info.RemainsPartitionWithGeneration.empty() && info.PartitionErrors == 0) {
            return;
        }
        TEvPQ::TEvWrite::TMessageExternalDeduplicationInfo messageExternalDeduplicationInfo;
        if (info.PartitionErrors != 0) {
            messageExternalDeduplicationInfo.Status = EMessageExternalDeduplicationStatus::Error;
        } else if (info.IsDuplicate) {
            messageExternalDeduplicationInfo.Status = EMessageExternalDeduplicationStatus::Duplicate;
            messageExternalDeduplicationInfo.OriginalPartitionAndOffset.emplace(info.PartitionId, info.Offset);
        } else {
            messageExternalDeduplicationInfo.Status = EMessageExternalDeduplicationStatus::Unique;
        }
        for (const TWriteRequestMessageIndex req : info.RequestsInQueue) {
            TWriteRequest* ptr = req.RequestPtr;
            ptr->UnresolvedDeduplicationIds.erase(deduplicateMessageId);
            auto& evWrite = *ptr->Event->Get();
            AFL_ENSURE(req.MessageIndex < evWrite.Msgs.size())("index", req.MessageIndex)("size", evWrite.Msgs.size());
            auto& msg = evWrite.Msgs[req.MessageIndex];
            AFL_ENSURE(msg.MessageDeduplicationId == deduplicateMessageId)("msg", msg.MessageDeduplicationId)("info", deduplicateMessageId);
            msg.ExternalDeduplicationInfo = messageExternalDeduplicationInfo;
            if (messageExternalDeduplicationInfo.Status == EMessageExternalDeduplicationStatus::Error) {
                // propagate error to the whole evWrite
                evWrite.ExternalDeduplicationStatus = EWriteExternalDeduplicationStatus::Error;
            }
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        const TEvPipeCache::TEvDeliveryProblem& record = *ev->Get();
        auto& tabletInfo = TabletInfo[record.TabletId];
        tabletInfo.Subscribed = false;
        tabletInfo.Generation += 1;
        for (auto it = DeduplicationInfo.begin(); it != DeduplicationInfo.end(); ++it) {
            auto& [messageDeduplicationId, deduplicationInfo] = *it;
            size_t affectedPartitions = 0;
            for (const ui32 partitionId : tabletInfo.Partitions) {
                affectedPartitions += deduplicationInfo.RemainsPartitionWithGeneration.erase(partitionId);
            }
            if (affectedPartitions != 0) {
                deduplicationInfo.PartitionErrors += affectedPartitions;
                TryFinalizeDeduplicationInfo(it);
            }
        }
        ProcessQueue();
    }

    static bool SetChecked(EWriteExternalDeduplicationStatus& status) {
        if (status == EWriteExternalDeduplicationStatus::Unchecked) {
            status = EWriteExternalDeduplicationStatus::Checked;
            return true;
        }
        return false;
    };

    void SendEvent(TEvPQ::TEvWrite::TPtr ev) {
        bool update = SetChecked(ev->Get()->ExternalDeduplicationStatus);
        LOG_D("Forward event " << ev->GetTypeRewrite() << " to " << PartitionActorId << "; update=" << update);
        Forward(ev, PartitionActorId);
    }

    void SendEvent(TEvPQ::TEvReserveBytes::TPtr ev) {
        bool prevFromDeduplicatedQueue = std::exchange(ev->Get()->FromDeduplicatedQueue, true);
        AFL_ENSURE(prevFromDeduplicatedQueue == false);
        LOG_D("Forward event " << ev->GetTypeRewrite() << " to " << PartitionActorId << "; update=" << !prevFromDeduplicatedQueue);
        Forward(ev, PartitionActorId);
    }

    void ProcessQueue() {
        while (!Queue.empty()) {
            auto& req = Queue.front();
            bool proceed = std::visit(TOverloaded{
                [&](TWriteRequest& req) {
                    if (!req.UnresolvedDeduplicationIds.empty()) {
                        return false;
                    }
                    SendEvent(req.Event);
                    return true;
                },
                [&](TReserveBytesRequest& req) {
                    SendEvent(req.Event);
                    return true;
                }
            }, req);
            if (!proceed) {
                break;
            }
            Queue.pop_front();
        }
        TryToSwitchToBypassMode();
    }

    void CancelQueue() {
        for (auto it = DeduplicationInfo.begin(); it != DeduplicationInfo.end(); ++it) {
            auto& [messageDeduplicationId, deduplicationInfo] = *it;
            deduplicationInfo.CancelIfNotReady();
            TryFinalizeDeduplicationInfo(it);
        }
        ProcessQueue();
        AFL_VERIFY_DEBUG(Queue.empty())("size", Queue.size());
    }

    void PassAway() override {
        CancelQueue();
        TActorBootstrapped::PassAway();
    }

    void TryToSwitchToBypassMode() {
        TMaybe<EBypassMode> newMode;
        switch (BypassMode) {
            case EBypassMode::Disabled:
                if (TAppData::TimeProvider->Now() < DisableTimestamp) [[likely]] {
                    return;
                }
                newMode = EBypassMode::Pending;
                [[fallthrough]];
            case EBypassMode::Pending:
                if (Queue.empty()) {
                    newMode = EBypassMode::Enabled;
                }
                break;
            case EBypassMode::Enabled:
                [[likely]] return;
        }
        if (!newMode.Defined()) {
            return;
        }
        AFL_ENSURE(newMode != BypassMode)("BypassMode", BypassMode)("NewMode", newMode);
        LOG_D("SwitchToBypassMode " << *newMode);
        if (newMode == EBypassMode::Enabled) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        }
        BypassMode = *newMode;
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPQ::TEvWrite, Handle);
            hFunc(TEvPQ::TEvReserveBytes, Handle);
            hFunc(NKikimr::TEvPersQueue::TEvCheckMessageDeduplicationResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
            default:
                LOG_E("Unexpected " << EventStr("StateWork", ev));
                AFL_VERIFY_DEBUG(false)("Unexpected", EventStr("StateInit", ev));
        }
    }

};

NActors::IActor* CreateDeduplicationWriteQueueActor(
    ui64 tabletId,
    TActorId tabletActorId,
    TActorId partitionActorId,
    TString topicName,
    ui32 partitionId,
    TVector<NKikimrPQ::TPQTabletConfig::TPartition> parentPartitions) {
    return new TDeduplicationQueueActor(
        tabletId,
        tabletActorId,
        partitionActorId,
        std::move(topicName),
        partitionId,
        std::move(parentPartitions));
}

} // namespace NKikimr::NPQ
