#pragma once

#include "defs.h"
#include "common.h"
#include "event.h"

namespace NKikimr::NBsQueue {

static constexpr size_t MaxUnusedItems = 1024;

class TBlobStorageQueue {
    enum class EItemQueue {
        NotSet,
        Waiting,
        InFlight
    };

    template<typename TKey>
    struct TCompare {
        template<typename TItem>
        static bool Compare(const TItem& left, const TItem& right) {
            return left.GetKey() < right.GetKey();
        }
        template<typename TItem>
        static bool Compare(const TKey& left, const TItem& right) {
            return left < right.GetKey();
        }
        template<typename TItem>
        static bool Compare(const TItem& left, const TKey& right) {
            return left.GetKey() < right;
        }
    };

    template<typename TDerived>
    struct TSenderNode : public TRbTreeItem<TSenderNode<TDerived>, TCompare<TActorId>> {
        const TActorId& GetKey() const {
            return static_cast<const TDerived&>(*this).Event.GetSender();
        }
    };

    struct TItem
        : public TSenderNode<TItem>
    {
        EItemQueue Queue;
        TCostModel::TMessageCostEssence CostEssence;
        NWilson::TSpan Span;
        TEventHolder Event;
        ui64 MsgId;
        ui64 SequenceId;
        TInstant Deadline;
        const ui64 QueueCookie;
        ui64 Cost;
        bool DirtyCost;
        TBSQueueTimer ProcessingTimer;

        TTrackableList<TItem>::iterator Iterator;

        template<typename TEvent>
        TItem(TAutoPtr<TEventHandle<TEvent>>& event, TInstant deadline,
                const ::NMonitoring::TDynamicCounters::TCounterPtr& serItems,
                const ::NMonitoring::TDynamicCounters::TCounterPtr& serBytes,
                const TBSProxyContextPtr& bspctx, ui32 interconnectChannel,
                bool local, bool useActorSystemTime)
            : Queue(EItemQueue::NotSet)
            , CostEssence(*event->Get())
            , Span(TWilson::VDiskTopLevel, std::move(event->TraceId), "Backpressure.InFlight")
            , Event(event, serItems, serBytes, bspctx, interconnectChannel, local)
            , MsgId(Max<ui64>())
            , SequenceId(0)
            , Deadline(deadline)
            , QueueCookie(RandomNumber<ui64>())
            , Cost(0)
            , DirtyCost(true)
            , ProcessingTimer(useActorSystemTime)
        {
            if (Span) {
                Span
                    .Attribute("event", TypeName<TEvent>())
                    .Attribute("local", local);
            }
        }

        ~TItem() {
            Y_ABORT_UNLESS(Queue == EItemQueue::NotSet, "Queue# %" PRIu32, ui32(Queue));
        }

        ui32 GetByteSize() const {
            return Event.GetByteSize();
        }
    };

    using TItemList = TTrackableList<TItem>;

    struct TQueues {
        TItemList Waiting;
        TItemList InFlight;
        TItemList Unused;

        TQueues(const TBSProxyContextPtr& bspctx)
            : Waiting(TMemoryConsumer(bspctx->Queue))
            , InFlight(TMemoryConsumer(bspctx->Queue))
            , Unused(TMemoryConsumer(bspctx->Queue))
        {}
    };

    using TSenderMap = TRbTree<TSenderNode<TItem>, TCompare<TActorId>>;

    TQueues Queues;
    TSenderMap SenderToItems;
    THashMap<std::pair<ui64, ui64>, TItemList::iterator> InFlightLookup;

    ui64 WindowSize;
    ui64 InFlightCost;
    ui64 NextMsgId;
    ui64 CurrentSequenceId;

    // queue gets "paused" when we are expecting to restart transmission with correct SequenceId/MsgId and we have
    // to wait for all out-of-order answers to arrive
    bool Paused = false;

    TString& LogPrefix;

    std::shared_ptr<const TCostModel> CostModel;
    TInstant CostSettingsUpdate;

    TBSProxyContextPtr BSProxyCtx;

    NBackpressure::TQueueClientId ClientId;

    ui64 BytesWaiting;

    const ui32 InterconnectChannel;

    const bool UseActorSystemTime;

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueWaitingItems;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueWaitingBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueInFlightItems;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueInFlightBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueInFlightCost;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueWindowSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueItemsPut;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueItemsPutBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueItemsProcessed;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueItemsRejected;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueItemsPruned;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueItemsSent;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueItemsUndelivered;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueItemsIncorrectMsgId;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueItemsWatermarkOverflow;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueOverflow;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueSerializedItems;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueSerializedBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueDeserializedItems;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueDeserializedBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueSize;

public:
    TBlobStorageQueue(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, TString& logPrefix,
            const TBSProxyContextPtr& bspctx, const NBackpressure::TQueueClientId& clientId, ui32 interconnectChannel,
            const TBlobStorageGroupType &gType,
            NMonitoring::TCountableBase::EVisibility visibility = NMonitoring::TCountableBase::EVisibility::Public,
            bool useActorSystemTime = false);

    ~TBlobStorageQueue();

    const NBackpressure::TQueueClientId& GetClientId() const {
        return ClientId;
    }

    void SetMessageId(const NBackpressure::TMessageId& msgId) {
        Y_ABORT_UNLESS(!InFlightCount());
        NextMsgId = msgId.MsgId;
        CurrentSequenceId = msgId.SequenceId;
    }

    ui64 GetBytesWaiting() const {
        return BytesWaiting;
    }

    ui64 GetItemsWaiting() const {
        return Queues.Waiting.size();
    }

    ui64 InFlightCount() {
        return Queues.InFlight.size();
    }

    ui64 GetInFlightCost() const {
        return InFlightCost;
    }

    void UpdateCostModel(TInstant now, const NKikimrBlobStorage::TVDiskCostSettings& settings,
            const TBlobStorageGroupType& type);
    void InvalidateCosts();
    bool SetMaxWindowSize(ui64 maxWindowSize);
    std::shared_ptr<const TCostModel> GetCostModel() const;

    void SetItemQueue(TItem& item, EItemQueue newQueue);

    void SendToVDisk(const TActorContext& ctx, const TActorId& remoteVDisk, ui32 vdiskOrderNumber);

    void ReplyWithError(TItem& item, NKikimrProto::EReplyStatus status, const TString& errorReason, const TActorContext& ctx);
    bool Expecting(ui64 msgId, ui64 sequenceId) const;
    bool OnResponse(ui64 msgId, ui64 sequenceId, ui64 cookie, TActorId *outSender, ui64 *outCookie, TDuration *processingTime);

    void Unwind(ui64 failedMsgId, ui64 failedSequenceId, ui64 expectedMsgId, ui64 expectedSequenceId);

    void DrainQueue(NKikimrProto::EReplyStatus status, const TString& errorReason, const TActorContext &ctx);

    void OnConnect();

    template<typename TPtr>
    void Enqueue(const TActorContext &ctx, TPtr& event, TInstant deadline, bool local) {
        Y_UNUSED(ctx);

        TItemList::iterator newIt;
        if (Queues.Unused.empty()) {
            newIt = Queues.Waiting.emplace(Queues.Waiting.end(), event, deadline,
                QueueSerializedItems, QueueSerializedBytes, BSProxyCtx, InterconnectChannel, local,
                UseActorSystemTime);
            ++*QueueSize;
        } else {
            newIt = Queues.Unused.begin();
            Queues.Waiting.splice(Queues.Waiting.end(), Queues.Unused, newIt);
            // reuse list item
            TItem& item = *newIt;
            item.~TItem();
            new(&item) TItem(event, deadline, QueueSerializedItems, QueueSerializedBytes, BSProxyCtx,
                InterconnectChannel, local, UseActorSystemTime);
        }

        newIt->Iterator = newIt;
        SetItemQueue(*newIt, EItemQueue::Waiting);
        SenderToItems.Insert(&*newIt);

        // count item
        ++*QueueItemsPut;
        *QueueItemsPutBytes += newIt->GetByteSize();
    }

    TItemList::iterator EraseItem(TItemList& queue, TItemList::iterator it);
    TMaybe<TDuration> GetWorstRequestProcessingTime() const;
};

} // NKikimr::NBsQueue
