#include "queue.h"

namespace NKikimr::NBsQueue {

TBlobStorageQueue::TBlobStorageQueue(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, TString& logPrefix,
        const TBSProxyContextPtr& bspctx, const NBackpressure::TQueueClientId& clientId, ui32 interconnectChannel,
        const TBlobStorageGroupType& gType, NMonitoring::TCountableBase::EVisibility visibility, bool useActorSystemTime)
    : Queues(bspctx)
    , WindowSize(0)
    , InFlightCost(0)
    , NextMsgId(0)
    , CurrentSequenceId(1)
    , LogPrefix(logPrefix)
    , CostModel(std::make_shared<const TCostModel>(2000, 100000000, 50000000, 540000, 540000, 500000, gType)) // default cost model
    , BSProxyCtx(bspctx)
    , ClientId(clientId)
    , BytesWaiting(0)
    , InterconnectChannel(interconnectChannel)
    , UseActorSystemTime(useActorSystemTime)
    // use parent group visibility
    , QueueWaitingItems(counters->GetCounter("QueueWaitingItems", false, visibility))
    , QueueWaitingBytes(counters->GetCounter("QueueWaitingBytes", false, visibility))
    , QueueInFlightItems(counters->GetCounter("QueueInFlightItems", false, visibility))
    , QueueInFlightBytes(counters->GetCounter("QueueInFlightBytes", false, visibility))
    , QueueInFlightCost(counters->GetCounter("QueueInFlightCost", false, visibility))
    , QueueWindowSize(counters->GetCounter("QueueWindowSize", false, visibility))
    , QueueItemsPut(counters->GetCounter("QueueItemsPut", true, visibility))
    , QueueItemsPutBytes(counters->GetCounter("QueueItemsPutBytes", true, visibility))
    , QueueItemsProcessed(counters->GetCounter("QueueItemsProcessed", true, visibility))
    , QueueItemsRejected(counters->GetCounter("QueueItemsRejected", true, visibility))
    , QueueItemsPruned(counters->GetCounter("QueueItemsPruned", true, visibility))
    , QueueItemsSent(counters->GetCounter("QueueItemsSent", true, visibility))
    , QueueItemsUndelivered(counters->GetCounter("QueueItemsUndelivered", true, visibility))
    , QueueItemsIncorrectMsgId(counters->GetCounter("QueueItemsIncorrectMsgId", true, visibility))
    , QueueItemsWatermarkOverflow(counters->GetCounter("QueueItemsWatermarkOverflow", true, visibility))
    , QueueOverflow(counters->GetCounter("QueueOverflow", true, visibility))
    , QueueSerializedItems(counters->GetCounter("QueueSerializedItems", true, visibility))
    , QueueSerializedBytes(counters->GetCounter("QueueSerializedBytes", true, visibility))
    , QueueDeserializedItems(counters->GetCounter("QueueDeserializedItems", true, visibility))
    , QueueDeserializedBytes(counters->GetCounter("QueueDeserializedBytes", true, visibility))
    , QueueSize(counters->GetCounter("QueueSize", false, visibility))
{}

TBlobStorageQueue::~TBlobStorageQueue() {
    SetMaxWindowSize(0);
    for (TItemList *queue : {&Queues.Waiting, &Queues.InFlight, &Queues.Unused}) {
        for (TItem& item : *queue) {
            SetItemQueue(item, EItemQueue::NotSet);
        }
    }
}

void TBlobStorageQueue::UpdateCostModel(TInstant now, const NKikimrBlobStorage::TVDiskCostSettings& settings,
        const TBlobStorageGroupType& type) {
    TCostModel newCostModel(settings, type);
    if (newCostModel != *CostModel) {
        CostModel = std::make_shared<const TCostModel>(std::move(newCostModel));
        InvalidateCosts();
    }
    CostSettingsUpdate = now + TDuration::Minutes(1);
}

void TBlobStorageQueue::InvalidateCosts() {
    for (TItem& item : Queues.Waiting) {
        item.DirtyCost = true;
    }
    for (TItem& item : Queues.InFlight) {
        item.DirtyCost = true;
    }
}

bool TBlobStorageQueue::SetMaxWindowSize(ui64 maxWindowSize) {
    if (WindowSize != maxWindowSize) {
        *QueueWindowSize += (i64)(maxWindowSize - WindowSize);
        WindowSize = maxWindowSize;
        return true;
    } else {
        return false;
    }
}

std::shared_ptr<const TCostModel> TBlobStorageQueue::GetCostModel() const {
    return CostModel;
}

void TBlobStorageQueue::SetItemQueue(TItem& item, EItemQueue newQueue) {
    switch (item.Queue) {
        case EItemQueue::NotSet:
            break;

        case EItemQueue::Waiting:
            --*QueueWaitingItems;
            *QueueWaitingBytes -= item.GetByteSize();
            BytesWaiting -= item.GetByteSize();
            break;

        case EItemQueue::InFlight:
            --*QueueInFlightItems;
            *QueueInFlightBytes -= item.GetByteSize();
            *QueueInFlightCost -= item.Cost;
            break;
    }

    item.Queue = newQueue;
    switch (newQueue) {
        case EItemQueue::NotSet:
            break;

        case EItemQueue::Waiting:
            ++*QueueWaitingItems;
            *QueueWaitingBytes += item.GetByteSize();
            BytesWaiting += item.GetByteSize();
            break;

        case EItemQueue::InFlight:
            ++*QueueInFlightItems;
            *QueueInFlightBytes += item.GetByteSize();
            *QueueInFlightCost += item.Cost;
            break;
    }
}

void TBlobStorageQueue::SendToVDisk(const TActorContext& ctx, const TActorId& remoteVDisk, ui32 vdiskOrderNumber) {
    const TInstant now = ctx.Now();

    const bool sendMeCostSettings = now >= CostSettingsUpdate;

    for (auto it = Queues.Waiting.begin(); !Paused && it != Queues.Waiting.end(); ) {
        Y_ABORT_UNLESS(it == Queues.Waiting.begin());
        TItem& item = *it;

        // check if deadline occured
        if (now >= item.Deadline) {
            ReplyWithError(item, NKikimrProto::DEADLINE, "deadline exceeded", ctx);
            it = EraseItem(Queues.Waiting, it);
            continue;
        }

        // update item parameters
        item.MsgId = NextMsgId;
        item.SequenceId = CurrentSequenceId;

        // update item's cost if it is dirty
        if (item.DirtyCost) {
            item.Cost = CostModel->CalculateCost(item.CostEssence);
            item.DirtyCost = false;
        }

        const bool postpone = InFlightCost + item.Cost > WindowSize && InFlightCount();

        auto getTypeName = [&]() -> TString {
            switch (item.Event.GetType()) {
#define TYPE_CASE(X) case X::EventType: return #X;
                TYPE_CASE(TEvBlobStorage::TEvVMovedPatch)
                TYPE_CASE(TEvBlobStorage::TEvVPut)
                TYPE_CASE(TEvBlobStorage::TEvVMultiPut)
                TYPE_CASE(TEvBlobStorage::TEvVGet)
                TYPE_CASE(TEvBlobStorage::TEvVBlock)
                TYPE_CASE(TEvBlobStorage::TEvVGetBlock)
                TYPE_CASE(TEvBlobStorage::TEvVCollectGarbage)
                TYPE_CASE(TEvBlobStorage::TEvVGetBarrier)
                TYPE_CASE(TEvBlobStorage::TEvVStatus)
                TYPE_CASE(TEvBlobStorage::TEvVAssimilate)
#undef TYPE_CASE
                default:
                    return Sprintf("0x%08" PRIx32, item.Event.GetType());
            }
        };

        QLOG_DEBUG_S("BSQ25", "sending"
                << " T# " << getTypeName()
                << " SequenceId# " << item.SequenceId
                << " MsgId# " << item.MsgId
                << " Cookie# " << item.QueueCookie
                << " InFlightCost# " << InFlightCost
                << " Cost# " << item.Cost
                << " WindowSize# " << WindowSize
                << " InFlightCount# " << InFlightCount()
                << " Postpone# " << (postpone ? "true" : "false")
                << " SendMeCostSettings# " << (sendMeCostSettings ? "true" : "false"));

        // check if window has enough space for such item
        if (postpone) {
            // can't send more items now
            QLOG_DEBUG_S("BSQ26", "Queue overflow: InFlightCost# " << InFlightCost << " WindowSize# "
                << WindowSize << " item.Cost# " << item.Cost << " InFlightCount# " << InFlightCount());
            ++*QueueOverflow;
            break;
        }

        if (!item.Event.Relevant()) {
            ++*QueueItemsPruned;
            it = EraseItem(Queues.Waiting, it);
            continue;
        }

        InFlightCost += item.Cost;

        // move item to in-flight queue
        SetItemQueue(item, EItemQueue::InFlight);
        const bool inserted = InFlightLookup.emplace(std::make_pair(item.SequenceId, item.MsgId), it).second;
        Y_ABORT_UNLESS(inserted);
        Queues.InFlight.splice(Queues.InFlight.end(), Queues.Waiting, it++);
        ++*QueueItemsSent;

        // send item
        item.Span && item.Span.Event("SendToVDisk", {
            {"VDiskOrderNumber", vdiskOrderNumber}
        });
        item.Event.SendToVDisk(ctx, remoteVDisk, item.QueueCookie, item.MsgId, item.SequenceId, sendMeCostSettings,
            item.Span.GetTraceId(), ClientId, item.ProcessingTimer);

        // update counters as far as item got sent
        ++NextMsgId;
    }
}

void TBlobStorageQueue::ReplyWithError(TItem& item, NKikimrProto::EReplyStatus status, const TString& errorReason,
        const TActorContext& ctx) {
    const TDuration processingTime = TDuration::Seconds(item.ProcessingTimer.Passed());
    QLOG_INFO_S("BSQ03", "Reply error type# " << item.Event.GetType()
        << " status# " << NKikimrProto::EReplyStatus_Name(status)
        << " errorReason# " << '"' << EscapeC(errorReason) << '"'
        << " cookie# " << item.Event.GetCookie()
        << " processingTime# " << processingTime);

    item.Span.EndError(TStringBuilder() << NKikimrProto::EReplyStatus_Name(status) << ": " << errorReason);
    item.Span = {};

    ctx.Send(item.Event.GetSender(), item.Event.MakeErrorReply(status, errorReason, QueueDeserializedItems,
            QueueDeserializedBytes), 0, item.Event.GetCookie());

    ++*QueueItemsRejected;
}

bool TBlobStorageQueue::Expecting(ui64 msgId, ui64 sequenceId) const {
    return InFlightLookup.count(std::make_pair(sequenceId, msgId));
}

bool TBlobStorageQueue::OnResponse(ui64 msgId, ui64 sequenceId, ui64 cookie, TActorId *outSender, ui64 *outCookie,
        TDuration *processingTime) {
    const auto lookupIt = InFlightLookup.find(std::make_pair(sequenceId, msgId));
    Y_ABORT_UNLESS(lookupIt != InFlightLookup.end());
    const TItemList::iterator it = lookupIt->second;

    Y_ABORT_UNLESS(cookie == it->QueueCookie || cookie == 0);

    Y_ABORT_UNLESS(InFlightCost >= it->Cost);
    InFlightCost -= it->Cost;

    const bool relevant = it->Event.Relevant();

    *outSender = it->Event.GetSender();
    *outCookie = it->Event.GetCookie();
    *processingTime = TDuration::Seconds(it->ProcessingTimer.Passed());
    LWTRACK(DSQueueVPutResultRecieved, it->Event.GetOrbit(), processingTime->SecondsFloat() * 1e3,
            it->Event.GetByteSize(), !relevant);

    InFlightLookup.erase(lookupIt);
    auto span = std::exchange(it->Span, {});
    span.EndOk();
    EraseItem(Queues.InFlight, it);

    // unpause execution when InFlight queue gets empty
    if (!InFlightLookup) {
        Paused = false;
    }

    ++*QueueItemsProcessed;
    return relevant;
}

void TBlobStorageQueue::Unwind(ui64 failedMsgId, ui64 failedSequenceId, ui64 expectedMsgId, ui64 expectedSequenceId) {
    // find item in the InFlight queue; it MUST exist in that queue
    const auto lookupIt = InFlightLookup.find(std::make_pair(failedSequenceId, failedMsgId));
    Y_ABORT_UNLESS(lookupIt != InFlightLookup.end());
    TItemList::iterator it = lookupIt->second;

    // process items
    ui64 cost = 0;
    for (auto x = it; x != Queues.InFlight.end(); ) {
        const ui32 erased = InFlightLookup.erase(std::make_pair(x->SequenceId, x->MsgId));
        Y_ABORT_UNLESS(erased);
        cost += x->Cost; // count item's cost
        if (!x->Event.Relevant()) {
            if (x == it) {
                ++it; // advance starting iterator as the item pointed to is being erased
            }
            x = EraseItem(Queues.InFlight, x);
        } else {
            SetItemQueue(*x, EItemQueue::Waiting);
            ++x;
        }
    }
    Y_ABORT_UNLESS(cost <= InFlightCost);
    InFlightCost -= cost;

    // splice items into waiting queue's front
    Queues.Waiting.splice(Queues.Waiting.begin(), Queues.InFlight, it, Queues.InFlight.end());

    // adjust correct sequence ids
    NextMsgId = expectedMsgId;
    CurrentSequenceId = expectedSequenceId;

    // pause execution if we have something unanswered
    Paused = static_cast<bool>(InFlightLookup);
}

void TBlobStorageQueue::DrainQueue(NKikimrProto::EReplyStatus status, const TString& errorReason, const TActorContext &ctx) {
    // remove all items from in-flight map
    InFlightCost = 0;

    // ACHTUNG: We may expect that if we drop the messages from the queue,
    //          then we can safely restart from that point

    auto flushQueue = [&](TItemList& queue) {
        for (auto it = queue.begin(); it != queue.end(); it = EraseItem(queue, it)) {
            if (it->Event.Relevant()) {
                ReplyWithError(*it, status, errorReason, ctx);
            }
        }
    };

    flushQueue(Queues.InFlight);
    flushQueue(Queues.Waiting);
    InFlightLookup.clear();

    Paused = false;
}

void TBlobStorageQueue::OnConnect() {
    SetMaxWindowSize(1000000000); // default value is one second
    CostSettingsUpdate = TInstant::Zero(); // request cost model update in first message
}

TBlobStorageQueue::TItemList::iterator TBlobStorageQueue::EraseItem(TItemList& queue, TItemList::iterator it) {
    SetItemQueue(*it, EItemQueue::NotSet);
    it->Span.EndError("EraseItem called");
    TItemList::iterator nextIter = std::next(it);
    if (Queues.Unused.size() < MaxUnusedItems) {
        Queues.Unused.splice(Queues.Unused.end(), queue, it);
        it->TSenderNode::UnLink();
        it->Event.Discard();
    } else {
        queue.erase(it);
        --*QueueSize;
    }
    return nextIter;
}

TMaybe<TDuration> TBlobStorageQueue::GetWorstRequestProcessingTime() const {
    if (Queues.InFlight.size()) {
        return TDuration::Seconds(Queues.InFlight.front().ProcessingTimer.Passed());
    } else if (Queues.Waiting.size()) {
        return TDuration::Seconds(Queues.Waiting.front().ProcessingTimer.Passed());
    } else {
        return {};
    }
}

} // NKikimr::NBsQueue
