#include "partition.h"

#include <ydb/core/persqueue/pqtablet/blob/blob_offset.h>

#include <algorithm>
#include <ranges>
#include <unordered_set>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ {

namespace {

// DataKeysBody / HeadKeys are in write order; write timestamps are non-decreasing
// (same invariant as GetOffsetEstimate). lower_bound finds the first key whose
// blob-end timestamp is >= timestamp; the previous key is also a candidate because
// that blob may still contain a matching message.
void AppendCandidateKeys(const std::deque<TDataKey>& keys, TInstant timestamp, TVector<const TDataKey*>& out) {
    if (keys.empty()) {
        return;
    }
    auto it = std::ranges::lower_bound(keys, timestamp, {}, &TDataKey::Timestamp);
    if (it != keys.begin()) {
        out.push_back(&*std::prev(it));
    }
    if (it != keys.end()) {
        out.push_back(&*it);
    }
}

bool BlobContainsSingleMessage(const TKey& key) {
    return key.GetCount() == 1;
}

} // namespace

Ydb::StatusIds::StatusCode TPartition::PqErrorToYdbStatus(NPersQueue::NErrorCode::EErrorCode errorCode) {
    switch (errorCode) {
        case NPersQueue::NErrorCode::OK:
            return Ydb::StatusIds::SUCCESS;
        case NPersQueue::NErrorCode::BAD_REQUEST:
        case NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_PAST:
        case NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE:
            return Ydb::StatusIds::BAD_REQUEST;
        case NPersQueue::NErrorCode::OVERLOAD:
            return Ydb::StatusIds::OVERLOADED;
        case NPersQueue::NErrorCode::ACCESS_DENIED:
            return Ydb::StatusIds::UNAUTHORIZED;
        default:
            return Ydb::StatusIds::GENERIC_ERROR;
    }
}

TInstant TPartition::ResetOffsetTimestamp(const NKikimrPQ::TEvResetOffsetRequest& rec) const {
    TInstant timestamp = TInstant::MilliSeconds(rec.GetTimestampMs());
    if (AppData()->FeatureFlags.GetEnableSkipMessagesWithObsoleteTimestamp()) {
        timestamp = TInstant::Seconds(timestamp.Seconds());
    }
    return timestamp;
}

ui64 TPartition::ResolveResetOffset(const NKikimrPQ::TEvResetOffsetRequest& rec) const {
    switch (rec.GetPosition()) {
        case NKikimrPQ::TEvResetOffsetRequest::EARLIEST:
            return GetStartOffset();
        case NKikimrPQ::TEvResetOffsetRequest::LATEST:
            return GetEndOffset();
        default:
            return GetEndOffset();
    }
}

bool TPartition::TryScheduleResetOffsetReply(const TEvPQ::TEvSetClientInfo& act, Ydb::StatusIds::StatusCode status, const TString& error) {
    if (!act.ResetOffsetReply) {
        return false;
    }
    const auto& pending = *act.ResetOffsetReply;
    Replies.emplace_back(pending.Sender, MakeHolder<TEvPQ::TEvResetOffsetResponse>(
        pending.PartitionId, status, TString(error), pending.Cookie).Release());
    return true;
}

void TPartition::ReplyResetOffset(
    const TActorId& sender,
    ui32 partitionId,
    Ydb::StatusIds::StatusCode status,
    TString message,
    ui64 cookie)
{
    Send(sender, new TEvPQ::TEvResetOffsetResponse(partitionId, status, std::move(message), cookie), 0, cookie);
}

void TPartition::FinishResetOffset(
    const TActorId& sender,
    ui64 cookie,
    ui32 partitionId,
    const TString& consumer,
    ui64 offset)
{
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(
        /*cookie=*/0,
        consumer,
        offset,
        /*sessionId=*/"",
        /*partitionSessionId=*/0,
        /*generation=*/0,
        /*step=*/0,
        TActorId{},
        TEvPQ::TEvSetClientInfo::ESCI_OFFSET);
    event->AllowInactiveRewind = true;
    event->ResetOffsetReply = TEvPQ::TEvSetClientInfo::TResetOffsetReply{
        .Sender = sender,
        .Cookie = cookie,
        .PartitionId = partitionId,
    };
    AddUserAct(event.Release());
    ProcessTxsAndUserActs(ActorContext());
}

TMaybe<ui64> TPartition::ScanHeadForResetOffset(const THead& head, TInstant timestamp) const {
    for (const auto& batch : head.GetBatches()) {
        TVector<TClientBlob> blobs;
        batch.UnpackTo(&blobs);
        if (auto found = FindFirstOffsetAtOrAfterTimestamp(timestamp, batch.GetOffset(), blobs)) {
            return found;
        }
    }
    return Nothing();
}

TMaybe<ui64> TPartition::ScanRequestedBlobsForResetOffset(
    const TVector<TRequestedBlob>& blobs,
    ui32 begin,
    ui32 end,
    TInstant timestamp) const
{
    for (ui32 i = begin; i < end && i < blobs.size(); ++i) {
        const auto& blob = blobs[i];
        if (blob.Empty()) {
            continue;
        }
        auto batches = blob.GetBatches();
        if (!batches) {
            continue;
        }
        if (auto found = FindFirstOffsetAtOrAfterTimestamp(timestamp, blob.Key.GetOffset(), *batches)) {
            return found;
        }
    }
    return Nothing();
}

TMaybe<ui64> TPartition::ResolveResetOffsetFromWrittenAt(
    const TVector<TResetOffsetBlobRead::TKeyRef>& compactionRefs,
    const TVector<TResetOffsetBlobRead::TKeyRef>& fastWriteRefs,
    const TVector<TRequestedBlob>* blobs,
    TInstant timestamp) const
{
    auto scanRefs = [&](const TVector<TResetOffsetBlobRead::TKeyRef>& refs) -> TMaybe<ui64> {
        for (const auto& ref : refs) {
            if (!ref.RequestedIndex.Defined()) {
                if (ref.Timestamp >= timestamp) {
                    return ref.Offset;
                }
                continue;
            }
            if (!blobs) {
                continue;
            }
            const ui32 index = *ref.RequestedIndex;
            if (auto found = ScanRequestedBlobsForResetOffset(*blobs, index, index + 1, timestamp)) {
                return found;
            }
        }
        return Nothing();
    };

    // Compaction zone holds older offsets than fast-write. Scan it first so the
    // first timestamp match is the earliest qualifying message. Empty compaction
    // sources fall through to fast-write, including in-memory Head.
    if (auto found = scanRefs(compactionRefs)) {
        return found;
    }
    if (auto found = ScanHeadForResetOffset(CompactionBlobEncoder.Head, timestamp)) {
        return found;
    }
    if (auto found = scanRefs(fastWriteRefs)) {
        return found;
    }
    if (auto found = ScanHeadForResetOffset(BlobEncoder.Head, timestamp)) {
        return found;
    }
    return Nothing();
}

void TPartition::RequestResetOffsetBlobs(TEvPQ::TEvResetOffsetRequest::TPtr& ev, TInstant timestamp) {
    const auto& rec = ev->Get()->Record;
    TVector<const TDataKey*> compactionKeys;
    TVector<const TDataKey*> fastWriteKeys;
    AppendCandidateKeys(CompactionBlobEncoder.DataKeysBody, timestamp, compactionKeys);
    AppendCandidateKeys(CompactionBlobEncoder.HeadKeys, timestamp, compactionKeys);
    AppendCandidateKeys(BlobEncoder.DataKeysBody, timestamp, fastWriteKeys);
    AppendCandidateKeys(BlobEncoder.HeadKeys, timestamp, fastWriteKeys);

    std::unordered_set<ui64> seenOffsets;
    TVector<TRequestedBlob> blobs;
    TBlobKeyTokens tokens;
    TVector<TResetOffsetBlobRead::TKeyRef> compactionRefs;
    TVector<TResetOffsetBlobRead::TKeyRef> fastWriteRefs;

    auto collect = [&](const TVector<const TDataKey*>& keys, TVector<TResetOffsetBlobRead::TKeyRef>& refs) {
        for (const TDataKey* key : keys) {
            if (!seenOffsets.insert(key->Key.GetOffset()).second) {
                continue;
            }
            TResetOffsetBlobRead::TKeyRef ref;
            ref.Offset = key->Key.GetOffset();
            ref.Timestamp = key->Timestamp;
            // Count == 1: the blob is a single message. Offset is Key.GetOffset(),
            // the next blob's offset is already known — do not read the value.
            if (!BlobContainsSingleMessage(key->Key)) {
                ref.RequestedIndex = blobs.size();
                blobs.push_back(TRequestedBlob(
                    key->Key.GetOffset(),
                    key->Key.GetPartNo(),
                    key->Key.GetCount(),
                    key->Key.GetInternalPartsCount(),
                    key->Size,
                    TString(),
                    key->Key,
                    key->Timestamp.Seconds()));
                tokens.Append(key->BlobKeyToken);
            }
            refs.push_back(ref);
        }
    };

    collect(compactionKeys, compactionRefs);
    collect(fastWriteKeys, fastWriteRefs);

    const ui64 replyCookie = rec.HasCookie() ? rec.GetCookie() : ev->Cookie;
    const ui32 partitionId = Partition.OriginalPartitionId;

    if (blobs.empty()) {
        TMaybe<ui64> found = ResolveResetOffsetFromWrittenAt(
            compactionRefs, fastWriteRefs, nullptr, timestamp);
        FinishResetOffset(
            ev->Sender,
            replyCookie,
            partitionId,
            rec.GetConsumer(),
            found.GetOrElse(GetEndOffset()));
        return;
    }

    ResetOffsetBlobRead = TResetOffsetBlobRead{
        .Sender = ev->Sender,
        .Cookie = replyCookie,
        .PartitionId = partitionId,
        .Consumer = rec.GetConsumer(),
        .Timestamp = timestamp,
        .CompactionKeys = std::move(compactionRefs),
        .FastWriteKeys = std::move(fastWriteRefs),
        .BlobKeyTokens = std::move(tokens),
    };

    YDB_LOG_DEBUG("Request blobs for ResetOffset FROM_WRITTEN_AT",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"timestampMs", timestamp.MilliSeconds()},
        {"blobCount", blobs.size()});

    auto request = MakeHolder<TEvPQ::TEvBlobRequest>(
        ERequestCookie::ReadBlobForResetOffset, Partition, std::move(blobs));
    Send(BlobCache, request.Release());
}

void TPartition::HandleResetOffsetBlobResponse(TEvPQ::TEvBlobResponse::TPtr& ev) {
    if (!ResetOffsetBlobRead) {
        return;
    }
    auto pending = std::move(*ResetOffsetBlobRead);
    ResetOffsetBlobRead.reset();

    const auto* response = ev->Get();
    if (HasError(*response)) {
        ReplyResetOffset(
            pending.Sender,
            pending.PartitionId,
            Ydb::StatusIds::GENERIC_ERROR,
            TStringBuilder() << "blob read failed: " << response->Error.ErrorStr,
            pending.Cookie);
        ProcessResetOffsetPendingEvents();
        return;
    }

    TMaybe<ui64> found = ResolveResetOffsetFromWrittenAt(
        pending.CompactionKeys,
        pending.FastWriteKeys,
        &response->GetBlobs(),
        pending.Timestamp);

    FinishResetOffset(
        pending.Sender,
        pending.Cookie,
        pending.PartitionId,
        pending.Consumer,
        found.GetOrElse(GetEndOffset()));
    ProcessResetOffsetPendingEvents();
}

void TPartition::BeginResetOffset(TEvPQ::TEvResetOffsetRequest::TPtr& ev) {
    const auto& rec = ev->Get()->Record;
    const ui32 partitionId = Partition.OriginalPartitionId;
    const ui64 replyCookie = rec.HasCookie() ? rec.GetCookie() : ev->Cookie;

    if (rec.GetPosition() == NKikimrPQ::TEvResetOffsetRequest::POSITION_UNSPECIFIED) {
        ReplyResetOffset(ev->Sender, partitionId, Ydb::StatusIds::BAD_REQUEST, "Position is required", replyCookie);
        return;
    }

    if (size_t count = GetUserActCount(rec.GetConsumer()); count > MAX_USER_ACTS) {
        ReplyResetOffset(
            ev->Sender,
            partitionId,
            Ydb::StatusIds::OVERLOADED,
            TStringBuilder() << "too big inflight: " << count,
            replyCookie);
        return;
    }

    if (rec.GetPosition() == NKikimrPQ::TEvResetOffsetRequest::FROM_WRITTEN_AT) {
        RequestResetOffsetBlobs(ev, ResetOffsetTimestamp(rec));
        return;
    }

    FinishResetOffset(ev->Sender, replyCookie, partitionId, rec.GetConsumer(), ResolveResetOffset(rec));
}

void TPartition::HandleOnInit(TEvPQ::TEvResetOffsetRequest::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnInit TEvPQ::TEvResetOffsetRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record.ShortDebugString()});
    ResetOffsetPendingEvents.emplace_back(std::move(ev));
}

void TPartition::Handle(TEvPQ::TEvResetOffsetRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvResetOffsetRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record.ShortDebugString()});

    if (ResetOffsetBlobRead) {
        ResetOffsetPendingEvents.emplace_back(std::move(ev));
        return;
    }
    BeginResetOffset(ev);
}

void TPartition::ProcessResetOffsetPendingEvents() {
    YDB_LOG_DEBUG("Process ResetOffset pending events. Count",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"count", ResetOffsetPendingEvents.size()});

    while (!ResetOffsetPendingEvents.empty() && !ResetOffsetBlobRead) {
        auto ev = std::move(ResetOffsetPendingEvents.front());
        ResetOffsetPendingEvents.pop_front();
        BeginResetOffset(ev);
    }
}

} // namespace NKikimr::NPQ
