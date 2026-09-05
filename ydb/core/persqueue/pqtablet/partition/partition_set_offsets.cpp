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

TInstant TPartition::SetOffsetsTimestamp(const NKikimrPQ::TEvSetOffsetsRequest& rec) const {
    TInstant timestamp = TInstant::MilliSeconds(rec.GetTimestampMs());
    if (AppData()->FeatureFlags.GetEnableSkipMessagesWithObsoleteTimestamp()) {
        timestamp = TInstant::Seconds(timestamp.Seconds());
    }
    return timestamp;
}

ui64 TPartition::ResolveSetOffsets(const NKikimrPQ::TEvSetOffsetsRequest& rec) const {
    switch (rec.GetPosition()) {
        case NKikimrPQ::TEvSetOffsetsRequest::EARLIEST:
            return GetStartOffset();
        case NKikimrPQ::TEvSetOffsetsRequest::LATEST:
            return GetEndOffset();
        default:
            return GetEndOffset();
    }
}

bool TPartition::TryScheduleSetOffsetsReply(const TEvPQ::TEvSetClientInfo& act, Ydb::StatusIds::StatusCode status, const TString& error) {
    if (!act.SetOffsetsReply) {
        return false;
    }
    const auto& pending = *act.SetOffsetsReply;
    Replies.emplace_back(pending.Sender, MakeHolder<TEvPQ::TEvSetOffsetsResponse>(
        pending.PartitionId, status, TString(error), pending.Cookie).Release());
    return true;
}

void TPartition::ReplySetOffsets(
    const TActorId& sender,
    ui32 partitionId,
    Ydb::StatusIds::StatusCode status,
    TString message,
    ui64 cookie)
{
    Send(sender, new TEvPQ::TEvSetOffsetsResponse(partitionId, status, std::move(message), cookie), 0, cookie);
}

void TPartition::FinishSetOffsets(
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
    event->SetOffsetsReply = TEvPQ::TEvSetClientInfo::TSetOffsetsReply{
        .Sender = sender,
        .Cookie = cookie,
        .PartitionId = partitionId,
    };
    AddUserAct(event.Release());
    ProcessTxsAndUserActs(ActorContext());
}

TMaybe<ui64> TPartition::ScanHeadForSetOffsets(const THead& head, TInstant timestamp) const {
    for (const auto& batch : head.GetBatches()) {
        TVector<TClientBlob> blobs;
        batch.UnpackTo(&blobs);
        if (auto found = FindFirstOffsetAtOrAfterTimestamp(timestamp, batch.GetOffset(), blobs)) {
            return found;
        }
    }
    return Nothing();
}

TMaybe<ui64> TPartition::ScanRequestedBlobsForSetOffsets(
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

TMaybe<ui64> TPartition::ResolveSetOffsetsFromWrittenAt(
    const TVector<TSetOffsetsBlobRead::TKeyRef>& compactionRefs,
    const TVector<TSetOffsetsBlobRead::TKeyRef>& fastWriteRefs,
    const TVector<TRequestedBlob>* blobs,
    TInstant timestamp) const
{
    auto scanRefs = [&](const TVector<TSetOffsetsBlobRead::TKeyRef>& refs) -> TMaybe<ui64> {
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
            if (auto found = ScanRequestedBlobsForSetOffsets(*blobs, index, index + 1, timestamp)) {
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
    if (auto found = ScanHeadForSetOffsets(CompactionBlobEncoder.Head, timestamp)) {
        return found;
    }
    if (auto found = scanRefs(fastWriteRefs)) {
        return found;
    }
    if (auto found = ScanHeadForSetOffsets(BlobEncoder.Head, timestamp)) {
        return found;
    }
    return Nothing();
}

void TPartition::RequestSetOffsetsBlobs(TEvPQ::TEvSetOffsetsRequest::TPtr& ev, TInstant timestamp) {
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
    TVector<TSetOffsetsBlobRead::TKeyRef> compactionRefs;
    TVector<TSetOffsetsBlobRead::TKeyRef> fastWriteRefs;

    auto collect = [&](const TVector<const TDataKey*>& keys, TVector<TSetOffsetsBlobRead::TKeyRef>& refs) {
        for (const TDataKey* key : keys) {
            if (!seenOffsets.insert(key->Key.GetOffset()).second) {
                continue;
            }
            TSetOffsetsBlobRead::TKeyRef ref;
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
        TMaybe<ui64> found = ResolveSetOffsetsFromWrittenAt(
            compactionRefs, fastWriteRefs, nullptr, timestamp);
        FinishSetOffsets(
            ev->Sender,
            replyCookie,
            partitionId,
            rec.GetConsumer(),
            found.GetOrElse(GetEndOffset()));
        return;
    }

    SetOffsetsBlobRead = TSetOffsetsBlobRead{
        .Sender = ev->Sender,
        .Cookie = replyCookie,
        .PartitionId = partitionId,
        .Consumer = rec.GetConsumer(),
        .Timestamp = timestamp,
        .CompactionKeys = std::move(compactionRefs),
        .FastWriteKeys = std::move(fastWriteRefs),
        .BlobKeyTokens = std::move(tokens),
    };

    YDB_LOG_DEBUG("Request blobs for SetOffsets FROM_WRITTEN_AT",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"timestampMs", timestamp.MilliSeconds()},
        {"blobCount", blobs.size()});

    auto request = MakeHolder<TEvPQ::TEvBlobRequest>(
        ERequestCookie::ReadBlobForSetOffsets, Partition, std::move(blobs));
    Send(BlobCache, request.Release());
}

void TPartition::HandleSetOffsetsBlobResponse(TEvPQ::TEvBlobResponse::TPtr& ev) {
    if (!SetOffsetsBlobRead) {
        return;
    }
    auto pending = std::move(*SetOffsetsBlobRead);
    SetOffsetsBlobRead.reset();

    const auto* response = ev->Get();
    if (HasError(*response)) {
        ReplySetOffsets(
            pending.Sender,
            pending.PartitionId,
            Ydb::StatusIds::GENERIC_ERROR,
            TStringBuilder() << "blob read failed: " << response->Error.ErrorStr,
            pending.Cookie);
        ProcessSetOffsetsPendingEvents();
        return;
    }

    TMaybe<ui64> found = ResolveSetOffsetsFromWrittenAt(
        pending.CompactionKeys,
        pending.FastWriteKeys,
        &response->GetBlobs(),
        pending.Timestamp);

    FinishSetOffsets(
        pending.Sender,
        pending.Cookie,
        pending.PartitionId,
        pending.Consumer,
        found.GetOrElse(GetEndOffset()));
    ProcessSetOffsetsPendingEvents();
}

void TPartition::BeginSetOffsets(TEvPQ::TEvSetOffsetsRequest::TPtr& ev) {
    const auto& rec = ev->Get()->Record;
    const ui32 partitionId = Partition.OriginalPartitionId;
    const ui64 replyCookie = rec.HasCookie() ? rec.GetCookie() : ev->Cookie;

    if (rec.GetPosition() == NKikimrPQ::TEvSetOffsetsRequest::POSITION_UNSPECIFIED) {
        ReplySetOffsets(ev->Sender, partitionId, Ydb::StatusIds::BAD_REQUEST, "Position is required", replyCookie);
        return;
    }

    if (size_t count = GetUserActCount(rec.GetConsumer()); count > MAX_USER_ACTS) {
        ReplySetOffsets(
            ev->Sender,
            partitionId,
            Ydb::StatusIds::OVERLOADED,
            TStringBuilder() << "too big inflight: " << count,
            replyCookie);
        return;
    }

    if (rec.GetPosition() == NKikimrPQ::TEvSetOffsetsRequest::FROM_WRITTEN_AT) {
        RequestSetOffsetsBlobs(ev, SetOffsetsTimestamp(rec));
        return;
    }

    FinishSetOffsets(ev->Sender, replyCookie, partitionId, rec.GetConsumer(), ResolveSetOffsets(rec));
}

void TPartition::HandleOnInit(TEvPQ::TEvSetOffsetsRequest::TPtr& ev) {
    YDB_LOG_DEBUG("HandleOnInit TEvPQ::TEvSetOffsetsRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record.ShortDebugString()});
    SetOffsetsPendingEvents.emplace_back(std::move(ev));
}

void TPartition::Handle(TEvPQ::TEvSetOffsetsRequest::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvPQ::TEvSetOffsetsRequest",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"ev", ev->Get()->Record.ShortDebugString()});

    if (SetOffsetsBlobRead) {
        SetOffsetsPendingEvents.emplace_back(std::move(ev));
        return;
    }
    BeginSetOffsets(ev);
}

void TPartition::ProcessSetOffsetsPendingEvents() {
    YDB_LOG_DEBUG("Process SetOffsets pending events. Count",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"count", SetOffsetsPendingEvents.size()});

    while (!SetOffsetsPendingEvents.empty() && !SetOffsetsBlobRead) {
        auto ev = std::move(SetOffsetsPendingEvents.front());
        SetOffsetsPendingEvents.pop_front();
        BeginSetOffsets(ev);
    }
}

} // namespace NKikimr::NPQ
