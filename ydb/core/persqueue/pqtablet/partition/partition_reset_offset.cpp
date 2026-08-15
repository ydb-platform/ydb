#include "partition.h"

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ {

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

ui64 TPartition::ResolveResetOffset(const NKikimrPQ::TEvResetOffsetRequest& rec) const {
    switch (rec.GetPosition()) {
        case NKikimrPQ::TEvResetOffsetRequest::EARLIEST:
            return GetStartOffset();
        case NKikimrPQ::TEvResetOffsetRequest::LATEST:
            return GetEndOffset();
        case NKikimrPQ::TEvResetOffsetRequest::FROM_WRITTEN_AT: {
            TInstant timestamp = TInstant::MilliSeconds(rec.GetTimestampMs());
            if (AppData()->FeatureFlags.GetEnableSkipMessagesWithObsoleteTimestamp()) {
                timestamp = TInstant::Seconds(timestamp.Seconds());
            }
            TMaybe<ui64> estimatedOffset = GetOffsetEstimate(CompactionBlobEncoder.DataKeysBody, timestamp);
            if (!estimatedOffset.Defined()) {
                estimatedOffset = GetOffsetEstimate(CompactionBlobEncoder.HeadKeys, timestamp);
            }
            if (!estimatedOffset.Defined()) {
                estimatedOffset = GetOffsetEstimate(BlobEncoder.DataKeysBody, timestamp);
            }
            return estimatedOffset.GetOrElse(GetEndOffset());
        }
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

    const auto& rec = ev->Get()->Record;
    const ui32 partitionId = Partition.OriginalPartitionId;
    const ui64 replyCookie = rec.GetCookie() ? rec.GetCookie() : ev->Cookie;

    auto replyNow = [&](Ydb::StatusIds::StatusCode status, TString message) {
        Send(ev->Sender, new TEvPQ::TEvResetOffsetResponse(partitionId, status, std::move(message), replyCookie), 0, replyCookie);
    };

    if (rec.GetPosition() == NKikimrPQ::TEvResetOffsetRequest::POSITION_UNSPECIFIED) {
        replyNow(Ydb::StatusIds::BAD_REQUEST, "Position is required");
        return;
    }

    if (size_t count = GetUserActCount(rec.GetConsumer()); count > MAX_USER_ACTS) {
        replyNow(Ydb::StatusIds::OVERLOADED, TStringBuilder() << "too big inflight: " << count);
        return;
    }

    const ui64 offset = ResolveResetOffset(rec);
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(
        /*cookie=*/0,
        rec.GetConsumer(),
        offset,
        /*sessionId=*/"",
        /*partitionSessionId=*/0,
        /*generation=*/0,
        /*step=*/0,
        TActorId{},
        TEvPQ::TEvSetClientInfo::ESCI_OFFSET);
    event->AllowInactiveRewind = true;
    event->ResetOffsetReply = TEvPQ::TEvSetClientInfo::TResetOffsetReply{
        .Sender = ev->Sender,
        .Cookie = replyCookie,
        .PartitionId = partitionId,
    };
    AddUserAct(event.Release());
    ProcessTxsAndUserActs(ActorContext());
}

void TPartition::ProcessResetOffsetPendingEvents() {
    YDB_LOG_DEBUG("Process ResetOffset pending events. Count",
        {"logPrefix", NPQ_LOG_PREFIX},
        {"count", ResetOffsetPendingEvents.size()});

    while (!ResetOffsetPendingEvents.empty()) {
        auto ev = std::move(ResetOffsetPendingEvents.front());
        ResetOffsetPendingEvents.pop_front();
        Handle(ev);
    }
}

} // namespace NKikimr::NPQ
