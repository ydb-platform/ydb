#include "event_helpers.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT service

namespace NKikimr {
namespace NPQ {

void ReplyPersQueueError(
    TActorId dstActor,
    const TActorContext& ctx,
    ui64 tabletId,
    const TString& topicName,
    TMaybe<TPartitionId> partition,
    NKikimr::TTabletCountersBase& counters,
    NKikimrServices::EServiceKikimr service,
    const ui64 responseCookie,
    NPersQueue::NErrorCode::EErrorCode errorCode,
    const TString& error,
    bool logDebug,
    bool isInternal
) {
    if (errorCode == NPersQueue::NErrorCode::BAD_REQUEST) {
        counters.Cumulative()[COUNTER_PQ_BAD_REQUEST].Increment(1);
    } else if (errorCode == NPersQueue::NErrorCode::INITIALIZING) {
        counters.Cumulative()[COUNTER_PQ_INITIALIZING].Increment(1);
    }

    TStringBuilder logStr;
    logStr << "tablet " << tabletId << " topic '" << topicName;
    if (partition) {
        logStr << "' partition " << partition;
    }
    logStr << " error: " << error;

    if (logDebug) {
        YDB_LOG_CTX_DEBUG(ctx, "",
            {"logStr", logStr});
    } else {
        YDB_LOG_CTX_WARN(ctx, "",
            {"logStr", logStr});
    }
    ctx.Send(dstActor, new TEvPQ::TEvError(errorCode, error, responseCookie, isInternal));
}

}// NPQ
}// NKikimr
