#pragma once

#include <ydb/core/persqueue/common/partition_id.h>
#include <ydb/core/persqueue/public/pqdata_transaction_compat.h>

#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

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
    bool logDebug = false,
    bool isInternal = false
);

inline
bool IsDeferredPublicationTxOperation(const NKikimrPQ::TPartitionOperation& operation)
{
    return IsDeferredPublicationFinalizeOperation(operation);
}

template <class C>
bool AllExistingWritesSkipConflictCheck(const C& ops)
{
    size_t writeOpsCount = 0;
    size_t flagsCount = 0;

    for (const auto& op : ops) {
        NKikimrPQ::TPartitionOperation operation = op;
        EnsureCanonical(operation);
        if (!IsWriteTxOperation(operation)) {
            continue;
        }

        ++writeOpsCount;

        if (GetSkipConflictCheck(operation)) {
            ++flagsCount;
        }
    }

    return (writeOpsCount > 0) && (writeOpsCount == flagsCount);
}

}// NPQ
}// NKikimr
