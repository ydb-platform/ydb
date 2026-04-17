#pragma once

#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/protos/data_events.pb.h>

namespace NKikimr {
namespace NKqp {

// Helper: look up VictimQuerySpanId from broken locks stored in TxManager and set it.
// Used when STATUS_LOCKS_BROKEN is received and we need to find
// the victim's QuerySpanId from TxManager's stored lock entries.
template <typename TLockCollection>
inline void SetVictimQuerySpanIdFromBrokenLocks(
    ui64 shardId,
    const TLockCollection& locks,
    const IKqpTransactionManagerPtr& txManager)
{
    for (const auto& lock : locks) {
        if (auto victimSpanId = txManager->LookupVictimQuerySpanId(shardId, lock)) {
            txManager->SetVictimQuerySpanId(*victimSpanId);
            return;
        }
    }
}

// Helper: build lock invalidation error message from TxManager issue, with fallback.
inline TString MakeLockInvalidatedMessage(const IKqpTransactionManagerPtr& txManager,
    const TString& tablePath = "")
{
    if (auto lockIssue = txManager->GetLockIssue()) {
        return lockIssue->GetMessage();
    }
    TStringBuilder builder;
    builder << "Transaction locks invalidated.";
    if (!tablePath.empty()) {
        builder << " Table: `" << tablePath << "`.";
    }
    return builder;
}

// Helper: build error issues from TxManager lock issue combined with additional issues.
inline NYql::TIssues MakeLockIssues(const IKqpTransactionManagerPtr& txManager,
    const NYql::TIssues& extraIssues = {})
{
    NYql::TIssues result;
    if (auto lockIssue = txManager->GetLockIssue()) {
        result.AddIssue(*lockIssue);
    }
    for (const auto& issue : extraIssues) {
        result.AddIssue(issue);
    }
    return result;
}

}
}
