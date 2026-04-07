#pragma once

#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/protos/query_stats.pb.h>

namespace NKikimr {
namespace NKqp {

// Helper: resolve VictimQuerySpanId from broken locks and set it on TxManager.
template <typename TLockCollection>
inline void SetVictimQuerySpanIdFromBrokenLocks(
    ui64 shardId,
    const TLockCollection& locks,
    const IKqpTransactionManagerPtr& txManager,
    const NKikimrDataEvents::TEvWriteResult* writeResult = nullptr)
{
    // Try to look up from TxManager's stored lock entries via the provided locks.
    for (const auto& lock : locks) {
        if (auto victimSpanId = txManager->LookupVictimQuerySpanId(shardId, lock)) {
            txManager->SetVictimQuerySpanId(*victimSpanId);
            return;
        }
    }
    // Fallback to DeferredVictimQuerySpanId from TxStats, set by the shard when TxLocks
    // are not populated (e.g. EnsureCurrentLock or serialization conflict paths).
    if (writeResult && writeResult->HasTxStats()
            && writeResult->GetTxStats().HasDeferredVictimQuerySpanId()) {
        txManager->SetVictimQuerySpanId(writeResult->GetTxStats().GetDeferredVictimQuerySpanId());
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
