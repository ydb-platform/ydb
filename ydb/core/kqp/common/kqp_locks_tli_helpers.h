#pragma once

#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/protos/data_events.pb.h>

namespace NKikimr {
namespace NKqp {

// Helper: extract VictimQuerySpanId from the first broken lock in a WriteResult record.
inline void SetVictimQuerySpanIdFromBrokenLocks(const NKikimrDataEvents::TEvWriteResult& record,
    const IKqpTransactionManagerPtr& txManager)
{
    for (const auto& lock : record.GetTxLocks()) {
        if (lock.HasQuerySpanId() && lock.GetQuerySpanId() != 0) {
            txManager->SetVictimQuerySpanId(lock.GetQuerySpanId());
            return;
        }
    }
}

// Helper: extract VictimQuerySpanId from the first broken lock in a ReadResult response.
inline void SetVictimQuerySpanIdFromBrokenLocks(
    const google::protobuf::RepeatedPtrField<NKikimrDataEvents::TLock>& brokenLocks,
    const IKqpTransactionManagerPtr& txManager)
{
    for (const auto& lock : brokenLocks) {
        if (lock.HasQuerySpanId() && lock.GetQuerySpanId() != 0) {
            txManager->SetVictimQuerySpanId(lock.GetQuerySpanId());
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
