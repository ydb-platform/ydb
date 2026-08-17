#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/actor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/datetime/base.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NColumnShard::NFlowControl {

enum class EAdmitDecision {
    Allow,
    RejectNow,
    Wait,
    DelayedReject,   // Queue is full; drop Arrow batch, send OVERLOADED after delay
};

// What a finished shard write learned about its destination.
//
// Unknown is not a quieter kind of Ok: a write that ended without ever hearing back (timeout,
// broken pipe) carries no evidence in either direction. Counting it as clean is what would let a
// shard that stopped answering altogether complete cohorts and drive the rate *up*, which is the
// worst possible response. Counting it as overloaded would be wrong too, since the cause may be a
// network fault that says nothing about the destination's capacity — so it counts as neither.
enum class EWriteOutcome {
    Ok,
    Overloaded,
    Unknown,
};

class TLongTxWrite {
    YDB_READONLY_DEF(TActorId, ReplyTo);
    YDB_READONLY_DEF(NLongTxService::TLongTxId, LongTxId);
    YDB_READONLY_DEF(TString, DedupId);
    YDB_READONLY_DEF(TString, DatabaseName);
    YDB_READONLY_DEF(TString, Path);
    YDB_READONLY_DEF(std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate>, NavigateResult);
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Batch);
    YDB_READONLY_DEF(std::shared_ptr<NYql::TIssues>, Issues);
    YDB_READONLY_DEF(TIntrusivePtr<NACLib::TUserContext>, UserCtx);
    YDB_READONLY_DEF(TInstant, Deadline);
    YDB_READONLY_DEF(TDuration, OperationTimeout);

public:
    TLongTxWrite(const TActorId& replyTo, const NLongTxService::TLongTxId& longTxId, const TString& dedupId, const TString& databaseName,
        const TString& path, std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> navigateResult, std::shared_ptr<arrow::RecordBatch> batch,
        std::shared_ptr<NYql::TIssues> issues, TIntrusivePtr<NACLib::TUserContext> userCtx, TInstant deadline = TInstant::Max(),
        TDuration operationTimeout = TDuration::Seconds(5 * 60))
        : ReplyTo(replyTo)
        , LongTxId(longTxId)
        , DedupId(dedupId)
        , DatabaseName(databaseName)
        , Path(path)
        , NavigateResult(std::move(navigateResult))
        , Batch(std::move(batch))
        , Issues(std::move(issues))
        , UserCtx(std::move(userCtx))
        , Deadline(deadline)
        , OperationTimeout(operationTimeout)
    {
    }

    // Prefer TFlowControlManagerServiceOperator::ComputeWaitDeadline at admit time.
    // Kept for helpers that still hold Deadline/Timeout locally.
    TInstant GetWaitDeadline() const;

    // Release the Arrow batch payload while keeping lightweight metadata.
    // Used on delayed-reject: we no longer intend to write, so free the (potentially large)
    // record batch immediately instead of holding it until the delayed OVERLOADED fires.
    void DetachBatch() {
        Batch.reset();
    }
};

}   // namespace NKikimr::NColumnShard::NFlowControl
