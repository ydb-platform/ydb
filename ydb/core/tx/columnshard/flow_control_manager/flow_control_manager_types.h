#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/actor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NColumnShard::NFlowControl {

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

public:
    TLongTxWrite(const TActorId& replyTo, const NLongTxService::TLongTxId& longTxId, const TString& dedupId, const TString& databaseName,
        const TString& path, std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> navigateResult, std::shared_ptr<arrow::RecordBatch> batch,
        std::shared_ptr<NYql::TIssues> issues, TIntrusivePtr<NACLib::TUserContext> userCtx)
        : ReplyTo(replyTo)
        , LongTxId(longTxId)
        , DedupId(dedupId)
        , DatabaseName(databaseName)
        , Path(path)
        , NavigateResult(std::move(navigateResult))
        , Batch(std::move(batch))
        , Issues(std::move(issues))
        , UserCtx(std::move(userCtx))
    {
    }
};

}   // namespace NKikimr::NColumnShard::NFlowControl
