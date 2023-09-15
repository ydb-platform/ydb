#include <ydb/public/api/protos/ydb_table.pb.h>

#include "base/base.h"

#include "audit_dml_operations.h"

namespace {
    // Query text could be very large, multilined and formatted with indentations.
    // It should be prepared and somewhat limited before getting dumped into the logs.
    const size_t MAX_QUERY_TEXT_LEN = 1024;

    TString PrepareText(const TString& original) {
        TString text = original;
        { // transform multiline indented text into a single line
            SubstGlobal(text, '\n', ' ');
            SubstGlobal(text, '\r', ' ');
            while (SubstGlobal(text, "  ", " ") > 0) {}
        }
        return CollapseInPlace(StripInPlace(text), MAX_QUERY_TEXT_LEN);
    }
}

namespace NKikimr::NGRpcService {

void AuditContextStart(IRequestCtxBase* ctx, const TString& database, const TString& userSID) {
    ctx->AddAuditLogPart("remote_address", ctx->GetPeerName());
    ctx->AddAuditLogPart("subject", userSID);
    ctx->AddAuditLogPart("database", database);
    ctx->AddAuditLogPart("operation", ctx->GetRequestName());
    ctx->AddAuditLogPart("start_time", TInstant::Now().ToString());
}

void AuditContextEnd(IRequestCtxBase* ctx) {
    ctx->AddAuditLogPart("end_time", TInstant::Now().ToString());
}

// ExecuteDataQuery
//
template <>
void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::ExecuteDataQueryRequest& request) {
    // yql_text or prepared_query_id
    {
        auto query = request.query();
        if (query.has_yql_text()) {
            ctx->AddAuditLogPart("query_text", PrepareText(query.yql_text()));
        } else if (query.has_id()) {
            ctx->AddAuditLogPart("prepared_query_id", query.id());
        }
    }
    // tx_id, explicit
    // begin_tx, commit_tx flags
    {
        auto tx_control = request.tx_control();
        switch (tx_control.tx_selector_case()) {
            case Ydb::Table::TransactionControl::kTxId:
                ctx->AddAuditLogPart("tx_id", tx_control.tx_id());
                break;
            case Ydb::Table::TransactionControl::kBeginTx:
                ctx->AddAuditLogPart("begin_tx", "1");
                break;
            case Ydb::Table::TransactionControl::TX_SELECTOR_NOT_SET:
                break;
        }
        ctx->AddAuditLogPart("commit_tx", ToString(tx_control.commit_tx()));
    }
}
template <>
void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::ExecuteDataQueryRequest& request, const Ydb::Table::ExecuteQueryResult& result) {
    // tx_id, autocreated
    if (request.tx_control().tx_selector_case() == Ydb::Table::TransactionControl::kBeginTx) {
        ctx->AddAuditLogPart("tx_id", result.tx_meta().id());
    }
    // log updated_row_count from ExecuteQueryResult.query_stats?
}

// PrepareDataQuery
//
template <>
void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::PrepareDataQueryRequest& request) {
    ctx->AddAuditLogPart("query_text", PrepareText(request.yql_text()));
}
template <>
void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::PrepareDataQueryRequest& request, const Ydb::Table::PrepareQueryResult& result) {
    Y_UNUSED(request);
    ctx->AddAuditLogPart("prepared_query_id", result.query_id());
}

// BeginTransaction
//
template <>
void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::BeginTransactionRequest& request, const Ydb::Table::BeginTransactionResult& result) {
    Y_UNUSED(request);
    ctx->AddAuditLogPart("tx_id", result.tx_meta().id());
}

// CommitTransaction
//
template <>
void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::CommitTransactionRequest& request) {
    ctx->AddAuditLogPart("tx_id", request.tx_id());
}
// log updated_row_count by CommitTransactionResult.query_stats?

// RollbackTransaction
//
template <>
void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::RollbackTransactionRequest& request) {
    ctx->AddAuditLogPart("tx_id", request.tx_id());
}

// BulkUpsert
//
template <>
void AuditContextAppend(IRequestCtx* ctx, const Ydb::Table::BulkUpsertRequest& request) {
    ctx->AddAuditLogPart("table", request.table());
    //NOTE: no type checking for the rows field (should be a list) --
    // -- there is no point in being more thorough than the actual implementation,
    // see rpc_load_rows.cpp
    ctx->AddAuditLogPart("row_count", ToString(request.rows().value().items_size()));
}

} // namespace NKikimr::NGRpcService