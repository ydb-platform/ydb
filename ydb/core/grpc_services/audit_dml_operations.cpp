#include <ydb/core/util/address_classifier.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_scripting.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/protos/draft/ydb_tablet.pb.h>

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

    template <class TxControl>
    void AddAuditLogTxControlPart(NKikimr::NGRpcService::IAuditCtx* ctx, const TxControl& tx_control)
    {
        switch (tx_control.tx_selector_case()) {
            case TxControl::kTxId:
                ctx->AddAuditLogPart("tx_id", tx_control.tx_id());
                break;
            case TxControl::kBeginTx:
                ctx->AddAuditLogPart("begin_tx", "1");
                break;
            case TxControl::TX_SELECTOR_NOT_SET:
                break;
        }
        ctx->AddAuditLogPart("commit_tx", ToString(tx_control.commit_tx()));
    }

    std::tuple<TString, TString, TString> GetDatabaseCloudIds(const std::vector<std::pair<TString, TString>>& databaseAttrs) {
        if (databaseAttrs.empty()) {
            return {};
        }
        auto getAttr = [&d = databaseAttrs](const TString &name) -> TString {
            const auto& found = std::find_if(d.begin(), d.end(), [name](const auto& item) { return item.first == name; });
            if (found != d.end()) {
                return found->second;
            }
            return {};
        };
        return std::make_tuple(
            getAttr("cloud_id"),
            getAttr("folder_id"),
            getAttr("database_id")
        );
    }
}

namespace NKikimr::NGRpcService {

void AuditContextStart(IAuditCtx* ctx, const TString& database, const TString& userSID, const std::vector<std::pair<TString, TString>>& databaseAttrs) {
    ctx->AddAuditLogPart("remote_address", NKikimr::NAddressClassifier::ExtractAddress(ctx->GetPeerName()));
    ctx->AddAuditLogPart("subject", userSID);
    ctx->AddAuditLogPart("database", database);
    ctx->AddAuditLogPart("operation", ctx->GetRequestName());
    ctx->AddAuditLogPart("start_time", TInstant::Now().ToString());

    auto [cloud_id, folder_id, database_id] = GetDatabaseCloudIds(databaseAttrs);
    if (cloud_id) {
        ctx->AddAuditLogPart("cloud_id", cloud_id);
    }
    if (folder_id) {
        ctx->AddAuditLogPart("folder_id", folder_id);
    }
    if (database_id) {
        ctx->AddAuditLogPart("resource_id", database_id);
    }
}

void AuditContextEnd(IAuditCtx* ctx) {
    ctx->AddAuditLogPart("end_time", TInstant::Now().ToString());
}

// ExecuteDataQuery
//
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::ExecuteDataQueryRequest& request) {
    // query_text or prepared_query_id
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
    AddAuditLogTxControlPart(ctx, request.tx_control());
}
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::ExecuteDataQueryRequest& request, const Ydb::Table::ExecuteQueryResult& result) {
    // tx_id, autocreated
    if (request.tx_control().tx_selector_case() == Ydb::Table::TransactionControl::kBeginTx) {
        ctx->AddAuditLogPart("tx_id", result.tx_meta().id());
    }
    // log updated_row_count collected from ExecuteQueryResult.query_stats?
}

// PrepareDataQuery
//
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::PrepareDataQueryRequest& request) {
    ctx->AddAuditLogPart("query_text", PrepareText(request.yql_text()));
}
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::PrepareDataQueryRequest& request, const Ydb::Table::PrepareQueryResult& result) {
    Y_UNUSED(request);
    ctx->AddAuditLogPart("prepared_query_id", result.query_id());
}

// BeginTransaction
//
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::BeginTransactionRequest& request, const Ydb::Table::BeginTransactionResult& result) {
    Y_UNUSED(request);
    ctx->AddAuditLogPart("tx_id", result.tx_meta().id());
}

// CommitTransaction
//
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::CommitTransactionRequest& request) {
    ctx->AddAuditLogPart("tx_id", request.tx_id());
}
// log updated_row_count collected from CommitTransactionResult.query_stats?

// RollbackTransaction
//
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::RollbackTransactionRequest& request) {
    ctx->AddAuditLogPart("tx_id", request.tx_id());
}

// BulkUpsert
//
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Table::BulkUpsertRequest& request) {
    ctx->AddAuditLogPart("table", request.table());
    //NOTE: no type checking for the rows field (should be a list) --
    // -- there is no point in being more thorough than the actual implementation,
    // see rpc_load_rows.cpp
    ctx->AddAuditLogPart("row_count", ToString(request.rows().value().items_size()));
}

// ExecuteYqlScript, StreamExecuteYqlScript
//
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Scripting::ExecuteYqlRequest& request) {
    ctx->AddAuditLogPart("query_text", PrepareText(request.script()));
}
// log updated_row_count collected from ExecuteYqlResult.query_stats?

// ExecuteQuery
//
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Query::ExecuteQueryRequest& request) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }
    // query_text
    {
        switch(request.query_case()) {
            case Ydb::Query::ExecuteQueryRequest::kQueryContent:
                ctx->AddAuditLogPart("query_text", PrepareText(request.query_content().text()));
                break;
            case Ydb::Query::ExecuteQueryRequest::QUERY_NOT_SET:
                break;
        }
    }
    // tx_id
    // begin_tx, commit_tx flags
    AddAuditLogTxControlPart(ctx, request.tx_control());
}
// log updated_row_count collected from ExecuteQueryResponsePart.exec_stats?

// ExecuteSrcipt
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Query::ExecuteScriptRequest& request) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }
    ctx->AddAuditLogPart("query_text", PrepareText(request.script_content().text()));
}
// log updated_row_count collected from ExecuteScriptMetadata.exec_stats?

// TabletService, ExecuteTabletMiniKQL
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Tablet::ExecuteTabletMiniKQLRequest& request) {
    if (request.dry_run()) {
        return;
    }
    ctx->AddAuditLogPart("tablet_id", TStringBuilder() << request.tablet_id());
    ctx->AddAuditLogPart("program_text", PrepareText(request.program()));
}

// TabletService, ChangeTabletSchema
template <>
void AuditContextAppend(IAuditCtx* ctx, const Ydb::Tablet::ChangeTabletSchemaRequest& request) {
    if (request.dry_run()) {
        return;
    }
    ctx->AddAuditLogPart("tablet_id", TStringBuilder() << request.tablet_id());
    ctx->AddAuditLogPart("schema_changes", PrepareText(request.schema_changes()));
}

} // namespace NKikimr::NGRpcService
