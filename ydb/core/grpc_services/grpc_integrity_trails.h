#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_scripting.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/core/data_integrity_trails/data_integrity_trails.h>
#include <ydb/core/kqp/common/events/events.h>

namespace NKikimr {
namespace NDataIntegrity {

// ExecuteDataQuery
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::ExecuteDataQueryRequest& request, const TActorContext& ctx) {
    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"sessionId", request.session_id()},
        {"traceId", traceId},
        {"type", "ExecuteDataQueryRequest"},
        LogTxControl(request.tx_control()));
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::ExecuteDataQueryRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    auto& record = response->Get()->Record;
    auto message = YDB_LOG_CREATE_MESSAGE(
        {"component", "Grpc"},
        {"sessionId", request.session_id()},
        {"traceId", traceId},
        {"type", "ExecuteDataQueryResponse"},
        {"status", record.GetYdbStatus()},
        {"issues", record.GetResponse().GetQueryIssues()});

    if (request.tx_control().tx_selector_case() == Ydb::Table::TransactionControl::kBeginTx) {
        YDB_LOG_UPDATE_MESSAGE(message,
            {"txId", record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "Empty"});
    }

    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "", message);
}

// BeginTransaction
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::BeginTransactionRequest& request, const TActorContext& ctx) {
    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"sessionId", request.session_id()},
        {"traceId", traceId},
        LogTxSettings(request.tx_settings()),
        {"type", "BeginTransactionRequest"});
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::BeginTransactionRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    Y_UNUSED(request);

    auto& record = response->Get()->Record;
    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"sessionId", request.session_id()},
        {"traceId", traceId},
        {"type", "BeginTransactionResponse"},
        {"txId", record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "Empty"},
        {"status", record.GetYdbStatus()},
        {"issues", record.GetResponse().GetQueryIssues()});
}

// CommitTransaction
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::CommitTransactionRequest& request, const TActorContext& ctx) {
    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"sessionId", request.session_id()},
        {"traceId", traceId},
        {"type", "CommitTransactionRequest"},
        {"txId", request.tx_id()});
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::CommitTransactionRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {

    const auto& record = response->Get()->Record;

    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"sessionId", record.GetResponse().GetSessionId()},
        {"traceId", traceId},
        {"type", "CommitTransactionResponse"},
        {"txId", request.tx_id()},
        {"status", record.GetYdbStatus()},
        {"issues", record.GetResponse().GetQueryIssues()});
}

// RollbackTransaction
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::RollbackTransactionRequest& request, const TActorContext& ctx) {
    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"sessionId", request.session_id()},
        {"traceId", traceId},
        {"type", "RollbackTransactionRequest"},
        {"txId", request.tx_id()});
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::RollbackTransactionRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    const auto& record = response->Get()->Record;
    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"sessionId", request.session_id()},
        {"traceId", traceId},
        {"type", "RollbackTransactionResponse"},
        {"txId", request.tx_id()},
        {"status", record.GetYdbStatus()},
        {"issues", record.GetResponse().GetQueryIssues()});
}

// ExecuteYqlScript/StreamExecuteYqlScript
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Scripting::ExecuteYqlRequest& request, const TActorContext& ctx) {
    Y_UNUSED(request);

    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"traceId", traceId},
        {"type", "[Stream]ExecuteYqlScriptRequest"});
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Scripting::ExecuteYqlRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    Y_UNUSED(request);

    const auto& record = response->Get()->Record;
    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"sessionId", record.GetResponse().GetSessionId()},
        {"traceId", traceId},
        {"type", "[Stream]ExecuteYqlScriptResponse"},
        {"status", record.GetYdbStatus()},
        {"issues", record.GetResponse().GetQueryIssues()});
}

// ExecuteQuery
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Query::ExecuteQueryRequest& request, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"sessionId", request.session_id()},
        {"traceId", traceId},
        {"type", "ExecuteQueryRequest"},
        LogTxControl(request.tx_control()));
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Query::ExecuteQueryRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    const auto& record = response->Get()->Record;
    auto message = YDB_LOG_CREATE_MESSAGE(
        {"component", "Grpc"},
        {"sessionId", record.GetResponse().GetSessionId()},
        {"traceId", traceId},
        {"type", "ExecuteQueryResponse"},
        {"status", record.GetYdbStatus()},
        {"issues", record.GetResponse().GetQueryIssues()});
    if (request.tx_control().tx_selector_case() == Ydb::Query::TransactionControl::kBeginTx) {
        YDB_LOG_UPDATE_MESSAGE(message,
            {"txId", record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "Empty"});
    }

    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "", message);
}

// ExecuteSrcipt
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Query::ExecuteScriptRequest& request, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"traceId", traceId},
        {"type", "ExecuteSrciptRequest"});
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Query::ExecuteScriptRequest& request, const NKqp::TEvKqp::TEvScriptResponse::TPtr& response, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "Grpc"},
        {"traceId", traceId},
        {"type", "ExecuteSrciptResponse"},
        {"status", response->Get()->Status},
        {"issues", response->Get()->Issues});
}

}
}
