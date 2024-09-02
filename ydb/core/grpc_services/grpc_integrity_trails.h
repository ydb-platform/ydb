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
    auto log = [](const auto& traceId, const auto& request) {
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", request.session_id(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogTxControl(request.tx_control(), ss);
        LogKeyValue("Type", "ExecuteDataQueryRequest", ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, request));
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::ExecuteDataQueryRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    auto log = [](const auto& traceId, const auto& request, const auto& response) {
        auto& record = response->Get()->Record.GetRef();

        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", record.GetResponse().GetSessionId(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "ExecuteDataQueryResponse", ss);

        if (request.tx_control().tx_selector_case() == Ydb::Table::TransactionControl::kBeginTx) {
            LogKeyValue("TxId", record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "Empty", ss);
        }

        LogKeyValue("Status", ToString(record.GetYdbStatus()), ss);
        LogKeyValue("Issues", ToString(record.GetResponse().GetQueryIssues()), ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, request, response));
}

// BeginTransaction
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::BeginTransactionRequest& request, const TActorContext& ctx) {
    auto log = [](const auto& traceId, const auto& request) {
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", request.session_id(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogTxSettings(request.tx_settings(), ss);
        LogKeyValue("Type", "BeginTransactionRequest", ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, request));
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::BeginTransactionRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    Y_UNUSED(request);

    auto log = [](const auto& traceId, const auto& response) {
        auto& record = response->Get()->Record.GetRef();

        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", record.GetResponse().GetSessionId(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "BeginTransactionResponse", ss);
        LogKeyValue("TxId", record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "Empty", ss);
        LogKeyValue("Status", ToString(record.GetYdbStatus()), ss);
        LogKeyValue("Issues", ToString(record.GetResponse().GetQueryIssues()), ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, response));
}

// CommitTransaction
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::CommitTransactionRequest& request, const TActorContext& ctx) {
    auto log = [](const auto& traceId, const auto& request) {
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", request.session_id(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "CommitTransactionRequest", ss);
        LogKeyValue("TxId", request.tx_id(), ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, request));
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::CommitTransactionRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    auto log = [](const auto& traceId, const auto& request, const auto& response) {
        const auto& record = response->Get()->Record.GetRef();
    
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", record.GetResponse().GetSessionId(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "CommitTransactionResponse", ss);
        LogKeyValue("TxId", request.tx_id(), ss);
        LogKeyValue("Status", ToString(record.GetYdbStatus()), ss);
        LogKeyValue("Issues", ToString(record.GetResponse().GetQueryIssues()), ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, request, response));
}

// RollbackTransaction
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::RollbackTransactionRequest& request, const TActorContext& ctx) {
    auto log = [](const auto& traceId, const auto& request) {
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", request.session_id(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "RollbackTransactionRequest", ss);
        LogKeyValue("TxId", request.tx_id(), ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, request));
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Table::RollbackTransactionRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    auto log = [](const auto& traceId, const auto& request, const auto& response) {
        const auto& record = response->Get()->Record.GetRef();
    
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", record.GetResponse().GetSessionId(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "RollbackTransactionResponse", ss);
        LogKeyValue("TxId", request.tx_id(), ss);
        LogKeyValue("Status", ToString(record.GetYdbStatus()), ss);
        LogKeyValue("Issues", ToString(record.GetResponse().GetQueryIssues()), ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, request, response));
}

// ExecuteYqlScript/StreamExecuteYqlScript
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Scripting::ExecuteYqlRequest& request, const TActorContext& ctx) {
    Y_UNUSED(request);
    
    auto log = [](const auto& traceId) {
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "[Stream]ExecuteYqlScriptRequest", ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId));
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Scripting::ExecuteYqlRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    Y_UNUSED(request);
    
    auto log = [](const auto& traceId, const auto& response) {
        const auto& record = response->Get()->Record.GetRef();

        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", record.GetResponse().GetSessionId(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "[Stream]ExecuteYqlScriptResponse", ss);
        LogKeyValue("Status", ToString(record.GetYdbStatus()), ss);
        LogKeyValue("Issues", ToString(record.GetResponse().GetQueryIssues()), ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, response));
}

// ExecuteQuery
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Query::ExecuteQueryRequest& request, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    auto log = [](const auto& traceId, const auto& request) {
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", request.session_id(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogTxControl(request.tx_control(), ss);
        LogKeyValue("Type", "ExecuteQueryRequest", ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, request));
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Query::ExecuteQueryRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    auto log = [](const auto&  traceId, const auto& request, const auto& response) {
        const auto& record = response->Get()->Record.GetRef();

        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("SessionId", record.GetResponse().GetSessionId(), ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "ExecuteQueryResponse", ss);
    
        if (request.tx_control().tx_selector_case() == Ydb::Query::TransactionControl::kBeginTx) {
            LogKeyValue("TxId", record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "Empty", ss);
        }

        LogKeyValue("Status", ToString(record.GetYdbStatus()), ss);
        LogKeyValue("Issues", ToString(record.GetResponse().GetQueryIssues()), ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, request, response));
}

// ExecuteSrcipt
inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Query::ExecuteScriptRequest& request, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    auto log = [](const auto& traceId) {
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "ExecuteSrciptRequest", ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId));
}

inline void LogIntegrityTrails(const TMaybe<TString>& traceId, const Ydb::Query::ExecuteScriptRequest& request, const NKqp::TEvKqp::TEvScriptResponse::TPtr& response, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    auto log = [](const auto& traceId, const auto& response) {
        TStringStream ss;
        LogKeyValue("Component", "Grpc", ss);
        LogKeyValue("TraceId", traceId ? *traceId : "Empty", ss);
        LogKeyValue("Type", "ExecuteSrciptResponse", ss);   
        LogKeyValue("Status", ToString(response->Get()->Status), ss);
        LogKeyValue("Issues", ToString(response->Get()->Issues), ss, /*last*/ true);
        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, response));
}

}
}
