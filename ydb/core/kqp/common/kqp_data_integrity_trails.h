#pragma once

#include <openssl/sha.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/data_integrity_trails/data_integrity_trails.h>

namespace NKikimr {
namespace NDataIntegrity {

inline bool ShouldBeLogged(NKikimrKqp::EQueryAction action, NKikimrKqp::EQueryType type) {
    switch (type) {
        case NKikimrKqp::QUERY_TYPE_SQL_DDL:
        case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
        case NKikimrKqp::QUERY_TYPE_AST_SCAN:
            return false;
        default:
            break;    
    }

    switch (action) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
        case NKikimrKqp::QUERY_ACTION_BEGIN_TX:
        case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
        case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX:
            return true;
        default:
            return false;
    }
}

// SessionActor
inline void LogIntegrityTrails(const NKqp::TEvKqp::TEvQueryRequest::TPtr& request, const TActorContext& ctx) {
    if (!ShouldBeLogged(request->Get()->GetAction(), request->Get()->GetType())) {
        return;
    }
    
    auto log = [](const auto& request) {
        TStringStream ss;
        LogKeyValue("Component", "SessionActor", ss);
        LogKeyValue("SessionId", request->Get()->GetSessionId(), ss);
        LogKeyValue("TraceId", request->Get()->GetTraceId(), ss);
        LogKeyValue("Type", "Request", ss);
        LogKeyValue("QueryAction", ToString(request->Get()->GetAction()), ss);
        LogKeyValue("QueryType", ToString(request->Get()->GetType()), ss);

        if (request->Get()->HasTxControl()) {
            LogTxControl(request->Get()->GetTxControl(), ss);
        }

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, log(request));
}

inline void LogIntegrityTrails(const TString& traceId, NKikimrKqp::EQueryAction action, NKikimrKqp::EQueryType type, const std::unique_ptr<NKqp::TEvKqp::TEvQueryResponse>& response, const TActorContext& ctx) {
    if (!ShouldBeLogged(action, type)) {
        return;
    }

    auto log = [](const auto& traceId, const auto& response) {
        auto& record = response->Record.GetRef();

        TStringStream ss;
        LogKeyValue("Component", "SessionActor", ss);
        LogKeyValue("SessionId", record.GetResponse().GetSessionId(), ss);
        LogKeyValue("TraceId", traceId, ss);
        LogKeyValue("Type", "Response", ss);
        LogKeyValue("TxId", record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "Empty", ss);
        LogKeyValue("Status", ToString(record.GetYdbStatus()), ss);
        LogKeyValue("Issues", ToString(record.GetResponse().GetQueryIssues()), ss, /*last*/ true);

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, response));
}

// DataExecuter
inline void LogIntegrityTrails(const TString& txType, const TString& traceId, ui64 txId, TMaybe<ui64> shardId, const TActorContext& ctx) {
    auto log = [](const auto& type, const auto& traceId, const auto& txId, const auto& shardId) {
        TStringStream ss;
        LogKeyValue("Component", "Executer", ss);
        LogKeyValue("TraceId", traceId, ss);
        LogKeyValue("PhyTxId", ToString(txId), ss);

        if (shardId) {
            LogKeyValue("ShardId", ToString(*shardId), ss);
        }

        LogKeyValue("Type", type, ss, /*last*/ true);

        return ss.Str();
    };

    LOG_NOTICE_S(ctx, NKikimrServices::DATA_INTEGRITY, log(txType, traceId, txId, shardId));
}

}
}
