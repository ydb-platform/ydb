#pragma once

#include <openssl/sha.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/events/events.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>
#include <util/string/escape.h>

#include <ydb/core/data_integrity_trails/data_integrity_trails.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NDataIntegrity {

inline void LogQueryTextImpl(NActors::NStructuredLog::TStructuredMessage& ss, const TString& queryText, bool hashed) {
    if (!hashed) {
        LogKeyValue("QueryText", EscapeC(queryText), ss);
        return;
    }

    // Hash the query text
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    if (SHA256_Init(&sha256) != 1) {
        return;
    }
    if (SHA256_Update(&sha256, queryText.data(), queryText.size()) != 1) {
        return;
    }
    if (SHA256_Final(hash, &sha256) != 1) {
        return;
    }
    std::string hashedQueryText(reinterpret_cast<char*>(hash), SHA256_DIGEST_LENGTH);
    LogKeyValue("QueryText", Base64Encode(hashedQueryText), ss);
}

inline void LogQueryText(NActors::NStructuredLog::TStructuredMessage& ss, const TString& queryText) {
    const auto& config = AppData()->DataIntegrityTrailsConfig;
    LogQueryTextImpl(ss, queryText, config.GetQueryTextLogMode() == NKikimrProto::TDataIntegrityTrailsConfig_ELogMode_HASHED);
}

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
        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "SessionActor", ss);
        LogKeyValue("SessionId", request->Get()->GetSessionId(), ss);

        if (!request->Get()->GetTraceId().empty()) {
            LogKeyValue("TraceId", request->Get()->GetTraceId(), ss);
        }

        LogKeyValue("Type", "Request", ss);
        LogKeyValue("QueryAction", ToString(request->Get()->GetAction()), ss);
        LogKeyValue("QueryType", ToString(request->Get()->GetType()), ss);

        LogQueryText(ss, request->Get()->GetQuery());

        if (request->Get()->HasTxControl()) {
            LogTxControl(request->Get()->GetTxControl(), ss);
        }

        return ss;
    };

    YDB_LOG_CTX_COMP_TRACE(ctx, DATA_INTEGRITY, "Integrity trails", log(request));
}

inline void LogIntegrityTrails(const TString& traceId, NKikimrKqp::EQueryAction action, NKikimrKqp::EQueryType type, const std::unique_ptr<NKqp::TEvKqp::TEvQueryResponse>& response, const TActorContext& ctx) {
    if (!ShouldBeLogged(action, type)) {
        return;
    }

    auto log = [](const auto& traceId, const auto& response) {
        auto& record = response->Record;

        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "SessionActor", ss);
        LogKeyValue("SessionId", record.GetResponse().GetSessionId(), ss);

        if (!traceId.empty()) {
            LogKeyValue("TraceId", traceId, ss);
        }

        LogKeyValue("Type", "Response", ss);
        LogKeyValue("TxId", record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "Empty", ss);
        LogKeyValue("Status", ToString(record.GetYdbStatus()), ss);
        LogKeyValue("Issues", ToString(record.GetResponse().GetQueryIssues()), ss);

        return ss;
    };

    YDB_LOG_CTX_COMP_TRACE(ctx, DATA_INTEGRITY, "Integrity trails", log(traceId, response));
}

// DataExecuter
inline void LogIntegrityTrails(const TString& txType, const TString& txLocksDebugStr, const TString& traceId, ui64 txId, TMaybe<ui64> shardId, const TActorContext& ctx) {
    auto log = [](const auto& type, const auto& txLocksDebugStr, const auto& traceId, const auto& txId, const auto& shardId) {
        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "Executer", ss);
        LogKeyValue("Type", "Request", ss);

        if (!traceId.empty()) {
            LogKeyValue("TraceId", traceId, ss);
        }

        LogKeyValue("PhyTxId", ToString(txId), ss);
        LogKeyValue("Locks", "[" + txLocksDebugStr + "]", ss);

        if (shardId) {
            LogKeyValue("ShardId", ToString(*shardId), ss);
        }

        LogKeyValue("TxType", type, ss);

        return ss;
    };

    YDB_LOG_CTX_COMP_TRACE(ctx, DATA_INTEGRITY, "Integrity trails", log(txType, txLocksDebugStr, traceId, txId, shardId));
}

inline void LogIntegrityTrails(const TString& state, const TString& traceId, const NEvents::TDataEvents::TEvWriteResult::TPtr& ev, const TActorContext& ctx) {
    auto log = [](const auto& state, const auto& traceId, const auto& ev) {
        const auto& record = ev->Get()->Record;

        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "Executer", ss);
        LogKeyValue("Type", "Response", ss);
        LogKeyValue("State", state, ss);

        if (!traceId.empty()) {
            LogKeyValue("TraceId", traceId, ss);
        }

        LogKeyValue("PhyTxId", ToString(record.GetTxId()), ss);
        LogKeyValue("ShardId", ToString(record.GetOrigin()), ss);

        TStringBuilder locksDebugStr;
        locksDebugStr << "[";
        for (const auto& lock : record.GetTxLocks()) {
            locksDebugStr << lock.ShortDebugString() << " ";
        }
        locksDebugStr << "]";

        LogKeyValue("Locks", locksDebugStr, ss);
        LogKeyValue("Status",  NKikimrDataEvents::TEvWriteResult::EStatus_Name(ev->Get()->GetStatus()), ss);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetIssues(), issues);
        LogKeyValue("Issues", issues.ToString(), ss);

        return ss;
    };

    YDB_LOG_CTX_COMP_TRACE(ctx, DATA_INTEGRITY, "Integrity trails", log(state, traceId, ev));
}

inline void LogIntegrityTrails(const TString& state, const TString& traceId, const TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
    auto log = [](const auto& state, const auto& traceId, const auto& ev) {
        const auto& record = ev->Get()->Record;

        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "Executer", ss);
        LogKeyValue("Type", "Response", ss);
        LogKeyValue("State", state, ss);

        if (!traceId.empty()) {
            LogKeyValue("TraceId", traceId, ss);
        }

        LogKeyValue("PhyTxId", ToString(record.GetTxId()), ss);
        LogKeyValue("ShardId", ToString(record.GetOrigin()), ss);

        TStringBuilder locksDebugStr;
        locksDebugStr << "[";
        for (const auto& lock : record.GetTxLocks()) {
            locksDebugStr << lock.ShortDebugString() << " ";
        }
        locksDebugStr << "]";

        LogKeyValue("Locks", locksDebugStr, ss);
        LogKeyValue("Status",  NKikimrTxDataShard::TEvProposeTransactionResult_EStatus_Name(ev->Get()->GetStatus()), ss);
        LogKeyValue("Issues", ev->Get()->GetError(), ss);

        return ss;
    };

    YDB_LOG_CTX_COMP_TRACE(ctx, DATA_INTEGRITY, "Integrity trails", log(state, traceId, ev));
}

template <typename TActorResultInfo>
inline void LogIntegrityTrails(const TString& type, const TString& traceId, ui64 txId, const TActorResultInfo& info, const TActorContext& ctx) {
    auto log = [](const auto& type, const auto& traceId, const auto& txId, const auto& info) {
        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "Executer", ss);
        LogKeyValue("Type", type, ss);

        if (!traceId.empty()) {
            LogKeyValue("TraceId", traceId, ss);
        }

        LogKeyValue("PhyTxId", ToString(txId), ss);

        TStringBuilder locksDebugStr;
        locksDebugStr << "[";
        for (const auto& lock : info.GetLocks()) {
            locksDebugStr << lock.ShortDebugString() << " ";
        }
        locksDebugStr << "]";

        LogKeyValue("Locks", locksDebugStr, ss);

        return ss;
    };

    YDB_LOG_CTX_COMP_TRACE(ctx, DATA_INTEGRITY, "Integrity trails", log(type, traceId, txId, info));
}

// WriteActor,BufferActor
inline void LogIntegrityTrails(const TString& txType, ui64 txId, TMaybe<ui64> shardId, const TActorContext& ctx, const TStringBuf component) {
    auto log = [](const auto& type, const auto& txId, const auto& shardId, const auto component) {
        NActors::NStructuredLog::TStructuredMessage ss;

        LogKeyValue("Component", component, ss);
        LogKeyValue("PhyTxId", ToString(txId), ss);

        if (shardId) {
            LogKeyValue("ShardId", ToString(*shardId), ss);
        }

        LogKeyValue("Type", type, ss);

        return ss;
    };

    YDB_LOG_CTX_COMP_TRACE(ctx, DATA_INTEGRITY, "Integrity trails", log(txType, txId, shardId, component));
}

}
}
