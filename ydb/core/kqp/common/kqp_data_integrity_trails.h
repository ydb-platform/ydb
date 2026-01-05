#pragma once

#include <openssl/sha.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/events/events.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/core/data_integrity_trails/data_integrity_trails.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NDataIntegrity {

// Class for collecting and managing query texts for TLI logging
class TQueryTextCollector {
public:
    // Add query text to the collection while avoiding duplicates and limiting size
    void AddQueryText(const TString& queryText) {
        if (!queryText.empty() && IS_INFO_LOG_ENABLED(NKikimrServices::TLI)) {
            // Only add if it's different from the previous query to avoid duplicates
            if (QueryTexts.empty() || QueryTexts.back() != queryText) {
                QueryTexts.push_back(queryText);
                // Keep only the last N queries to prevent unbounded memory growth
                constexpr size_t MAX_QUERY_TEXTS = 100;
                if (QueryTexts.size() > MAX_QUERY_TEXTS) {
                    QueryTexts.erase(QueryTexts.begin(), QueryTexts.begin() + (QueryTexts.size() - MAX_QUERY_TEXTS));
                }
            }
        }
    }

    // Combine all query texts into a single string for logging
    TString CombineQueryTexts() const {
        if (QueryTexts.empty()) {
            return "";
        }

        TStringBuilder builder;
        builder << std::accumulate(QueryTexts.begin() + 1, QueryTexts.end(), QueryTexts[0],
            [](const TString& acc, const TString& query) {
                return acc + "; " + query;
            });
        return builder;
    }

    // Check if there are any query texts
    bool Empty() const {
        return QueryTexts.empty();
    }

    // Clear all query texts
    void Clear() {
        QueryTexts.clear();
    }

private:
    TVector<TString> QueryTexts;
};

inline void LogQueryTextImpl(TStringStream& ss, const TString& queryText, bool hashed) {
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

inline void LogQueryText(TStringStream& ss, const TString& queryText) {
    const auto& config = AppData()->DataIntegrityTrailsConfig;
    const auto queryTextLogMode = config.HasQueryTextLogMode()
        ? config.GetQueryTextLogMode()
        : NKikimrProto::TDataIntegrityTrailsConfig_ELogMode_HASHED;

    LogQueryTextImpl(ss, queryText, queryTextLogMode == NKikimrProto::TDataIntegrityTrailsConfig_ELogMode_HASHED);
}

inline void LogQueryTextTli(TStringStream& ss, const TString& queryText) {
    const auto& config = AppData()->LogTliConfig;
    const auto queryTextLogMode = config.HasQueryTextLogMode()
        ? config.GetQueryTextLogMode()
        : NKikimrProto::TLogTliConfig_ELogMode_HASHED;

    LogQueryTextImpl(ss, queryText, queryTextLogMode == NKikimrProto::TLogTliConfig_ELogMode_HASHED);
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
        TStringStream ss;
        LogKeyValue("Component", "SessionActor", ss);
        LogKeyValue("SessionId", request->Get()->GetSessionId(), ss);
        LogKeyValue("TraceId", request->Get()->GetTraceId(), ss);
        LogKeyValue("Type", "Request", ss);
        LogKeyValue("QueryAction", ToString(request->Get()->GetAction()), ss);
        LogKeyValue("QueryType", ToString(request->Get()->GetType()), ss);

        LogQueryText(ss, request->Get()->GetQuery());

        if (request->Get()->HasTxControl()) {
            LogTxControl(request->Get()->GetTxControl(), ss);
        }

        return ss.Str();
    };

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(request));
}

inline void LogIntegrityTrails(const TString& traceId, NKikimrKqp::EQueryAction action, NKikimrKqp::EQueryType type, const std::unique_ptr<NKqp::TEvKqp::TEvQueryResponse>& response, const TActorContext& ctx) {
    if (!ShouldBeLogged(action, type)) {
        return;
    }

    auto log = [](const auto& traceId, const auto& response) {
        auto& record = response->Record;

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

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, log(traceId, response));
}

inline void LogTli(const TString& component, const TString& message, const TString& queryText, const TActorContext& ctx) {
    if (!IS_INFO_LOG_ENABLED(NKikimrServices::TLI)) {
        return;
    }

    TStringStream ss;
    LogKeyValue("Component", component, ss);
    LogKeyValue("Message", message, ss);

    LogQueryTextTli(ss, queryText);

    LOG_INFO_S(ctx, NKikimrServices::TLI, ss.Str());
}

// DataExecuter
inline void LogIntegrityTrails(const TString& txType, const TString& txLocksDebugStr, const TString& traceId, ui64 txId, TMaybe<ui64> shardId, const TActorContext& ctx) {
    auto log = [](const auto& type, const auto& txLocksDebugStr, const auto& traceId, const auto& txId, const auto& shardId) {
        TStringStream ss;
        LogKeyValue("Component", "Executer", ss);
        LogKeyValue("Type", "Request", ss);
        LogKeyValue("TraceId", traceId, ss);
        LogKeyValue("PhyTxId", ToString(txId), ss);
        LogKeyValue("Locks", "[" + txLocksDebugStr + "]", ss);

        if (shardId) {
            LogKeyValue("ShardId", ToString(*shardId), ss);
        }

        LogKeyValue("TxType", type, ss, /*last*/ true);

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, log(txType, txLocksDebugStr, traceId, txId, shardId));
}

inline void LogIntegrityTrails(const TString& state, const TString& traceId, const NEvents::TDataEvents::TEvWriteResult::TPtr& ev, const TActorContext& ctx) {
    auto log = [](const auto& state, const auto& traceId, const auto& ev) {
        const auto& record = ev->Get()->Record;

        TStringStream ss;
        LogKeyValue("Component", "Executer", ss);
        LogKeyValue("Type", "Response", ss);
        LogKeyValue("State", state, ss);
        LogKeyValue("TraceId", traceId, ss);
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
        LogKeyValue("Issues", issues.ToString(), ss, /*last*/ true);

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, log(state, traceId, ev));
}

inline void LogIntegrityTrails(const TString& state, const TString& traceId, const TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
    auto log = [](const auto& state, const auto& traceId, const auto& ev) {
        const auto& record = ev->Get()->Record;

        TStringStream ss;
        LogKeyValue("Component", "Executer", ss);
        LogKeyValue("Type", "Response", ss);
        LogKeyValue("State", state, ss);
        LogKeyValue("TraceId", traceId, ss);
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
        LogKeyValue("Issues", ev->Get()->GetError(), ss, /*last*/ true);

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, log(state, traceId, ev));
}

template <typename TActorResultInfo>
inline void LogIntegrityTrails(const TString& type, const TString& traceId, ui64 txId, const TActorResultInfo& info, const TActorContext& ctx) {
    auto log = [](const auto& type, const auto& traceId, const auto& txId, const auto& info) {
        TStringStream ss;
        LogKeyValue("Component", "Executer", ss);
        LogKeyValue("Type", type, ss);
        LogKeyValue("TraceId", traceId, ss);
        LogKeyValue("PhyTxId", ToString(txId), ss);

        TStringBuilder locksDebugStr;
        locksDebugStr << "[";
        for (const auto& lock : info.GetLocks()) {
            locksDebugStr << lock.ShortDebugString() << " ";
        }
        locksDebugStr << "]";

        LogKeyValue("Locks", locksDebugStr, ss);

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, log(type, traceId, txId, info));
}

// WriteActor,BufferActor
inline void LogIntegrityTrails(const TString& txType, ui64 txId, TMaybe<ui64> shardId, const TActorContext& ctx, const TStringBuf component) {
    auto log = [](const auto& type, const auto& txId, const auto& shardId, const auto component) {
        TStringStream ss;
        LogKeyValue("Component", component, ss);
        LogKeyValue("PhyTxId", ToString(txId), ss);

        if (shardId) {
            LogKeyValue("ShardId", ToString(*shardId), ss);
        }

        LogKeyValue("Type", type, ss, /*last*/ true);

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, log(txType, txId, shardId, component));
}

}
}
