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

inline void LogQueryTextImpl(TStructuredMessage& message, const TString& queryText, bool hashed) {
    if (!hashed) {
        YDB_LOG_UPDATE_CONTEXT(message,
            {"queryText", queryText});
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

    YDB_LOG_UPDATE_CONTEXT(message,
        {"queryText", Base64Encode(hashedQueryText)});
}

inline TStructuredMessage LogQueryText(const TString& queryText) {
    TStructuredMessage message;
    const auto& config = AppData()->DataIntegrityTrailsConfig;
    LogQueryTextImpl(message, queryText, config.GetQueryTextLogMode() == NKikimrProto::TDataIntegrityTrailsConfig_ELogMode_HASHED);
    return message;
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
    YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "SessionActor"},
        {"sessionId", request->Get()->GetSessionId()},
        {"traceId", request->Get()->GetTraceId()},
        {"type", "Request"},
        {"queryAction", ToString(request->Get()->GetAction())},
        {"queryType", ToString(request->Get()->GetType())},
        LogQueryText(request->Get()->GetQuery())
    );
}

inline void LogIntegrityTrails(const TString& traceId, NKikimrKqp::EQueryAction action, NKikimrKqp::EQueryType type, const std::unique_ptr<NKqp::TEvKqp::TEvQueryResponse>& response, const TActorContext& ctx) {
    if (!ShouldBeLogged(action, type)) {
        return;
    }

    auto& record = response->Record;
    YDB_LOG_DEBUG_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
        {"component", "SessionActor"},
        {"sessionId", record.GetResponse().GetSessionId()},
        {"traceId", traceId},
        {"type", "Response"},
        {"txId", record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "Empty"},
        {"status", record.GetYdbStatus()},
        {"issues", record.GetResponse().GetQueryIssues()});
}

// template <typename TLockInfo>
// inline TStructuredMessage ToStructuredLog(const TLockInfo& lock) {

inline TStructuredMessage ToStructuredLog(const NKikimrDataEvents::TLock& lock) {
    TStructuredMessage result;
    if (lock.HasLockId()) {
        YDB_LOG_UPDATE_MESSAGE(result , {"lockId", lock.GetLockId()});
    }

    if (lock.HasDataShard()) {
        YDB_LOG_UPDATE_MESSAGE(result , {"dataShard", lock.GetDataShard()});
    }

    if (lock.HasGeneration()) {
        YDB_LOG_UPDATE_MESSAGE(result , {"generation", lock.GetGeneration()});
    }

    if (lock.HasCounter()) {
        YDB_LOG_UPDATE_MESSAGE(result , {"counter", lock.GetCounter()});
    }

    if (lock.HasSchemeShard()) {
        YDB_LOG_UPDATE_MESSAGE(result , {"schemeShard", lock.GetCounter()});
    }

    if (lock.HasPathId()) {
        YDB_LOG_UPDATE_MESSAGE(result , {"schemeShard", lock.GetPathId()});
    }
    return result;
}

// DataExecuter
inline void LogIntegrityTrails(const TString& state, const TString& traceId, const NEvents::TDataEvents::TEvWriteResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    NYql::TIssues issues;
    NYql::IssuesFromMessage(record.GetIssues(), issues);

    auto message = YDB_LOG_CREATE_MESSAGE({"component", "Executer"},
        {"type", "Request"},
        {"state", state},
        {"traceId", traceId},
        {"phyTxId", ToString(record.GetTxId())},
        {"shardId", ToString(record.GetOrigin())},
        {"status", NKikimrDataEvents::TEvWriteResult::EStatus_Name(ev->Get()->GetStatus())},
        {"issues", issues.ToString()});

    if (record.GetTxLocks().empty()) {
        YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "", message);
        return ;
    }

    if (record.GetTxLocks().empty()) {
        YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "", message);
        return ;
    }

    for (const auto& lock : record.GetTxLocks()) {
        YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
            message,
            {"lock", ToStructuredLog(lock)});
    }
}

inline void LogIntegrityTrails(const TString& state, const TString& traceId, const TEvDataShard::TEvProposeTransactionResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    auto message = YDB_LOG_CREATE_MESSAGE(
        {"component", "Executer"},
        {"type", "Response"},
        {"state", state},
        {"traceId", traceId},
        {"phyTxId", ToString(record.GetTxId())},
        {"shardId", ToString(record.GetOrigin())},
        {"status", NKikimrTxDataShard::TEvProposeTransactionResult_EStatus_Name(ev->Get()->GetStatus())},
        {"issues", ev->Get()->GetError()});

    if (record.GetTxLocks().empty()) {
        YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "", message);
        return ;
    }

    for (const auto& lock : record.GetTxLocks()) {
        YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
            message,
            {"lock", ToStructuredLog(lock)});
        }
}

template <typename TActorResultInfo>
inline void LogIntegrityTrails(const TString& type, const TString& traceId, ui64 txId, const TActorResultInfo& info, const TActorContext& ctx) {
    auto message = YDB_LOG_CREATE_MESSAGE(
        {"component", "Executer"},
        {"type", type},
        {"traceId", traceId},
        {"phyTxId", ToString(txId)});

    if (info.GetLocks().empty()) {
        YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "", message);
        return ;
    }

    for (const auto& lock : info.GetLocks()) {
        YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
            message,
            {"lock", ToStructuredLog(lock)});
    }
}

// WriteActor,BufferActor
inline void LogIntegrityTrails(const TString& txType, ui64 txId, TMaybe<ui64> shardId, const TActorContext& ctx, const TStringBuf component) {
    if (!IS_CTX_LOG_PRIORITY_ENABLED(ctx, NActors::NLog::PRI_INFO, NKikimrServices::DATA_INTEGRITY, 0)) {
        return;
    }
    auto message = YDB_LOG_CREATE_MESSAGE(
        {"component", component},
        {"type", txType},
        {"phyTxId", ToString(txId)});
    if (shardId) {
        YDB_LOG_UPDATE_MESSAGE(message,
            {"shardId", ToString(*shardId)});
    }
    YDB_LOG_INFO_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "", message);
}

}
}
