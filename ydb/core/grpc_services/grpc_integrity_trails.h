#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_scripting.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/core/kqp/common/events/events.h>

namespace NKikimr {
namespace NDataIntegrity {

template <class TransactionSettings>
inline void LogTxSettings(const TransactionSettings& txSettings, TStringStream& stream) {

    stream << "txMOde: ";
    switch (txSettings.tx_mode_case()) {
        case TransactionSettings::kSerializableReadWrite:
            stream << "SerializableReadWrite";
            break;
        case TransactionSettings::kOnlineReadOnly:
            stream << "OnlineReadOnly, allow_inconsistent_read: " << txSettings.online_read_only().allow_inconsistent_reads();
            break;
        case TransactionSettings::kStaleReadOnly:
            stream << "StaleReadOnly";
            break;
        case TransactionSettings::kSnapshotReadOnly:
            stream << "SnapshotReadOnly";
            break;
        case TransactionSettings::TX_MODE_NOT_SET:
            stream << "undefined";
            break;
    }
}    

template <class TxControl>
inline void LogTxControl(const TxControl& txControl, TStringStream& stream)
{
    switch (txControl.tx_selector_case()) {
        case TxControl::kTxId:
            stream << "user_tx_id: " << txControl.tx_id() << ",";
            break;
        case TxControl::kBeginTx:
            stream << "begin_tx: true,";
            LogTxSettings(txControl.begin_tx(), stream);
            break;
        case TxControl::TX_SELECTOR_NOT_SET:
            break;
    }

    stream << "need_commit_tx: " << ToString(txControl.commit_tx()) << ",";
}

// ExecuteDataQuery NKqp::TEvKqp::TEvQueryRequest
inline void LogIntegrityTrails(const Ydb::Table::ExecuteDataQueryRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    auto& record = response->Get()->Record.GetRef();

    TStringStream stream;
    stream << "session_id: " << record.GetResponse().GetSessionId() << ",";
    stream << "type: ExecuteDataQuery,";
    stream << "status: " << record.GetYdbStatus() << ",";

    LogTxControl(request.tx_control(), stream);

    if (request.tx_control().tx_selector_case() == Ydb::Table::TransactionControl::kBeginTx) {
        stream << "user_tx_id: " << (record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "empty") << ",";
    }

    stream << record.GetResponse().GetQueryIssues();

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, stream.Str());
}

// BeginTransaction
inline void LogIntegrityTrails(const Ydb::Table::BeginTransactionRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    auto& record = response->Get()->Record.GetRef();

    TStringStream stream;
    stream << "session_id: " << record.GetResponse().GetSessionId() << ",";
    stream << "type: BeginTransaction,";
    stream << "status: " << record.GetYdbStatus() << ",";
    stream << "user_tx_id: " << (record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "empty") << ",";

    LogTxSettings(request.tx_settings(), stream);
    stream << record.GetResponse().GetQueryIssues();

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, stream.Str());
}

// CommitTransaction
inline void LogIntegrityTrails(const Ydb::Table::CommitTransactionRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    const auto& record = response->Get()->Record.GetRef();
    
    TStringStream stream;
    stream << "session_id: " << record.GetResponse().GetSessionId() << ",";
    stream << "type: CommitTransaction,";
    stream << "status: " << record.GetYdbStatus() << ",";
    stream << "user_tx_id: " << request.tx_id() << ",";
    stream << record.GetResponse().GetQueryIssues();

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, stream.Str());
}

// RollbackTransaction
inline void LogIntegrityTrails(const Ydb::Table::RollbackTransactionRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    const auto& record = response->Get()->Record.GetRef();
    
    TStringStream stream;
    stream << "session_id: " << record.GetResponse().GetSessionId() << ",";
    stream << "type: RollbackTransaction,";
    stream << "status: " << record.GetYdbStatus() << ",";
    stream << "user_tx_id: " << request.tx_id() << ",";
    stream << record.GetResponse().GetQueryIssues();

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, stream.Str());
}

// ExecuteYqlScript/StreamExecuteYqlScript
inline void LogIntegrityTrails(const Ydb::Scripting::ExecuteYqlRequest& /*request*/, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    const auto& record = response->Get()->Record.GetRef();

    TStringStream stream;
    stream << "session_id: " << record.GetResponse().GetSessionId() << ",";
    stream << "type: ExecuteYqlScript/StreamExecuteYqlScript,";
    stream << "status: " << record.GetYdbStatus() << ",";
    stream << record.GetResponse().GetQueryIssues();

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, stream.Str());
}


// ExecuteQuery
inline void LogIntegrityTrails(const Ydb::Query::ExecuteQueryRequest& request, NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    const auto& record = response->Get()->Record.GetRef();

    TStringStream stream;
    stream << "session_id: " << record.GetResponse().GetSessionId() << ",";
    stream << "type: ExecuteQuery,";     
    stream << "status: " << record.GetYdbStatus() << ",";
    LogTxControl(request.tx_control(), stream);
    
    if (request.tx_control().tx_selector_case() == Ydb::Query::TransactionControl::kBeginTx) {
        stream << "user_tx_id: " << (record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "empty") << ",";
    }

    stream << record.GetResponse().GetQueryIssues();
    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, stream.Str());
}

// ExecuteSrcipt
inline void LogIntegrityTrails(const Ydb::Query::ExecuteScriptRequest& request, const NKqp::TEvKqp::TEvScriptResponse::TPtr& response, const TActorContext& ctx) {
    if (request.exec_mode() != Ydb::Query::EXEC_MODE_EXECUTE) {
        return;
    }

    TStringStream stream;
    stream << "type: ExecuteSrcipt,";     
    stream << "status: " << response->Get()->Status << ",";

    stream << response->Get()->Issues;
    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, stream.Str());
}

// TODO: add logging for BulkUpsert

}
}