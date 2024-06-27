#pragma once

namespace NKikimr {
namespace NDataIntegrity {

inline void LogIntegrityTrails(const NKqp::TEvKqp::TEvQueryRequest::TPtr& request, const TActorContext& ctx) {
    
    TStringStream stream;
    stream << "session_id: " << request->Get()->GetSessionId() << ",";
    // TODO: log request info

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, stream.Str());
}

inline void LogIntegrityTrails(const NKqp::TEvKqp::TEvQueryRequest::TPtr& /*request*/, const NKqp::TEvKqp::TEvQueryResponse::TPtr& response, const TActorContext& ctx) {
    auto& record = response->Get()->Record.GetRef();

    TStringStream stream;
    stream << "session_id: " << record.GetResponse().GetSessionId() << ",";
    stream << "user_tx_id: " << (record.GetResponse().HasTxMeta() ? record.GetResponse().GetTxMeta().id() : "empty") << ",";
    stream << "status: " << record.GetYdbStatus() << ",";
    stream << record.GetResponse().GetQueryIssues();

    // TODO: log request info if needed

    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, stream.Str());
}

}
}    