#include "kqp_log_query.h"

#include <ydb/core/kqp/common/events/query.h>
#include <ydb/core/kqp/session_actor/kqp_query_state.h>
#include <ydb/core/protos/kqp.pb.h>

#include <library/cpp/json/writer/json.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr::NKqp {
namespace {

constexpr size_t SQL_TEXT_MAX_SIZE = 4000;

#define _KQP_REQ_LOG(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_REQUEST, "[REQ_JSON] " << stream)

void WriteJsonChunks(TStringBuf poolId, TString reqId, TStringBuf sessionId, TStringBuf userSID,
                     TStringBuf eventName, TStringBuf requestText, 
                     const NYql::TIssues& issues) 
{
    const size_t total = requestText.empty() ? 1 :
        (requestText.size() + SQL_TEXT_MAX_SIZE - 1) / SQL_TEXT_MAX_SIZE;

    for (size_t i = 0; i < total; ++i) {
        TStringStream ss;
        NJsonWriter::TBuf json(NJsonWriter::HEM_RELAXED, &ss);

        json.BeginObject();
        json.WriteKey("req_id").WriteString(reqId);
        json.WriteKey("pool").WriteString(poolId);
        json.WriteKey("session").WriteString(sessionId);
        json.WriteKey("user").WriteString(userSID);
        json.WriteKey("part").WriteInt(i + 1);
        json.WriteKey("total").WriteInt(total);
            
        json.WriteKey("request").BeginObject();
        json.WriteKey("event").WriteString(eventName);
        
        if (!requestText.empty()) {
            json.WriteKey("data").WriteString(requestText.SubStr(i * SQL_TEXT_MAX_SIZE, SQL_TEXT_MAX_SIZE));
        }

        if (!issues.Empty()) {
            json.WriteKey("issues").WriteString(issues.ToOneLineString());
        }
        
        json.EndObject();
        json.EndObject();

        _KQP_REQ_LOG(ss.Str());
    }
}

TString GetRequestId(const TKqpQueryState& state) {
    auto res = TStringBuilder() 
        << TActivationContext::Now().MicroSeconds() << "_";

    if (state.RequestEv) {
        res << (const void*)state.RequestEv->GetQuery().data();
    } else {
        res << (const void*)&state;
    }
    
    return res;
}

} // anonymous namespace

TLogQuery TLogQuery::Started(const TKqpQueryState& state) {
    return TLogQuery([&state]() {
        TStringBuf poolId = state.UserRequestContext 
            ? TStringBuf(state.UserRequestContext->PoolId)
            : TStringBuf{};

        TStringBuf sessionId = state.UserRequestContext
            ? TStringBuf(state.UserRequestContext->SessionId)
            : TStringBuf{};

        TString userSid = state.UserToken 
            ? state.UserToken->GetUserSID()
            : TString{};

        auto query = state.ExtractQueryText();

        WriteJsonChunks(
            poolId,
            GetRequestId(state),
            sessionId,
            userSid,
            "started",
            query,
            {}
        );
    });
}

TLogQuery TLogQuery::Completed(const TKqpQueryState& state,
                               const NKikimrKqp::TEvQueryResponse& record) {
    return TLogQuery([&state, &record]() {
        TStringBuf sessionId = state.UserRequestContext
            ? TStringBuf(state.UserRequestContext->SessionId)
            : TStringBuf{};

        TString userSID = state.UserToken
            ? state.UserToken->GetUserSID()
            : TString{};

        auto queryText = state.ExtractQueryText();

        NYql::TIssues issues;
        TStringBuf poolId;

        if (record.HasResponse()) {
            poolId = TStringBuf(record.GetResponse().GetEffectivePoolId());
            NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
        } else {
            poolId = state.UserRequestContext
                ? TStringBuf(state.UserRequestContext->PoolId)
                : TStringBuf{};
        }

        WriteJsonChunks(
            poolId,
            GetRequestId(state),
            sessionId,
            userSID,
            "completed",
            queryText,
            issues
        );
    });
}

} // namespace NKikimr::NKqp
