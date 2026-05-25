#include "kqp_log_query.h"

#include <ydb/core/kqp/common/events/query.h>
#include <ydb/core/kqp/session_actor/kqp_query_state.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/json/writer/json.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr::NKqp {
namespace {

constexpr size_t SQL_TEXT_MAX_SIZE = 4000;

// Emit at a runtime-chosen priority. Started + successful Completed go at
// TRACE (operator opts in by raising KQP_REQUEST). Failed Completed goes at
// WARN, so that KQP_REQUEST=WARN gives only failures and KQP_REQUEST=TRACE
// gives the full picture.
#define _KQP_REQ_LOG_AT(prio, stream) \
    LOG_LOG_S(*TlsActivationContext, (prio), NKikimrServices::KQP_REQUEST, "[REQ_JSON] " << stream)

struct TCompletedFields {
    TStringBuf Database;
    TStringBuf DatabaseId;
    TStringBuf TraceId;
    TStringBuf Action;
    TStringBuf Type;
    TStringBuf Status;
    ui64 DurationUs = 0;
    ui64 ParametersSize = 0;
    ui64 ResultsSize = 0;
    ui64 QueryLen = 0;
};

void WriteJsonChunks(NActors::NLog::EPriority prio,
                     TStringBuf poolId, TString reqId, TStringBuf sessionId, TStringBuf userSID,
                     TStringBuf eventName, TStringBuf requestText,
                     const NYql::TIssues& issues,
                     const TCompletedFields* extra)
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

        // Additive completed-only fields. Kept inside `request` to keep the
        // envelope shape stable for existing consumers; only emitted in the
        // first chunk so they are not duplicated for chunked SQL.
        if (extra && i == 0) {
            if (!extra->Database.empty()) {
                json.WriteKey("database").WriteString(extra->Database);
            }
            if (!extra->DatabaseId.empty()) {
                json.WriteKey("database_id").WriteString(extra->DatabaseId);
            }
            if (!extra->TraceId.empty()) {
                json.WriteKey("trace_id").WriteString(extra->TraceId);
            }
            if (!extra->Action.empty()) {
                json.WriteKey("action").WriteString(extra->Action);
            }
            if (!extra->Type.empty()) {
                json.WriteKey("type").WriteString(extra->Type);
            }
            if (!extra->Status.empty()) {
                json.WriteKey("status").WriteString(extra->Status);
            }
            json.WriteKey("duration_us").WriteULongLong(extra->DurationUs);
            json.WriteKey("parameters_size").WriteULongLong(extra->ParametersSize);
            json.WriteKey("results_size").WriteULongLong(extra->ResultsSize);
            json.WriteKey("query_len").WriteULongLong(extra->QueryLen);
        }

        json.EndObject();
        json.EndObject();

        _KQP_REQ_LOG_AT(prio, ss.Str());
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

bool IsLogPriorityEnabled(NActors::NLog::EPriority prio) {
    return IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, prio, NKikimrServices::KQP_REQUEST, 0ull);
}

NActors::NLog::EPriority PickCompletedPriority(Ydb::StatusIds::StatusCode status) {
    return status == Ydb::StatusIds::SUCCESS
        ? NActors::NLog::PRI_TRACE
        : NActors::NLog::PRI_WARN;
}

ui64 ComputeResultsSize(const NKikimrKqp::TEvQueryResponse& record) {
    if (!record.HasResponse()) {
        return 0;
    }
    ui64 size = 0;
    for (const auto& result : record.GetResponse().GetYdbResults()) {
        size += result.ByteSize();
    }
    return size;
}

} // anonymous namespace

TLogQuery TLogQuery::Started(const TKqpQueryState& state) {
    return TLogQuery([&state]() {
        // Re-check inside the lambda: KQP_REQ_LOG only guarantees WARN+ is
        // enabled (because of failure-completed); started is TRACE-only.
        if (!IsLogPriorityEnabled(NActors::NLog::PRI_TRACE)) {
            return;
        }

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
            NActors::NLog::PRI_TRACE,
            poolId,
            GetRequestId(state),
            sessionId,
            userSid,
            "started",
            query,
            {},
            /*extra=*/nullptr
        );
    });
}

TLogQuery TLogQuery::Completed(const TKqpQueryState& state,
                               const NKikimrKqp::TEvQueryResponse& record) {
    return TLogQuery([&state, &record]() {
        const auto status = record.GetYdbStatus();
        const auto prio = PickCompletedPriority(status);
        if (!IsLogPriorityEnabled(prio)) {
            return;
        }

        TStringBuf sessionId = state.UserRequestContext
            ? TStringBuf(state.UserRequestContext->SessionId)
            : TStringBuf{};

        TString userSID = state.UserToken
            ? state.UserToken->GetUserSID()
            : TString{};

        auto queryText = state.ExtractQueryText();
        const ui64 origQueryLen = queryText.size();

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

        TCompletedFields extra;
        extra.Database = state.GetDatabase();
        extra.DatabaseId = state.UserRequestContext
            ? TStringBuf(state.UserRequestContext->DatabaseId)
            : TStringBuf{};
        extra.TraceId = state.UserRequestContext
            ? TStringBuf(state.UserRequestContext->TraceId)
            : TStringBuf{};
        extra.Action = NKikimrKqp::EQueryAction_Name(state.GetAction());
        extra.Type = NKikimrKqp::EQueryType_Name(state.GetType());
        extra.Status = Ydb::StatusIds::StatusCode_Name(status);
        extra.DurationUs = state.QueryStats.DurationUs;
        extra.ParametersSize = state.ParametersSize;
        extra.ResultsSize = ComputeResultsSize(record);
        extra.QueryLen = origQueryLen;

        WriteJsonChunks(
            prio,
            poolId,
            GetRequestId(state),
            sessionId,
            userSID,
            "completed",
            queryText,
            issues,
            &extra
        );
    });
}

} // namespace NKikimr::NKqp
