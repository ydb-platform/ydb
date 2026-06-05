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
constexpr size_t QUERY_TEXT_LIMIT = 10240;
constexpr TStringBuf UI_QUERY_EXCLUDE_MARKER = "/*UI-QUERY-EXCLUDE*/";

#define _KQP_REQ_LOG_AT(prio, stream) \
    LOG_LOG_S(*TlsActivationContext, (prio), NKikimrServices::KQP_REQUEST, "[REQ_JSON] " << stream)

bool IsUiExcludedQuery(TStringBuf queryText) {
    return queryText.StartsWith(UI_QUERY_EXCLUDE_MARKER);
}

TString SafeExtractQueryText(const TKqpQueryState& state) {
    if (state.CompileResult) {
        if (state.CompileResult->Query) {
            return state.CompileResult->Query->Text;
        }
        return {};
    }
    if (state.RequestEv) {
        return state.RequestEv->GetQuery();
    }
    return {};
}

struct TCompletedFields {
    TStringBuf Database;
    TStringBuf DatabaseId;
    TStringBuf TraceId;
    TStringBuf QueryId;
    TString Action;
    TString Type;
    ui64 QueryLen = 0;
    TInstant StartedAt;
    TString Status;
    ui64 DurationUs = 0;
    ui64 ResultsSize = 0;
    ui64 QueuedTimeUs = 0;
    bool HasCompileStats = false;
    bool CompileFromCache = false;
    ui64 CompileTimeUs = 0;
};

void WriteCompletedFields(NJsonWriter::TBuf& json, const TCompletedFields& f) {
    if (!f.Database.empty()) {
        json.WriteKey("database").WriteString(f.Database);
    }
    if (!f.DatabaseId.empty()) {
        json.WriteKey("database_id").WriteString(f.DatabaseId);
    }
    if (!f.TraceId.empty()) {
        json.WriteKey("trace_id").WriteString(f.TraceId);
    }
    if (!f.QueryId.empty()) {
        json.WriteKey("query_id").WriteString(f.QueryId);
    }
    if (!f.Action.empty()) {
        json.WriteKey("action").WriteString(TStringBuf(f.Action));
    }
    if (!f.Type.empty()) {
        json.WriteKey("type").WriteString(TStringBuf(f.Type));
    }
    json.WriteKey("query_len").WriteULongLong(f.QueryLen);
    if (f.StartedAt) {
        json.WriteKey("started_at_us").WriteULongLong(f.StartedAt.MicroSeconds());
    }
    if (!f.Status.empty()) {
        json.WriteKey("status").WriteString(TStringBuf(f.Status));
    }
    json.WriteKey("duration_us").WriteULongLong(f.DurationUs);
    json.WriteKey("results_size").WriteULongLong(f.ResultsSize);
    if (f.QueuedTimeUs) {
        json.WriteKey("queued_time_us").WriteULongLong(f.QueuedTimeUs);
    }
    if (f.HasCompileStats) {
        json.WriteKey("compile_from_cache").WriteBool(f.CompileFromCache);
        json.WriteKey("compile_time_us").WriteULongLong(f.CompileTimeUs);
    }
}

void WriteJsonChunks(NActors::NLog::EPriority prio,
                     TStringBuf poolId,
                     TString reqId,
                     TStringBuf sessionId,
                     TStringBuf userSID,
                     TStringBuf requestText,
                     const NYql::TIssues& issues,
                     const TCompletedFields& fields,
                     bool truncateText)
{
    bool wasTruncated = false;
    if (truncateText && requestText.size() > QUERY_TEXT_LIMIT) {
        requestText = requestText.SubStr(0, QUERY_TEXT_LIMIT);
        wasTruncated = true;
    }

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
        json.WriteKey("event").WriteString("completed");

        if (!requestText.empty()) {
            json.WriteKey("data").WriteString(requestText.SubStr(i * SQL_TEXT_MAX_SIZE, SQL_TEXT_MAX_SIZE));
        }

        if (!issues.Empty()) {
            json.WriteKey("issues").WriteString(issues.ToOneLineString());
        }

        if (i == 0) {
            WriteCompletedFields(json, fields);
            if (wasTruncated) {
                json.WriteKey("data_truncated").WriteBool(true);
            }
        }

        json.EndObject();
        json.EndObject();

        _KQP_REQ_LOG_AT(prio, ss.Str());
    }
}

TString GetRequestId(const TKqpQueryState& state) {
    return ToString(state.ProxyRequestId);
}

bool IsLogPriorityEnabled(NActors::NLog::EPriority prio) {
    return IS_CTX_LOG_PRIORITY_ENABLED(*TlsActivationContext, prio, NKikimrServices::KQP_REQUEST, 0ull);
}

NActors::NLog::EPriority PickCompletedPriority(Ydb::StatusIds::StatusCode status) {
    return status == Ydb::StatusIds::SUCCESS
        ? NActors::NLog::PRI_DEBUG
        : NActors::NLog::PRI_WARN;
}

} // anonymous namespace

TLogQuery TLogQuery::Completed(const TKqpQueryState& state,
                               const NKikimrKqp::TEvQueryResponse& record,
                               ui64 responseByteSize) {
    const auto status = record.GetYdbStatus();
    const auto prio = PickCompletedPriority(status);
    if (!IsLogPriorityEnabled(prio)) {
        return {};
    }

    return TLogQuery([&state, &record, responseByteSize, status, prio]() {
        const TString queryText = SafeExtractQueryText(state);
        if (IsUiExcludedQuery(queryText) && status == Ydb::StatusIds::SUCCESS) {
            return;
        }

        const auto* userCtx = state.UserRequestContext.Get();
        TStringBuf sessionId = userCtx ? TStringBuf(userCtx->SessionId) : TStringBuf{};

        TString userSID;
        if (state.UserToken) {
            userSID = state.UserToken->GetUserSID();
        }

        const ui64 origQueryLen = queryText.size();

        NYql::TIssues issues;
        TStringBuf poolId;

        if (record.HasResponse()) {
            poolId = TStringBuf(record.GetResponse().GetEffectivePoolId());
            NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
        } else if (userCtx) {
            poolId = TStringBuf(userCtx->PoolId);
        }

        TCompletedFields fields;
        fields.Database = state.Database;
        if (userCtx) {
            fields.DatabaseId = userCtx->DatabaseId;
            fields.TraceId = userCtx->TraceId;
        }
        if (state.CompileResult && !state.CompileResult->Uid.empty()) {
            fields.QueryId = state.CompileResult->Uid;
        }
        fields.Action = TString{NKikimrKqp::EQueryAction_Name(state.GetAction())};
        fields.Type = TString{NKikimrKqp::EQueryType_Name(state.GetType())};
        fields.QueryLen = origQueryLen;
        fields.StartedAt = state.StartTime;
        fields.Status = TString{Ydb::StatusIds::StatusCode_Name(status)};
        fields.DurationUs = state.QueryStats.DurationUs;
        fields.ResultsSize = responseByteSize;
        fields.QueuedTimeUs = state.QueryStats.QueuedTimeUs;

        if (state.QueryStats.Compilation) {
            fields.HasCompileStats = true;
            fields.CompileFromCache = state.QueryStats.Compilation->FromCache;
            fields.CompileTimeUs = state.QueryStats.Compilation->DurationUs;
        }

        const bool truncate = !IsLogPriorityEnabled(NActors::NLog::PRI_TRACE);

        WriteJsonChunks(
            prio,
            poolId,
            GetRequestId(state),
            sessionId,
            userSID,
            queryText,
            issues,
            fields,
            truncate
        );
    });
}

} // namespace NKikimr::NKqp
