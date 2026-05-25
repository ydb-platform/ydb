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

struct TBaseFields {
    TStringBuf Database;
    TStringBuf DatabaseId;
    TStringBuf TraceId;
    TStringBuf QueryId;
    TString Action;
    TString Type;
    ui64 QueryLen = 0;
    TInstant StartedAt;
};

struct TCompletedExtras {
    TString Status;
    ui64 DurationUs = 0;
    ui64 ResultsSize = 0;
    ui64 QueuedTimeUs = 0;
    bool HasCompileStats = false;
    bool CompileFromCache = false;
    ui64 CompileTimeUs = 0;
};

void WriteBaseFields(NJsonWriter::TBuf& json, const TBaseFields& base) {
    if (!base.Database.empty()) {
        json.WriteKey("database").WriteString(base.Database);
    }
    if (!base.DatabaseId.empty()) {
        json.WriteKey("database_id").WriteString(base.DatabaseId);
    }
    if (!base.TraceId.empty()) {
        json.WriteKey("trace_id").WriteString(base.TraceId);
    }
    if (!base.QueryId.empty()) {
        json.WriteKey("query_id").WriteString(base.QueryId);
    }
    if (!base.Action.empty()) {
        json.WriteKey("action").WriteString(TStringBuf(base.Action));
    }
    if (!base.Type.empty()) {
        json.WriteKey("type").WriteString(TStringBuf(base.Type));
    }
    json.WriteKey("query_len").WriteULongLong(base.QueryLen);
    if (base.StartedAt) {
        json.WriteKey("started_at_us").WriteULongLong(base.StartedAt.MicroSeconds());
    }
}

void WriteCompletedExtras(NJsonWriter::TBuf& json, const TCompletedExtras& extras) {
    if (!extras.Status.empty()) {
        json.WriteKey("status").WriteString(TStringBuf(extras.Status));
    }
    json.WriteKey("duration_us").WriteULongLong(extras.DurationUs);
    json.WriteKey("results_size").WriteULongLong(extras.ResultsSize);
    if (extras.QueuedTimeUs) {
        json.WriteKey("queued_time_us").WriteULongLong(extras.QueuedTimeUs);
    }
    if (extras.HasCompileStats) {
        json.WriteKey("compile_from_cache").WriteBool(extras.CompileFromCache);
        json.WriteKey("compile_time_us").WriteULongLong(extras.CompileTimeUs);
    }
}

void WriteJsonChunks(NActors::NLog::EPriority prio,
                     TStringBuf poolId,
                     TString reqId,
                     TStringBuf sessionId,
                     TStringBuf userSID,
                     TStringBuf eventName,
                     TStringBuf requestText,
                     const NYql::TIssues& issues,
                     const TBaseFields* base,
                     const TCompletedExtras* completedExtras,
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
        json.WriteKey("event").WriteString(eventName);

        if (!requestText.empty()) {
            json.WriteKey("data").WriteString(requestText.SubStr(i * SQL_TEXT_MAX_SIZE, SQL_TEXT_MAX_SIZE));
        }

        if (!issues.Empty()) {
            json.WriteKey("issues").WriteString(issues.ToOneLineString());
        }

        if (i == 0) {
            if (base) {
                WriteBaseFields(json, *base);
            }
            if (completedExtras) {
                WriteCompletedExtras(json, *completedExtras);
            }
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

void FillBaseFields(TBaseFields& base, const TKqpQueryState& state, ui64 queryLen, bool includeQueryId) {
    base.Database = state.Database;
    base.DatabaseId = state.UserRequestContext
        ? TStringBuf(state.UserRequestContext->DatabaseId)
        : TStringBuf{};
    base.TraceId = state.UserRequestContext
        ? TStringBuf(state.UserRequestContext->TraceId)
        : TStringBuf{};
    if (includeQueryId && state.CompileResult) {
        base.QueryId = state.CompileResult->Uid;
    }
    base.Action = NKikimrKqp::EQueryAction_Name(state.GetAction());
    base.Type = NKikimrKqp::EQueryType_Name(state.GetType());
    base.QueryLen = queryLen;
    base.StartedAt = state.StartTime;
}

} // anonymous namespace

TLogQuery TLogQuery::Started(const TKqpQueryState& state) {
    return TLogQuery([&state]() {
        if (!IsLogPriorityEnabled(NActors::NLog::PRI_DEBUG)) {
            return;
        }

        auto query = state.ExtractQueryText();
        if (IsUiExcludedQuery(query)) {
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

        TBaseFields base;
        FillBaseFields(base, state, query.size(), /*includeQueryId=*/false);

        const bool truncate = !IsLogPriorityEnabled(NActors::NLog::PRI_TRACE);

        WriteJsonChunks(
            NActors::NLog::PRI_DEBUG,
            poolId,
            GetRequestId(state),
            sessionId,
            userSid,
            "started",
            query,
            {},
            &base,
            nullptr,
            truncate
        );
    });
}

TLogQuery TLogQuery::Completed(const TKqpQueryState& state,
                               const NKikimrKqp::TEvQueryResponse& record,
                               ui64 responseByteSize) {
    return TLogQuery([&state, &record, responseByteSize]() {
        const auto status = record.GetYdbStatus();
        const auto prio = PickCompletedPriority(status);
        if (!IsLogPriorityEnabled(prio)) {
            return;
        }

        auto queryText = state.ExtractQueryText();
        if (IsUiExcludedQuery(queryText) && status == Ydb::StatusIds::SUCCESS) {
            return;
        }

        TStringBuf sessionId = state.UserRequestContext
            ? TStringBuf(state.UserRequestContext->SessionId)
            : TStringBuf{};

        TString userSID = state.UserToken
            ? state.UserToken->GetUserSID()
            : TString{};

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

        TBaseFields base;
        FillBaseFields(base, state, origQueryLen, /*includeQueryId=*/true);

        TCompletedExtras extras;
        extras.Status = TString{Ydb::StatusIds::StatusCode_Name(status)};
        extras.DurationUs = state.QueryStats.DurationUs;
        extras.ResultsSize = responseByteSize;
        extras.QueuedTimeUs = state.QueryStats.QueuedTimeUs;

        if (state.QueryStats.Compilation) {
            extras.HasCompileStats = true;
            extras.CompileFromCache = state.QueryStats.Compilation->FromCache;
            extras.CompileTimeUs = state.QueryStats.Compilation->DurationUs;
        }

        const bool truncate = !IsLogPriorityEnabled(NActors::NLog::PRI_TRACE);

        WriteJsonChunks(
            prio,
            poolId,
            GetRequestId(state),
            sessionId,
            userSID,
            "completed",
            queryText,
            issues,
            &base,
            &extras,
            truncate
        );
    });
}

} // namespace NKikimr::NKqp
