#include "kqp_log_query.h"
#include "kqp_worker_common.h"

#include <ydb/core/kqp/common/events/query.h>
#include <ydb/core/kqp/common/kqp_mask_literals.h>
#include <ydb/core/kqp/session_actor/kqp_query_state.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>

#include <library/cpp/json/writer/json.h>
#include <util/charset/utf8.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr::NKqp {
namespace {

constexpr size_t SQL_TEXT_MAX_SIZE = 20000;
constexpr TStringBuf SENSITIVE_QUERY_PLACEHOLDER =
    "[query may contain sensitive data, text is not shown]";

#define _KQP_REQ_LOG(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_REQUEST, "[REQ_JSON] " << stream)

struct TTableStat {
    TString Path;
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    ui64 WriteRows = 0;
    ui64 WriteBytes = 0;
};

struct TJsonExtra {
    TStringBuf Database;
    TStringBuf DatabaseId;
    TString Cluster;
    ui32 NodeId = 0;
    TString NodeName;
    TString QueryType;
    TString Action;
    TString ApplicationName;
    TStringBuf ClientAddress;
    TString CommandTag;
    ui64 ParametersSize = 0;
    // Completed-only fields
    TString Status;
    i64 DurationUs = -1;
    i64 CpuTimeUs = -1;
    i8 CompileCacheHit = -1; // -1 unknown, 0 miss, 1 hit
    // Execution stats (completed-only)
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    ui64 WriteRows = 0;
    ui64 WriteBytes = 0;
    ui64 AffectedShards = 0;
    ui64 ConsumedRu = 0;
    TVector<TTableStat> Tables;
};

void WriteJsonEntry(TStringBuf poolId, const TString& reqId, TStringBuf sessionId, TStringBuf userSID,
                    TStringBuf eventName, TStringBuf requestText,
                    const NYql::TIssues& issues,
                    const TJsonExtra& extra,
                    TInstant timestamp, TMaybe<TInstant> endTime = Nothing())
{
    bool truncated = false;
    TStringBuf loggedText = requestText;
    if (SQL_TEXT_MAX_SIZE > 0 && loggedText.size() > SQL_TEXT_MAX_SIZE) {
        loggedText = Utf8TruncateRobust(loggedText, SQL_TEXT_MAX_SIZE);
        if (loggedText.empty()) {
            loggedText = requestText.Head(SQL_TEXT_MAX_SIZE);
        }
        truncated = true;
    }

    TStringStream ss;
    NJsonWriter::TBuf json(NJsonWriter::HEM_RELAXED, &ss);

    json.BeginObject();
    json.WriteKey("timestamp").WriteString(timestamp.ToString());
    if (endTime) {
        json.WriteKey("end_time").WriteString(endTime->ToString());
    }
    json.WriteKey("req_id").WriteString(reqId);
    json.WriteKey("pool").WriteString(poolId);
    json.WriteKey("session").WriteString(sessionId);
    json.WriteKey("user").WriteString(userSID);
    json.WriteKey("event").WriteString(eventName);

    if (!loggedText.empty()) {
        json.WriteKey("query_text").WriteString(loggedText);
    }
    if (truncated) {
        json.WriteKey("query_text_truncated").WriteBool(true);
    }

    if (!issues.Empty()) {
        json.WriteKey("issues").WriteString(issues.ToOneLineString());
    }

    if (extra.Database) {
        json.WriteKey("database").WriteString(extra.Database);
    }
    if (extra.DatabaseId) {
        json.WriteKey("database_id").WriteString(extra.DatabaseId);
    }
    if (extra.Cluster) {
        json.WriteKey("cluster").WriteString(extra.Cluster);
    }
    if (extra.NodeId > 0) {
        json.WriteKey("node_id").WriteULongLong(extra.NodeId);
    }
    if (extra.NodeName) {
        json.WriteKey("node_name").WriteString(extra.NodeName);
    }
    if (extra.QueryType) {
        json.WriteKey("query_type").WriteString(extra.QueryType);
    }
    if (extra.Action) {
        json.WriteKey("action").WriteString(extra.Action);
    }
    if (extra.ApplicationName) {
        json.WriteKey("application").WriteString(extra.ApplicationName);
    }
    if (extra.ClientAddress) {
        json.WriteKey("client_address").WriteString(extra.ClientAddress);
    }
    if (extra.CommandTag) {
        json.WriteKey("command_tag").WriteString(extra.CommandTag);
    }
    if (extra.ParametersSize > 0) {
        json.WriteKey("parameters_size").WriteULongLong(extra.ParametersSize);
    }
    if (extra.Status) {
        json.WriteKey("status").WriteString(extra.Status);
    }
    if (extra.DurationUs >= 0) {
        json.WriteKey("duration_us").WriteLongLong(extra.DurationUs);
    }
    if (extra.CpuTimeUs >= 0) {
        json.WriteKey("cpu_time_us").WriteLongLong(extra.CpuTimeUs);
    }
    if (extra.CompileCacheHit >= 0) {
        json.WriteKey("compile_cache_hit").WriteBool(extra.CompileCacheHit == 1);
    }
    if (extra.ReadRows > 0) {
        json.WriteKey("read_rows").WriteULongLong(extra.ReadRows);
    }
    if (extra.ReadBytes > 0) {
        json.WriteKey("read_bytes").WriteULongLong(extra.ReadBytes);
    }
    if (extra.WriteRows > 0) {
        json.WriteKey("write_rows").WriteULongLong(extra.WriteRows);
    }
    if (extra.WriteBytes > 0) {
        json.WriteKey("write_bytes").WriteULongLong(extra.WriteBytes);
    }
    if (extra.AffectedShards > 0) {
        json.WriteKey("affected_shards").WriteULongLong(extra.AffectedShards);
    }
    if (extra.ConsumedRu > 0) {
        json.WriteKey("consumed_ru").WriteULongLong(extra.ConsumedRu);
    }
    if (!extra.Tables.empty()) {
        json.WriteKey("tables").BeginList();
        for (const auto& t : extra.Tables) {
            json.BeginObject();
            json.WriteKey("path").WriteString(t.Path);
            if (t.ReadRows > 0) {
                json.WriteKey("read_rows").WriteULongLong(t.ReadRows);
            }
            if (t.ReadBytes > 0) {
                json.WriteKey("read_bytes").WriteULongLong(t.ReadBytes);
            }
            if (t.WriteRows > 0) {
                json.WriteKey("write_rows").WriteULongLong(t.WriteRows);
            }
            if (t.WriteBytes > 0) {
                json.WriteKey("write_bytes").WriteULongLong(t.WriteBytes);
            }
            json.EndObject();
        }
        json.EndList();
    }

    json.EndObject();

    _KQP_REQ_LOG(ss.Str());
}

TString MakeRequestId(const TKqpQueryState& state) {
    auto res = TStringBuilder()
        << TActivationContext::Now().MicroSeconds() << "_";

    if (state.RequestEv) {
        res << (const void*)state.RequestEv->GetQuery().data();
    } else {
        res << (const void*)&state;
    }

    return res;
}

// A scripting query (QUERY_TYPE_SQL_SCRIPT / _SCRIPT_STREAMING) is forwarded from the
// session actor to KqpWorkerActor, which splits it into one or more AST sub-queries
// (QUERY_TYPE_AST_DML / _AST_SCAN) and re-enters the normal query pipeline — so LogStarted
// and LogCompleted get called again for each sub-query. We suppress those sub-query
// entries: the user-visible request is the original scripting query, and it is logged
// once by the session actor via LogForwardedCompleted. Without this, every scripting
// query would produce a burst of confusing internal AST log entries.
bool IsAstQueryType(NKikimrKqp::EQueryType type) {
    return type == NKikimrKqp::QUERY_TYPE_AST_DML
        || type == NKikimrKqp::QUERY_TYPE_AST_SCAN;
}

} // anonymous namespace

bool HasSensitiveSchemeOperation(const NKqpProto::TKqpPhyQuery& phyQuery) {
    for (const auto& tx : phyQuery.GetTransactions()) {
        if (!tx.HasSchemeOperation()) {
            continue;
        }
        const auto& op = tx.GetSchemeOperation();
        if (op.HasCreateUser() || op.HasAlterUser() ||
            op.HasCreateSecret() || op.HasAlterSecret()) {
            return true;
        }
        if (op.HasCreateObject() || op.HasUpsertObject() || op.HasAlterObject()) {
            if (op.GetObjectType() == "SECRET") {
                return true;
            }
        }
    }
    return false;
}

TString TLogQuery::LogStarted(const TKqpQueryState& state) {
    // See IsAstQueryType: these are internal sub-queries of a scripting request and
    // must not appear in the log. Returning an empty req_id also signals to the caller
    // that no "completed" entry should be written for this state.
    if (IsAstQueryType(state.GetType())) {
        return {};
    }

    TString reqId = MakeRequestId(state);

    TLogQuery log([&state, reqId]() {
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

        TStringBuf queryText = IsQueryAllowedToLog(query)
            ? TStringBuf(query)
            : SENSITIVE_QUERY_PLACEHOLDER;

        TJsonExtra extra;
        extra.Database = state.GetDatabase();
        if (state.UserRequestContext) {
            extra.DatabaseId = state.UserRequestContext->DatabaseId;
        }
        extra.Cluster = state.Cluster;
        extra.NodeId = TActivationContext::ActorSystem()->NodeId;
        extra.NodeName = AppData()->NodeName;
        extra.QueryType = NKikimrKqp::EQueryType_Name(state.GetType());
        extra.Action = NKikimrKqp::EQueryAction_Name(state.GetAction());
        extra.ApplicationName = state.ApplicationName.GetOrElse(TString{});
        extra.ClientAddress = state.ClientAddress;
        if (state.CommandTagName) {
            extra.CommandTag = *state.CommandTagName;
        }
        extra.ParametersSize = state.ParametersSize;

        WriteJsonEntry(
            poolId,
            reqId,
            sessionId,
            userSid,
            "started",
            queryText,
            {},
            extra,
            state.StartTime
        );
    });
    log.Log();
    return reqId;
}

void TLogQuery::LogCompleted(const TKqpQueryState& state,
                              const NKikimrKqp::TEvQueryResponse& record,
                              const TString& reqId) {
    // Mirror the suppression in LogStarted: AST sub-queries of a scripting request
    // must not produce "completed" entries either.
    if (IsAstQueryType(state.GetType())) {
        return;
    }

    TLogQuery log([&state, &record, &reqId]() {
        const auto now = TActivationContext::Now();

        TStringBuf sessionId = state.UserRequestContext
            ? TStringBuf(state.UserRequestContext->SessionId)
            : TStringBuf{};

        TString userSID = state.UserToken
            ? state.UserToken->GetUserSID()
            : TString{};

        auto query = state.ExtractQueryText();

        // Two masking paths depending on how far the query got:
        //  - PreparedQuery present: compilation succeeded, so we can inspect the physical
        //    query AST and precisely mask literals only when the query touches secrets/users.
        //  - No PreparedQuery (compile failure / early reject): fall back to the lexical
        //    heuristic IsQueryAllowedToLog and suppress the whole text if it looks sensitive.
        TString queryText;
        if (state.PreparedQuery) {
            if (HasSensitiveSchemeOperation(state.PreparedQuery->GetPhysicalQuery())) {
                queryText = MaskSensitiveLiterals(query);
            } else {
                queryText = query;
            }
        } else {
            queryText = IsQueryAllowedToLog(query)
                ? query
                : TString(SENSITIVE_QUERY_PLACEHOLDER);
        }

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

        TJsonExtra extra;
        extra.Database = state.GetDatabase();
        if (state.UserRequestContext) {
            extra.DatabaseId = state.UserRequestContext->DatabaseId;
        }
        extra.Cluster = state.Cluster;
        extra.NodeId = TActivationContext::ActorSystem()->NodeId;
        extra.NodeName = AppData()->NodeName;
        extra.QueryType = NKikimrKqp::EQueryType_Name(state.GetType());
        extra.Action = NKikimrKqp::EQueryAction_Name(state.GetAction());
        extra.ApplicationName = state.ApplicationName.GetOrElse(TString{});
        extra.ClientAddress = state.ClientAddress;
        if (state.CommandTagName) {
            extra.CommandTag = *state.CommandTagName;
        }
        extra.ParametersSize = state.ParametersSize;
        extra.Status = Ydb::StatusIds::StatusCode_Name(record.GetYdbStatus());
        extra.DurationUs = (now - state.StartTime).MicroSeconds();
        extra.CpuTimeUs = state.CpuTime.MicroSeconds();
        extra.CompileCacheHit = state.CompileStats.FromCache ? 1 : 0;
        extra.ConsumedRu = record.GetConsumedRu();

        // Collect per-table execution stats and aggregate totals
        THashMap<TString, TTableStat> tableMap;
        for (const auto& exec : state.QueryStats.Executions) {
            for (const auto& table : exec.GetTables()) {
                auto& ts = tableMap[table.GetTablePath()];
                ts.Path = table.GetTablePath();
                ts.ReadRows += table.GetReadRows();
                ts.ReadBytes += table.GetReadBytes();
                ts.WriteRows += table.GetWriteRows();
                ts.WriteBytes += table.GetWriteBytes();
                extra.ReadRows += table.GetReadRows();
                extra.ReadBytes += table.GetReadBytes();
                extra.WriteRows += table.GetWriteRows();
                extra.WriteBytes += table.GetWriteBytes();
                extra.AffectedShards += table.GetAffectedPartitions();
            }
        }
        extra.Tables.reserve(tableMap.size());
        for (auto& [_, ts] : tableMap) {
            extra.Tables.push_back(std::move(ts));
        }

        WriteJsonEntry(
            poolId,
            reqId,
            sessionId,
            userSID,
            "completed",
            queryText,
            issues,
            extra,
            state.StartTime,
            now
        );
    });
    log.Log();
}

void TLogQuery::LogForwardedCompleted(const TString& queryText,
                                       const TString& database,
                                       NKikimrKqp::EQueryType queryType,
                                       NKikimrKqp::EQueryAction queryAction,
                                       TInstant startTime,
                                       const NKikimrKqp::TEvQueryResponse& record,
                                       const TString& reqId) {
    TLogQuery log([&]() {
        const auto now = TActivationContext::Now();

        TJsonExtra extra;
        extra.Database = database;
        extra.NodeId = TActivationContext::ActorSystem()->NodeId;
        extra.NodeName = AppData()->NodeName;
        extra.QueryType = NKikimrKqp::EQueryType_Name(queryType);
        extra.Action = NKikimrKqp::EQueryAction_Name(queryAction);
        extra.Status = Ydb::StatusIds::StatusCode_Name(record.GetYdbStatus());
        extra.DurationUs = (now - startTime).MicroSeconds();

        NYql::TIssues issues;
        TStringBuf poolId;
        if (record.HasResponse()) {
            poolId = TStringBuf(record.GetResponse().GetEffectivePoolId());
            NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
        }

        TString loggedText = IsQueryAllowedToLog(queryText) ? queryText : TString(SENSITIVE_QUERY_PLACEHOLDER);

        WriteJsonEntry(poolId, reqId, {}, {}, "completed", loggedText, issues, extra,
                        startTime, now);
    });
    log.Log();
}

} // namespace NKikimr::NKqp
