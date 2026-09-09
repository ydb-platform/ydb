#include "kqp_query_tracing.h"
#include "kqp_trace_settings.h"

#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/core/kqp/common/simple/query_stats.h>
#include <ydb/library/security/util.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <google/protobuf/any.pb.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp {

NWilson::TSpan MakeQueryPhaseTraceSpan(ui8 verbosity, NWilson::TTraceId parent,
        EQueryTracePhase phase, NWilson::TFlags flags, NActors::TActorSystem* actorSystem) {
    struct TDescription {
        const char* Name;
        const char* Phase;
        const char* Actor;
        const char* Component = nullptr;
        const char* Peer = nullptr;
    };
    const auto description = [phase]() -> TDescription {
        switch (phase) {
            case EQueryTracePhase::Admission:
                return {"Queued", "Admission", "TKqpSessionActor", nullptr, "WorkloadService"};
            case EQueryTracePhase::ResolveTables:
                return {"Resolve tables", "ResolveTables", "TKqpTableResolver", "KqpExecuter.Prepare"};
            case EQueryTracePhase::ResolveShards:
                return {"Locate shards", "ResolveShards", "TKqpShardsResolver", "KqpExecuter.Prepare"};
            case EQueryTracePhase::ResolveMetadata:
                return {"Metadata", "ResolveMetadata", "TKqpTableResolver", "KqpExecuter.Prepare", "SchemeCache"};
            case EQueryTracePhase::ResolvePartitioning:
                return {"Partitioning", "ResolvePartitioning", "TKqpTableResolver", "KqpExecuter.Prepare", "SchemeCache"};
            case EQueryTracePhase::Snapshot:
                return {"Acquire snapshot", "Snapshot", "TKqpDataExecuter", "KqpExecuter.Prepare", "TLongTxService"};
            case EQueryTracePhase::SessionSnapshot:
                return {"Acquire snapshot", "Snapshot", "TKqpSessionActor", nullptr, "TSnapshotManagerActor"};
            case EQueryTracePhase::PersistentSnapshot:
                return {"Acquire persistent snapshot", "Snapshot", "TKqpSessionActor", nullptr, "TSnapshotManagerActor"};
            case EQueryTracePhase::RunTasks:
                return {"Run tasks", "RunTasks", nullptr, "DqExecution"};
            case EQueryTracePhase::BufferLookup:
                return {"Check rows", "BufferLookup", "TKqpBufferLookupActor", "KqpBufferLookup", "DataShard"};
            case EQueryTracePhase::Write:
                return {"Write", "Write", "TKqpBufferWriteActor"};
            case EQueryTracePhase::WaitForWrites:
                return {"Wait for writes", "WaitForWrites", "TKqpBufferWriteActor"};
            case EQueryTracePhase::FlushEffects:
                return {"Flush effects", "FlushEffects", "TKqpBufferWriteActor"};
            case EQueryTracePhase::Commit:
                return {"Commit", "Commit", "TKqpBufferWriteActor"};
            case EQueryTracePhase::CommitPrepareShards:
                return {"Prepare shards", "CommitPrepareShards", "TKqpBufferWriteActor", nullptr, "DataShard"};
            case EQueryTracePhase::CommitApplyShards:
                return {"Apply commit", "CommitApplyShards", "TKqpBufferWriteActor", nullptr, "DataShard"};
            case EQueryTracePhase::CommitCoordinator:
                return {"Coordinator", "CommitCoordinator", "TKqpBufferWriteActor", nullptr, "TxCoordinator"};
            case EQueryTracePhase::Rollback:
                return {"Rollback", "Rollback", "TKqpBufferWriteActor"};
        }
        Y_UNREACHABLE();
    }();
    NWilson::TSpan span(verbosity, std::move(parent), description.Name, flags, actorSystem);
    if (!span) {
        return span;
    }
    span.Attribute("ydb.phase", TString(description.Phase));
    if (description.Actor) {
        span.Attribute("ydb.actor.type", TString(description.Actor));
    }
    if (description.Component) {
        span.Attribute("ydb.code.component", TString(description.Component));
    }
    if (description.Peer) {
        span.Attribute("ydb.peer.actor.type", TString(description.Peer));
    }
    return span;
}

namespace {

template<class TStats>
void AddQueryExecutionAttributes(NWilson::TSpan& span, const TQueryTraceDescription& description,
        const TStats& stats, ui64 requestUnits, Ydb::StatusIds::StatusCode status) {
    span.Attribute("db.query.summary", description.DisplayName);
    span.Attribute("db.operation.name", description.Operation);
    ui64 cpuUs = 0;
    ui64 rowsRead = 0;
    ui64 bytesRead = 0;
    ui64 rowsWritten = 0;
    ui64 waitUs = 0;
    ui64 spilledBytes = 0;
    double maxTaskSkew = 0;
    bool taskStatsIncomplete = false;
    for (const auto& execution : stats.GetExecutions()) {
        cpuUs += GetExecutionTraceCpuTimeUs(execution);
        NKqpProto::TKqpExecutionExtraStats extra;
        if (execution.GetExtra().UnpackTo(&extra)) {
            waitUs += extra.GetWaitTimeUs();
            spilledBytes += extra.GetSpilledBytes();
            maxTaskSkew = Max(maxTaskSkew, extra.GetMaxTaskSkew());
            taskStatsIncomplete |= extra.GetTaskStatsIncomplete();
        }
        for (const auto& table : execution.GetTables()) {
            rowsRead += table.GetReadRows();
            bytesRead += table.GetReadBytes();
            rowsWritten += table.GetWriteRows() + table.GetEraseRows();
        }
    }
    span.Attribute("ydb.cpu_us", static_cast<i64>(cpuUs));
    span.Attribute("ydb.session.cpu_us", static_cast<i64>(stats.GetWorkerCpuTimeUs()));
    span.Attribute("ydb.rows_read", static_cast<i64>(rowsRead));
    span.Attribute("ydb.bytes_read", static_cast<i64>(bytesRead));
    span.Attribute("ydb.rows_written", static_cast<i64>(rowsWritten));
    if (span.GetTraceId().GetVerbosity() >= TComponentTracingLevels::TQueryProcessor::Basic) {
        span.Attribute("ydb.wait_us", static_cast<i64>(waitUs));
        span.Attribute("ydb.spilled_bytes", static_cast<i64>(spilledBytes));
        span.Attribute("ydb.max_task_skew", maxTaskSkew);
        span.Attribute("ydb.task_stats_incomplete", taskStatsIncomplete);
    }
    span.Attribute("ydb.consumed_ru", static_cast<i64>(requestUnits));
    span.Attribute("ydb.status_code", Ydb::StatusIds::StatusCode_Name(status));
}

void AddQueryCompilationAttributes(NWilson::TSpan& span, bool fromCache, ui64 cpuUs, ui64 durationUs) {
    span.Attribute("ydb.compile.cpu_us", static_cast<i64>(cpuUs));
    span.Attribute("ydb.compile.cache_hit", fromCache);
    span.Attribute("ydb.compile.duration_us", static_cast<i64>(durationUs));
}

} // namespace

void AddQueryResultAttributes(NWilson::TSpan& span, const TQueryTraceDescription& description,
        const TKqpQueryStats& stats, ui64 requestUnits, Ydb::StatusIds::StatusCode status) {
    if (!span) {
        return;
    }
    AddQueryExecutionAttributes(span, description, stats, requestUnits, status);
    if (stats.Compilation) {
        AddQueryCompilationAttributes(span, stats.Compilation->FromCache,
            stats.Compilation->CpuTimeUs, stats.Compilation->DurationUs);
    }
    span.Attribute("ydb.locks_broken_as_victim", static_cast<i64>(stats.LocksBrokenAsVictim));
    span.Attribute("ydb.locks_broken_as_breaker", static_cast<i64>(stats.LocksBrokenAsBreaker));
}

void AddWorkerQueryResultAttributes(NWilson::TSpan& span, const TQueryTraceDescription& description,
        const NKikimrKqp::TEvQueryResponse& response, const NKqpProto::TKqpStatsQuery* workerStats) {
    if (span) {
        const auto& stats = workerStats ? *workerStats : response.GetResponse().GetQueryStats();
        AddQueryExecutionAttributes(span, description, stats, response.GetConsumedRu(), response.GetYdbStatus());
        if (stats.HasCompilation()) {
            const auto& compilation = stats.GetCompilation();
            AddQueryCompilationAttributes(span, compilation.GetFromCache(), compilation.GetCpuTimeUs(), compilation.GetDurationUs());
        }
    }
}

ui64 GetExecutionTraceCpuTimeUs(const NYql::NDqProto::TDqExecutionStats& stats) {
    NKqpProto::TKqpExecutionExtraStats extra;
    return stats.GetExtra().UnpackTo(&extra) && extra.HasCpuTimeUs()
        ? extra.GetCpuTimeUs() : stats.GetCpuTimeUs();
}

void AddReadTraceStats(NWilson::TSpan& span, NYql::NDqProto::TDqTaskStats& stats,
        const TString& table, ui64 rows, ui64 retries) {
    if (span) {
        span.Attribute("db.collection.name", table);
        span.Attribute("ydb.rows", static_cast<i64>(rows));
        span.Attribute("ydb.read_retries", static_cast<i64>(retries));
    }
    if (span.GetTraceId() && retries) {
        NKqpProto::TKqpTaskExtraStats extra;
        stats.GetExtra().UnpackTo(&extra);
        extra.SetReadRetriesCount(extra.GetReadRetriesCount() + retries);
        stats.MutableExtra()->PackFrom(extra);
    }
}

void AddKqpTaskTraceAttributes(NWilson::TSpan& span, const NYql::NDqProto::TDqComputeActorStats& stats) {
    if (span && stats.TasksSize() == 1) {
        NKqpProto::TKqpTaskExtraStats extra;
        if (stats.GetTasks(0).GetExtra().UnpackTo(&extra)) {
            span.Attribute("ydb.read_retries", static_cast<i64>(extra.GetReadRetriesCount()));
        }
    }
}

NWilson::TSpan MakeMetadataTraceSpan(const NWilson::TTraceId& parent, NActors::TActorSystem* actorSystem,
        const TString& name, const TString& table, const char* purpose) {
    NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Detailed,
        NWilson::TTraceId(parent), name, NWilson::EFlags::NONE, actorSystem);
    span.Attribute("db.collection.name", table);
    span.Attribute("ydb.actor.type", TString("TActorRequestHandler"));
    span.Attribute("ydb.code.component", TString("KqpTableMetadataLoader"));
    span.Attribute("ydb.peer.actor.type", TString(name == "Load metadata" ? "SchemeCache" : "StatisticsService"));
    span.Attribute("ydb.compile_dependency.purpose", TString(purpose));
    return span;
}

void MarkJoinedCompilation(NWilson::TSpan& waiter, const NWilson::TSpan& compilation) {
    if (!waiter) {
        return;
    }
    waiter.Attribute("ydb.trace.coverage", TString("joined_in_progress"));
    if (compilation) {
        waiter.Link(compilation.GetTraceId());
    }
}

bool TShardTraceEvents::Retain(const NWilson::TSpan& span, bool last) {
    if (!span || span.GetTraceId().GetVerbosity() < TComponentTracingLevels::TQueryProcessor::Diagnostic) {
        return false;
    }
    if (Count >= NQueryTraceSettings::MaxShardEvents
            || (Count >= NQueryTraceSettings::MaxShardEvents - 1 && !last)) {
        ++Dropped;
        return false;
    }
    ++Count;
    return true;
}

void TShardTraceEvents::Acknowledge(NWilson::TSpan& span, ui64 shardId, bool last) {
    if (Retain(span, last)) {
        span.Event("Shard acknowledged", {
            {"ydb.shard_id", static_cast<i64>(shardId)},
            {"ydb.last_shard", last},
        });
    }
}

void TShardTraceEvents::Finish(NWilson::TSpan& span) {
    if (span && Dropped) {
        span.Attribute("ydb.shard_events_dropped", static_cast<i64>(Dropped));
    }
    Count = 0;
    Dropped = 0;
}

void TCommitTracePhase::Start(const NWilson::TSpan& parent, EQueryTracePhase phase, ui64 shards) {
    End(Ydb::StatusIds::SUCCESS);
    Span = MakeQueryPhaseTraceSpan(TComponentTracingLevels::TQueryProcessor::Detailed,
        parent.GetTraceId(), phase, NWilson::EFlags::AUTO_END, parent.GetActorSystem());
    Span.Attribute("ydb.shards", static_cast<i64>(shards));
}

void TCommitTracePhase::Acknowledge(ui64 shardId, bool last) {
    Events.Acknowledge(Span, shardId, last);
}

void TCommitTracePhase::End(Ydb::StatusIds::StatusCode status) {
    Events.Finish(Span);
    EndQueryTraceSpan(Span, status);
}

namespace {

const char* GetTableSinkModeVerb(NKikimrKqp::TKqpTableSinkSettings::EType mode) {
    switch (mode) {
        case NKikimrKqp::TKqpTableSinkSettings::MODE_FILL:             return "FILL";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE:          return "REPLACE";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT:           return "UPSERT";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT_INCREMENT: return "UPSERT INCREMENT";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT:           return "INSERT";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE:           return "DELETE";
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE:           return "UPDATE";
        default:                                                       return nullptr;
    }
}

TQueryTraceDescription DescribePhysicalQuery(const NKqpProto::TKqpPhyQuery& query,
        const TMaybe<TString>& commandTag) {
    TString inferredWriteVerb;
    TString writeTable;
    TString readTable;
    bool hasReads = false;
    bool multiWrite = false;
    bool multiRead = false;
    bool mixedWriteVerbs = false;
    auto noteWriteVerb = [&](TStringBuf verb) {
        mixedWriteVerbs = mixedWriteVerbs || (inferredWriteVerb && inferredWriteVerb != verb);
        if (!inferredWriteVerb) {
            inferredWriteVerb = verb;
        }
    };
    auto noteTable = [](TString& table, bool& multi, const TString& path) {
        if (path) {
            multi = multi || (table && table != path);
            table = table ? table : path;
        }
    };
    auto noteTableSink = [&](const NKqpProto::TKqpInternalSink& sink) {
        if (!sink.GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
            return;
        }
        NKikimrKqp::TKqpTableSinkSettings settings;
        if (sink.GetSettings().UnpackTo(&settings)) {
            if (const char* verb = GetTableSinkModeVerb(settings.GetType())) {
                noteWriteVerb(verb);
                noteTable(writeTable, multiWrite, settings.GetTable().GetPath());
            }
        }
    };
    for (const auto& tx : query.GetTransactions()) {
        if (tx.GetType() == NKqpProto::TKqpPhyTx::TYPE_SCHEME) {
            return {"DDL", "DDL"};
        }
        for (const auto& stage : tx.GetStages()) {
            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink) {
                    noteTableSink(sink.GetInternalSink());
                }
            }
            for (const auto& transform : stage.GetOutputTransforms()) {
                if (transform.GetTypeCase() == NKqpProto::TKqpOutputTransform::kInternalSink) {
                    noteTableSink(transform.GetInternalSink());
                }
            }
            for (const auto& op : stage.GetTableOps()) {
                switch (op.GetTypeCase()) {
                    case NKqpProto::TKqpPhyTableOperation::kUpsertRows:
                        noteWriteVerb("UPSERT");
                        noteTable(writeTable, multiWrite, op.GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyTableOperation::kDeleteRows:
                        noteWriteVerb("DELETE");
                        noteTable(writeTable, multiWrite, op.GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyTableOperation::kReadRange:
                    case NKqpProto::TKqpPhyTableOperation::kReadOlapRange:
                    case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                        hasReads = true;
                        noteTable(readTable, multiRead, op.GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyTableOperation::TYPE_NOT_SET:
                        break;
                }
            }
            for (const auto& source : stage.GetSources()) {
                hasReads = true;
                switch (source.GetTypeCase()) {
                    case NKqpProto::TKqpSource::kReadRangesSource:
                        noteTable(readTable, multiRead, source.GetReadRangesSource().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpSource::kFullTextSource:
                        noteTable(readTable, multiRead, source.GetFullTextSource().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpSource::kSysViewSource:
                        noteTable(readTable, multiRead, source.GetSysViewSource().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpSource::kExternalSource:
                    case NKqpProto::TKqpSource::TYPE_NOT_SET:
                        break;
                }
            }
            for (const auto& input : stage.GetInputs()) {
                switch (input.GetTypeCase()) {
                    case NKqpProto::TKqpPhyConnection::kStreamLookup:
                        hasReads = true;
                        noteTable(readTable, multiRead, input.GetStreamLookup().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyConnection::kVectorResolve:
                        hasReads = true;
                        noteTable(readTable, multiRead, input.GetVectorResolve().GetTable().GetPath());
                        break;
                    case NKqpProto::TKqpPhyConnection::kVectorSearch:
                        hasReads = true;
                        noteTable(readTable, multiRead, input.GetVectorSearch().GetTable().GetPath());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    TString operation = commandTag.GetOrElse(TString{});
    if (!operation) {
        if (inferredWriteVerb) {
            if (mixedWriteVerbs) {
                return {"EXECUTE SCRIPT", "EXECUTE SCRIPT"};
            }
            operation = inferredWriteVerb;
        } else if (hasReads || query.ResultBindingsSize() > 0) {
            operation = "SELECT";
        }
    }
    if (!operation) {
        return {};
    }

    const bool isRead = operation == "SELECT";
    const TString& table = isRead ? readTable : writeTable;
    const bool ambiguousTable = isRead ? multiRead : multiWrite;
    return {
        !table || ambiguousTable
            ? operation : TStringBuilder() << operation << " " << table,
        operation,
    };
}

} // namespace

TQueryTraceDescription DescribeQueryTrace(NKikimrKqp::EQueryType queryType,
        size_t statementCount, const NKqpProto::TKqpPhyQuery& physicalQuery,
        const TMaybe<TString>& commandTag) {
    switch (queryType) {
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            return {"EXECUTE SCRIPT", "EXECUTE SCRIPT"};
        default:
            break;
    }
    if (statementCount > 1) {
        return {"EXECUTE SCRIPT", "EXECUTE SCRIPT"};
    }
    return DescribePhysicalQuery(physicalQuery, commandTag);
}

TString FallbackQueryTraceName(NKikimrKqp::EQueryType queryType,
        NKikimrKqp::EQueryAction queryAction) {
    switch (queryType) {
        case NKikimrKqp::QUERY_TYPE_SQL_DDL:
            return "DDL";
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            return "EXECUTE SCRIPT";
        default:
            break;
    }
    return QueryTraceActionName(queryAction);
}

TString QueryTraceActionName(NKikimrKqp::EQueryAction action) {
    TString name = NKikimrKqp::EQueryAction_Name(action);
    constexpr TStringBuf prefix = "QUERY_ACTION_";
    if (name.StartsWith(prefix)) {
        name = name.substr(prefix.size());
    }
    return name;
}

TString QueryTraceSpanName(NKikimrKqp::EQueryAction action) {
    switch (action) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            return "Query";
        case NKikimrKqp::QUERY_ACTION_EXPLAIN:
            return "Explain query";
        case NKikimrKqp::QUERY_ACTION_VALIDATE:
            return "Validate query";
        case NKikimrKqp::QUERY_ACTION_PREPARE:
            return "Prepare query";
        case NKikimrKqp::QUERY_ACTION_BEGIN_TX:
            return "Begin transaction";
        case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
            return "Commit transaction";
        case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX:
            return "Rollback transaction";
        case NKikimrKqp::QUERY_ACTION_PARSE:
            return "Parse query";
        case NKikimrKqp::QUERY_ACTION_TOPIC:
            return "Topic operation";
        default:
            return "Query request";
    }
}

void AddQueryTraceAttributes(NWilson::TSpan& span, NKikimrKqp::EQueryType queryType,
        NKikimrKqp::EQueryAction action, const TString& database, const TString& query) {
    if (!span) {
        return;
    }
    span.Attribute("db.system.name", TString("ydb"));
    span.Attribute("ydb.code.component", TString("KQP"));
    span.Attribute("ydb.query.type", NKikimrKqp::EQueryType_Name(queryType));
    span.Attribute("ydb.query.action", QueryTraceActionName(action));
    if (database) {
        span.Attribute("db.namespace", database);
    }
    if (!query.empty() && query.size() <= NQueryTraceSettings::MaxQueryTextBytes) {
        span.Attribute("db.query.text", NKikimr::ProtectQueryForLoggingIfSensitive(query));
    }
}

void EndQueryTraceSpan(NWilson::TSpan& span, Ydb::StatusIds::StatusCode status) {
    if (!span) {
        return;
    }
    span.Attribute("ydb.status_code", Ydb::StatusIds::StatusCode_Name(status));
    if (status == Ydb::StatusIds::SUCCESS) {
        span.EndOk();
    } else if (status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
        span.End();
    } else {
        span.EndError(Ydb::StatusIds::StatusCode_Name(status));
    }
}

void EndProxyQueryTraceSpan(NWilson::TSpan& span, const NKikimrKqp::TEvQueryResponse& response) {
    if (span) {
        switch (response.GetRejectionStage()) {
            case NKikimrKqp::TEvQueryResponse::REJECTION_STAGE_PROXY:
                span.Attribute("ydb.rejected", true);
                span.Attribute("ydb.trace.coverage", TString("proxy_only"));
                break;
            case NKikimrKqp::TEvQueryResponse::REJECTION_STAGE_SESSION:
                span.Attribute("ydb.rejected", true);
                span.Attribute("ydb.trace.coverage", TString("rejected_before_query_state"));
                break;
            default:
                break;
        }
    }
    EndQueryTraceSpan(span, response.GetYdbStatus());
}

} // namespace NKikimr::NKqp
