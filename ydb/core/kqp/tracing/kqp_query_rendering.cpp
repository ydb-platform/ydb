#include "kqp_query_rendering.h"

#include "kqp_query_stats_rendering.h"
#include "kqp_trace_settings.h"

#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/security/util.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <util/string/builder.h>

#include <google/protobuf/any.pb.h>

namespace NKikimr::NKqp {

NWilson::TSpan MakeQueryPhaseTraceSpan(ui8 verbosity, NWilson::TTraceId parent,
        const TQueryTraceSpanDescription& description, NWilson::TFlags flags, NActors::TActorSystem* actorSystem) {
    NWilson::TSpan span(verbosity, std::move(parent), description.Name, flags, actorSystem);
    if (!span) {
        return span;
    }
    span.Attribute("ydb.phase", TString(description.Phase));
    if (description.ActorType) {
        span.Attribute("ydb.actor.type", TString(description.ActorType));
    }
    if (description.Component) {
        span.Attribute("ydb.code.component", TString(description.Component));
    }
    if (description.PeerActorType) {
        span.Attribute("ydb.peer.actor.type", TString(description.PeerActorType));
    }
    return span;
}

void AddWorkerQueryResultAttributes(NWilson::TSpan& span, const TQueryTraceDescription& description,
        const NKikimrKqp::TEvQueryResponse& response, const NKqpProto::TKqpStatsQuery* workerStats,
        bool spilledBytesAvailable) {
    if (span) {
        const auto& stats = workerStats ? *workerStats : response.GetResponse().GetQueryStats();
        NPrivate::AddQueryExecutionAttributes(span, description, stats, response.GetConsumedRu(), response.GetYdbStatus(),
            spilledBytesAvailable);
        if (stats.HasCompilation()) {
            const auto& compilation = stats.GetCompilation();
            NPrivate::AddQueryCompilationAttributes(span, compilation.GetFromCache(), compilation.GetCpuTimeUs(), compilation.GetDurationUs());
        }
    }
}

NWilson::TSpan MakeMetadataTraceSpan(const NWilson::TTraceId& parent, NActors::TActorSystem* actorSystem,
        EMetadataTraceOperation operation, const TString& table, const char* purpose) {
    const bool loadsMetadata = operation == EMetadataTraceOperation::LoadMetadata;
    const TString name = loadsMetadata ? "Load metadata" : "Load statistics";
    NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::Detailed,
        NWilson::TTraceId(parent), name, NWilson::EFlags::NONE, actorSystem);
    span.Attribute("db.collection.name", table);
    span.Attribute("ydb.actor.type", TString("TActorRequestHandler"));
    span.Attribute("ydb.code.component", TString("KqpTableMetadataLoader"));
    span.Attribute("ydb.peer.actor.type", TString(loadsMetadata ? "SchemeCache" : "StatisticsService"));
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
    if (Count_ >= NQueryTraceSettings::MAX_SHARD_EVENTS
            || (Count_ >= NQueryTraceSettings::MAX_SHARD_EVENTS - 1 && !last)) {
        ++Dropped_;
        return false;
    }
    ++Count_;
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
    if (span && Dropped_) {
        span.Attribute("ydb.shard_events_dropped", static_cast<i64>(Dropped_));
    }
    Count_ = 0;
    Dropped_ = 0;
}

bool TCommitTracePhase::StartSpan(const NWilson::TSpan& parent, const TQueryTraceSpanDescription& description) {
    End(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);
    Span_ = MakeQueryPhaseTraceSpan(TComponentTracingLevels::TQueryProcessor::Detailed,
        parent.GetTraceId(), description, NWilson::EFlags::AUTO_END, parent.GetActorSystem());
    return bool(Span_);
}

void TCommitTracePhase::Acknowledge(ui64 shardId, bool last) {
    Events_.Acknowledge(Span_, shardId, last);
}

void TCommitTracePhase::End(Ydb::StatusIds::StatusCode status) {
    Events_.Finish(Span_);
    EndQueryTraceSpan(Span_, status);
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
    }
    return nullptr;
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
                operation = "WRITE";
            } else {
                operation = inferredWriteVerb;
            }
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
            return "Query session";
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
    if (!query.empty() && query.size() <= NQueryTraceSettings::MAX_QUERY_TEXT_BYTES) {
        span.Attribute("db.query.text", NKikimr::ProtectQueryForLoggingIfSensitive(query));
    }
}

void AddQuerySessionTraceAttributes(NWilson::TSpan& span, const TString& sessionId,
        const ::Ydb::Table::TransactionControl* txControl) {
    if (!span) {
        return;
    }
    if (!sessionId.empty()) {
        span.Attribute("ydb.session_id", sessionId);
    }
    if (!txControl) {
        return;
    }
    span.Attribute("ydb.tx.commit", txControl->commit_tx());
    switch (txControl->tx_selector_case()) {
        case ::Ydb::Table::TransactionControl::kTxId:
            span.Attribute("ydb.tx.id", txControl->tx_id());
            break;
        case ::Ydb::Table::TransactionControl::kBeginTx:
            switch (txControl->begin_tx().tx_mode_case()) {
                case ::Ydb::Table::TransactionSettings::kSerializableReadWrite:
                    span.Attribute("ydb.tx.mode", TString("SerializableReadWrite"));
                    break;
                case ::Ydb::Table::TransactionSettings::kOnlineReadOnly:
                    span.Attribute("ydb.tx.mode", TString("OnlineReadOnly"));
                    break;
                case ::Ydb::Table::TransactionSettings::kStaleReadOnly:
                    span.Attribute("ydb.tx.mode", TString("StaleReadOnly"));
                    break;
                case ::Ydb::Table::TransactionSettings::kSnapshotReadOnly:
                    span.Attribute("ydb.tx.mode", TString("SnapshotReadOnly"));
                    break;
                default:
                    break;
            }
            break;
        default:
            break;
    }
}

void SetQueryTraceTransactionId(NWilson::TSpan& span, const TString& txId) {
    if (span && !txId.empty()) {
        span.Attribute("ydb.tx.id", txId);
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

NWilson::TSpan MakeQueryRedirectTraceSpan(const NWilson::TSpan& parent, ui32 sourceNodeId, ui32 targetNodeId) {
    NWilson::TSpan span(TComponentTracingLevels::TQueryProcessor::TopLevel,
        parent.GetTraceId(), "KQP redirect", NWilson::EFlags::AUTO_END, parent.GetActorSystem());
    if (span) {
        span.Attribute("ydb.code.component", TString("KQP"));
        span.Attribute("ydb.actor.type", TString("TKqpProxyService"));
        span.Attribute("ydb.source_node_id", static_cast<i64>(sourceNodeId));
        span.Attribute("ydb.target_node_id", static_cast<i64>(targetNodeId));
    }
    return span;
}

} // namespace NKikimr::NKqp
