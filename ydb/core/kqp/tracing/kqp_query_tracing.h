#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKqpProto {
class TKqpPhyQuery;
class TKqpStatsQuery;
} // namespace NKqpProto

namespace NYql::NDqProto {
class TDqComputeActorStats;
class TDqExecutionStats;
class TDqTaskStats;
} // namespace NYql::NDqProto

namespace NKikimr::NKqp {

struct TKqpQueryStats;

struct TQueryTraceDescription {
    TString DisplayName;
    TString Operation;
};

enum class EQueryTracePhase {
    Admission,
    ResolveTables,
    ResolveShards,
    ResolveMetadata,
    ResolvePartitioning,
    Snapshot,
    SessionSnapshot,
    PersistentSnapshot,
    RunTasks,
    BufferLookup,
    Write,
    WaitForWrites,
    FlushEffects,
    Commit,
    CommitPrepareShards,
    CommitApplyShards,
    CommitCoordinator,
    Rollback,
};

NWilson::TSpan MakeQueryPhaseTraceSpan(ui8 verbosity, NWilson::TTraceId parent,
    EQueryTracePhase phase, NWilson::TFlags flags = NWilson::EFlags::NONE,
    NActors::TActorSystem* actorSystem = nullptr);

TQueryTraceDescription DescribeQueryTrace(NKikimrKqp::EQueryType queryType,
    size_t statementCount, const NKqpProto::TKqpPhyQuery& physicalQuery,
    const TMaybe<TString>& commandTag);

TString QueryTraceActionName(NKikimrKqp::EQueryAction action);
TString QueryTraceSpanName(NKikimrKqp::EQueryAction action);
TString FallbackQueryTraceName(NKikimrKqp::EQueryType queryType,
    NKikimrKqp::EQueryAction action);

void AddQueryTraceAttributes(NWilson::TSpan& span, NKikimrKqp::EQueryType queryType,
    NKikimrKqp::EQueryAction action, const TString& database, const TString& query);
void EndQueryTraceSpan(NWilson::TSpan& span, Ydb::StatusIds::StatusCode status);
void EndProxyQueryTraceSpan(NWilson::TSpan& span, const NKikimrKqp::TEvQueryResponse& response);
void AddQueryResultAttributes(NWilson::TSpan& span, const TQueryTraceDescription& description,
    const TKqpQueryStats& stats, ui64 requestUnits, Ydb::StatusIds::StatusCode status);
void AddWorkerQueryResultAttributes(NWilson::TSpan& span, const TQueryTraceDescription& description,
    const NKikimrKqp::TEvQueryResponse& response, const NKqpProto::TKqpStatsQuery* workerStats);
void AddExecutionTraceCpuTime(NWilson::TSpan& span, NYql::NDqProto::TDqExecutionStats& stats, ui64 cpuUs);
ui64 GetExecutionTraceCpuTimeUs(const NYql::NDqProto::TDqExecutionStats& stats);
void AddReadTraceStats(NWilson::TSpan& span, NYql::NDqProto::TDqTaskStats& stats,
    const TString& table, ui64 rows, ui64 retries);
void AddKqpTaskTraceAttributes(NWilson::TSpan& span, const NYql::NDqProto::TDqComputeActorStats& stats);
NWilson::TSpan MakeMetadataTraceSpan(const NWilson::TTraceId& parent, NActors::TActorSystem* actorSystem,
    const TString& name, const TString& table, const char* purpose);
void MarkJoinedCompilation(NWilson::TSpan& waiter, const NWilson::TSpan& compilation);

class TShardTraceEvents {
public:
    void Acknowledge(NWilson::TSpan& span, ui64 shardId, bool last);
    void Finish(NWilson::TSpan& span);

private:
    bool Retain(const NWilson::TSpan& span, bool last = false);

private:
    ui64 Count_ = 0;
    ui64 Dropped_ = 0;
};

class TCommitTracePhase {
public:
    template<class TCountShards>
    void Start(const NWilson::TSpan& parent, EQueryTracePhase phase, TCountShards&& countShards) {
        if (StartSpan(parent, phase)) {
            Span_.Attribute("ydb.shards", static_cast<i64>(countShards()));
        }
    }
    void Acknowledge(ui64 shardId, bool last);
    void End(Ydb::StatusIds::StatusCode status);

private:
    bool StartSpan(const NWilson::TSpan& parent, EQueryTracePhase phase);

private:
    NWilson::TSpan Span_;
    TShardTraceEvents Events_;
};

} // namespace NKikimr::NKqp
