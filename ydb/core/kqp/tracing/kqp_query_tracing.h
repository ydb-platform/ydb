#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp {

struct TKqpQueryStats;

struct TQueryTraceDescription {
    TString DisplayName;
    TString Operation;
};

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
    ui64 Count = 0;
    ui64 Dropped = 0;
};

class TCommitTracePhase {
public:
    void Start(const NWilson::TSpan& parent, const char* name, ui64 shards);
    void Acknowledge(ui64 shardId, bool last);
    void End(Ydb::StatusIds::StatusCode status);

private:
    NWilson::TSpan Span;
    TShardTraceEvents Events;
};

} // namespace NKikimr::NKqp
