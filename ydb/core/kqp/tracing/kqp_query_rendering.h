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

namespace Ydb::Table {
class TransactionControl;
} // namespace Ydb::Table

namespace NKikimr::NKqp {

struct TQueryTraceDescription {
    TString DisplayName;
    TString Operation;
};

struct TQueryTraceSpanDescription {
    const char* Name;
    const char* Phase;
    const char* ActorType;
    const char* Component = nullptr;
    const char* PeerActorType = nullptr;
};

enum class EMetadataTraceOperation {
    LoadMetadata,
    LoadStatistics,
};

NWilson::TSpan MakeQueryPhaseTraceSpan(ui8 verbosity, NWilson::TTraceId parent,
    const TQueryTraceSpanDescription& description, NWilson::TFlags flags = NWilson::EFlags::NONE,
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
void AddQuerySessionTraceAttributes(NWilson::TSpan& span, const TString& sessionId,
    const ::Ydb::Table::TransactionControl* txControl);
void SetQueryTraceTransactionId(NWilson::TSpan& span, const TString& txId);
void EndQueryTraceSpan(NWilson::TSpan& span, Ydb::StatusIds::StatusCode status);
void EndProxyQueryTraceSpan(NWilson::TSpan& span, const NKikimrKqp::TEvQueryResponse& response);
NWilson::TSpan MakeQueryRedirectTraceSpan(const NWilson::TSpan& parent, ui32 sourceNodeId, ui32 targetNodeId);
void AddWorkerQueryResultAttributes(NWilson::TSpan& span, const TQueryTraceDescription& description,
    const NKikimrKqp::TEvQueryResponse& response, const NKqpProto::TKqpStatsQuery* workerStats,
    bool spilledBytesAvailable);
NWilson::TSpan MakeMetadataTraceSpan(const NWilson::TTraceId& parent, NActors::TActorSystem* actorSystem,
    EMetadataTraceOperation operation, const TString& table, const char* purpose);
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
    void Start(const NWilson::TSpan& parent, const TQueryTraceSpanDescription& description, TCountShards&& countShards) {
        if (StartSpan(parent, description)) {
            Span_.Attribute("ydb.shards", static_cast<i64>(countShards()));
        }
    }
    void Acknowledge(ui64 shardId, bool last);
    void End(Ydb::StatusIds::StatusCode status);

private:
    bool StartSpan(const NWilson::TSpan& parent, const TQueryTraceSpanDescription& description);

private:
    NWilson::TSpan Span_;
    TShardTraceEvents Events_;
};

} // namespace NKikimr::NKqp
