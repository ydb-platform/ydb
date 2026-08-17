#include "kqp_warmup_compile_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/query_actor/query_actor.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/library/actors/core/scheduler_cookie.h>

#include <util/random/shuffle.h>

#include <util/generic/hash.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPILE_SERVICE


namespace NKikimr::NKqp {

struct TEvPrivate {
    enum EEv {
        EvFetchCacheResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvDelayedComplete,
        EvHardDeadline,
        EvSoftDeadline,
        EvTruncatedCountResult,
        EvCheckTopology,
        EvRetryFetch,
    };

    struct TQueryToCompile {
        TString QueryText;
        TString UserSID;
        ui64 CompilationDurationMs = 0;
        TString Metadata;
        TString QueryType;  // EQueryType name from compile_cache_queries (e.g. QUERY_TYPE_SQL_DML)
        TString Syntax;     // Ydb::Query::Syntax name (e.g. SYNTAX_YQL_V1, SYNTAX_PG)
    };

    struct TEvFetchCacheResult : public NActors::TEventLocal<TEvFetchCacheResult, EvFetchCacheResult> {
        bool Success;
        TString Error;
        TString Warnings;
        std::deque<TQueryToCompile> Queries;

        TEvFetchCacheResult(bool success, TString error = {})
            : Success(success)
            , Error(std::move(error))
        {}
    };

    struct TEvDelayedComplete : public NActors::TEventLocal<TEvDelayedComplete, EvDelayedComplete> {};
    struct TEvHardDeadline : public NActors::TEventLocal<TEvHardDeadline, EvHardDeadline> {};
    struct TEvSoftDeadline : public NActors::TEventLocal<TEvSoftDeadline, EvSoftDeadline> {};
    struct TEvCheckTopology : public NActors::TEventLocal<TEvCheckTopology, EvCheckTopology> {};
    struct TEvRetryFetch : public NActors::TEventLocal<TEvRetryFetch, EvRetryFetch> {};

    struct TEvTruncatedCountResult : public NActors::TEventLocal<TEvTruncatedCountResult, EvTruncatedCountResult> {
        bool Success;
        ui64 Count;
        ui64 EmptyQueryTypeCount = 0;

        TEvTruncatedCountResult(bool success, ui64 count = 0, ui64 emptyQueryTypeCount = 0)
            : Success(success)
            , Count(count)
            , EmptyQueryTypeCount(emptyQueryTypeCount)
        {}
    };
};

namespace {

// IN (...) — partial peer failures are tolerated within a single sysview scan;
// UNION ALL of point reads fails the whole fetch if any branch fails. IN collapses
// to [min..max] so callers pass a contiguous slice (see StartFetch).
TString BuildNodeIdInClause(const TVector<ui32>& nodeIds) {
    TStringBuilder clause;
    clause << "NodeId IN (";
    for (size_t i = 0; i < nodeIds.size(); ++i) {
        if (i > 0) {
            clause << ", ";
        }
        clause << nodeIds[i];
    }
    clause << ")";
    return clause;
}

} // anonymous namespace

/*
    Helper actor to fetch and parse data from /.sys/compile_cache_queries.
    See TFetchCacheActor::OnRunQuery() for query constraints on cache records.
*/
class TFetchCacheActor : public TQueryBase {
public:
    TFetchCacheActor(const TString& database, ui32 maxQueriesToLoad, ui64 maxCompilationDurationMs, const TVector<ui32>& nodeIds)
        : TQueryBase(NKikimrServices::KQP_COMPILE_SERVICE, {}, database, true, true)
        , MaxQueriesToLoad(maxQueriesToLoad)
        , MaxCompilationDurationMs(maxCompilationDurationMs)
        , NodeIds(nodeIds)
    {
        ForwardStreamIssuesOnSuccess = true;
    }

    void OnRunQuery() override {
        const auto limit = std::max<ui32>(1u, MaxQueriesToLoad);
        TStringBuilder sql;
        sql << "SELECT Query, UserSID, MAX(Metadata) AS Metadata, SUM(AccessCount) AS AccessCount, "
            << "MAX(CompilationDurationMs) AS CompilationDurationMs, MAX(QueryType) AS QueryType, MAX(Syntax) AS Syntax"
            << " FROM `" << Database << "/.sys/compile_cache_queries`"
            << " WHERE IsTruncated = false"
            << " AND AccessCount > 0"
            << " AND QueryType IS NOT NULL AND QueryType != ''"
            << " AND CompilationDurationMs < " << MaxCompilationDurationMs
            << " AND " << BuildNodeIdInClause(NodeIds)
            << " GROUP BY Query, UserSID, QueryType, Syntax"
            << " ORDER BY AccessCount DESC"
            << " LIMIT " << limit;

        RunStreamQuery(sql);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto queryText = parser.ColumnParser("Query").GetOptionalUtf8();
            auto userSID = parser.ColumnParser("UserSID").GetOptionalUtf8();
            auto metadata = parser.ColumnParser("Metadata").GetOptionalUtf8();
            auto compilationDuration = parser.ColumnParser("CompilationDurationMs").GetOptionalUint64();
            auto queryType = parser.ColumnParser("QueryType").GetOptionalUtf8();
            auto syntax = parser.ColumnParser("Syntax").GetOptionalUtf8();

            if (queryText && !queryText->empty()) {
                TEvPrivate::TQueryToCompile query;
                query.QueryText = TString(*queryText);
                query.UserSID = userSID ? TString(*userSID) : TString();
                query.Metadata = metadata ? TString(*metadata) : TString();
                query.CompilationDurationMs = compilationDuration.value_or(0);
                query.QueryType = queryType ? TString(*queryType) : TString();
                query.Syntax = syntax ? TString(*syntax) : TString();
                Result->Queries.push_back(std::move(query));
            }
        }
    }

    void OnQueryResult() override {
        Result->Success = true;
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status != Ydb::StatusIds::SUCCESS) {
            Result->Success = false;
            Result->Error = issues.ToString();
        } else if (!issues.Empty()) {
            Result->Warnings = issues.ToOneLineString();
        }
        Send(Owner, Result.release());
    }

private:
    ui32 MaxQueriesToLoad;
    ui64 MaxCompilationDurationMs;
    TVector<ui32> NodeIds;
    std::unique_ptr<TEvPrivate::TEvFetchCacheResult> Result = std::make_unique<TEvPrivate::TEvFetchCacheResult>(false);
};

/*
    Helper actor to count truncated queries in /.sys/compile_cache_queries.
    Runs independently from the main fetch, result is used for observability only.
*/
class TFetchTruncatedCountActor : public TQueryBase {
public:
    TFetchTruncatedCountActor(const TString& database, const TVector<ui32>& nodeIds)
        : TQueryBase(NKikimrServices::KQP_COMPILE_SERVICE, {}, database, true, true)
        , NodeIds(nodeIds)
    {}

    void OnRunQuery() override {
        TStringBuilder sql;
        sql << "SELECT SUM(CASE WHEN IsTruncated = true THEN 1 ELSE 0 END) AS TruncatedCount,"
            << " SUM(CASE WHEN QueryType IS NULL OR QueryType = '' THEN 1 ELSE 0 END) AS EmptyQueryTypeCount"
            << " FROM `" << Database << "/.sys/compile_cache_queries`"
            << " WHERE AccessCount > 0"
            << " AND " << BuildNodeIdInClause(NodeIds);

        RunStreamQuery(sql);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        NYdb::TResultSetParser parser(resultSet);
        if (parser.TryNextRow()) {
            TruncatedCount = static_cast<ui64>(parser.ColumnParser("TruncatedCount").GetOptionalInt64().value_or(0));
            EmptyQueryTypeCount = static_cast<ui64>(parser.ColumnParser("EmptyQueryTypeCount").GetOptionalInt64().value_or(0));
        }
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&&) override {
        Send(Owner, new TEvPrivate::TEvTruncatedCountResult(
            status == Ydb::StatusIds::SUCCESS, TruncatedCount, EmptyQueryTypeCount));
    }

private:
    TVector<ui32> NodeIds;
    ui64 TruncatedCount = 0;
    ui64 EmptyQueryTypeCount = 0;
};

namespace {

void FillYdbParametersFromMetadata(
    const TString& metadata,
    google::protobuf::Map<TProtoStringType, Ydb::TypedValue>& params)
{
    if (metadata.empty()) {
        return;
    }

    try {
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(metadata, &json, true)) {
            return;
        }

        if (!json.Has("parameters") || !json["parameters"].IsMap()) {
            return;
        }

        const auto& parameters = json["parameters"].GetMap();

        for (const auto& [paramName, typeJson] : parameters) {
            if (!typeJson.IsMap()) {
                continue;
            }

            try {
                Ydb::Type typeProto;
                NProtobufJson::Json2Proto(typeJson, typeProto);

                Ydb::TypedValue typedValue;
                typedValue.mutable_type()->CopyFrom(typeProto);

                params[paramName] = std::move(typedValue);
            } catch (...) {
                continue;
            }
        }
    } catch (...) {
        // Ignore errors - will compile without parameters if metadata is invalid
    }
}

} // anonymous namespace

// Runs before the node accepts client traffic. Fetches popular queries from peer
// compile caches via /.sys/compile_cache_queries and pre-compiles them so the
// first user queries don't pay compile latency. Triggered by KqpProxy after it
// discovers peers (TEvStartWarmup) or self-skips on single-node tenants
// (HandleCheckTopology). SoftDeadline = stop scheduling new compiles, HardDeadline = abort.
class TKqpCompileCacheWarmupActor : public NActors::TActorBootstrapped<TKqpCompileCacheWarmupActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPILE_SERVICE;
    }

    TKqpCompileCacheWarmupActor(const TKqpWarmupConfig& config, const TString& database,
                           const TString& cluster, TVector<NActors::TActorId> notifyActorIds)
        : Config(config)
        , Database(database)
        , Cluster(cluster)
        , NotifyActorIds(std::move(notifyActorIds))
    {}

    void Bootstrap() {
        Counters = MakeIntrusive<TKqpCounters>(AppData()->Counters, &TlsActivationContext->AsActorContext());
        const auto softDeadline = Config.SoftDeadline;
        const auto hardDeadline = std::max(Config.HardDeadline, softDeadline);
        MaxConcurrentCompilations = std::max<ui32>(1u, Config.MaxConcurrentCompilations);
        HardDeadlineTimestamp = TActivationContext::Now() + hardDeadline;

        YDB_LOG_INFO("Warmup actor started, self-orchestrating topology discovery",
            {"logPrefix", LogPrefix()},
            {"database", Database},
            {"softDeadline", softDeadline},
            {"hardDeadline", hardDeadline},
            {"hardDeadlineAdjusted", Config.HardDeadline < softDeadline},
            {"originalHardDeadline", Config.HardDeadline},
            {"maxConcurrent", MaxConcurrentCompilations},
            {"maxConcurrentAdjusted", Config.MaxConcurrentCompilations == 0});

        // Soft deadline is armed only after the board is up (HandleCheckTopology), so a long board wait on a cold v2 bootstrap doesn't eat the compile budget.
        Schedule(hardDeadline, new TEvPrivate::TEvHardDeadline());

        if (Database.empty()) {
            YDB_LOG_INFO("Database is empty, skipping warmup",
                {"logPrefix", LogPrefix()});
            SkipReason = "Skipped: empty database";
            ScheduleComplete();
            return;
        }

        Schedule(TopologyCheckInterval, new TEvPrivate::TEvCheckTopology());
        Become(&TThis::StateWaitingTopology);
    }

    void ScheduleComplete() {
        // to ensure that the actor system is set up and ready to process incoming messages
        Schedule(TDuration::MilliSeconds(100), new TEvPrivate::TEvDelayedComplete());
        Become(&TThis::StateWaitingComplete);
    }

private:
    STFUNC(StateWaitingComplete) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvPrivate::EvDelayedComplete, HandleDelayedComplete);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(TEvPrivate::EvSoftDeadline, HandleSoftDeadline);
            // Late tick from the topology poller scheduled in Bootstrap; harmless after we left StateWaitingTopology.
            IgnoreFunc(TEvPrivate::TEvCheckTopology);
            hFunc(TEvPrivate::TEvTruncatedCountResult, HandleTruncatedCount);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            YDB_LOG_WARN("StateWaitingComplete: unexpected event",
                {"logPrefix", LogPrefix()},
                {"eventType", ev->GetTypeRewrite()});
            break;
        }
    }

    STFUNC(StateWaitingTopology) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStartWarmup, HandleStartWarmup);
            cFunc(TEvPrivate::EvCheckTopology, HandleCheckTopology);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(TEvPrivate::EvSoftDeadline, HandleSoftDeadlineInTopology);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            YDB_LOG_WARN("StateWaitingTopology: unexpected event",
                {"logPrefix", LogPrefix()},
                {"eventType", ev->GetTypeRewrite()});
            break;
        }
    }

    STFUNC(StateFetching) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvFetchCacheResult, HandleFetchResult);
            cFunc(TEvPrivate::EvRetryFetch, StartFetch);
            hFunc(TEvPrivate::TEvTruncatedCountResult, HandleTruncatedCount);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(TEvPrivate::EvSoftDeadline, HandleSoftDeadline);
            // Late tick from the topology poller scheduled in Bootstrap; harmless after we left StateWaitingTopology.
            IgnoreFunc(TEvPrivate::TEvCheckTopology);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            YDB_LOG_WARN("StateFetching: unexpected event",
                {"logPrefix", LogPrefix()},
                {"eventType", ev->GetTypeRewrite()});
            break;
        }
    }
    STFUNC(StateCompiling) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqp::TEvQueryResponse, HandleQueryResponse);
            hFunc(TEvPrivate::TEvTruncatedCountResult, HandleTruncatedCount);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(TEvPrivate::EvSoftDeadline, HandleSoftDeadline);
            // Late tick from the topology poller scheduled in Bootstrap; harmless after we left StateWaitingTopology.
            IgnoreFunc(TEvPrivate::TEvCheckTopology);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            YDB_LOG_WARN("StateCompiling: unexpected event",
                {"logPrefix", LogPrefix()},
                {"eventType", ev->GetTypeRewrite()});
            break;
        }
    }

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[KqpCompileCacheWarmup] ";
    }

    void HandleDelayedComplete() {
        Complete(true, SkipReason);
    }

    void HandleTruncatedCount(TEvPrivate::TEvTruncatedCountResult::TPtr& ev) {
        if (ev->Get()->Success && Counters) {
            Counters->WarmupQueriesTruncated->Set(ev->Get()->Count);
            Counters->WarmupQueriesEmptyQueryType->Set(ev->Get()->EmptyQueryTypeCount);
        }
        YDB_LOG_INFO("Truncated queries in empty",
            {"logPrefix", LogPrefix()},
            {"cache", ev->Get()->Count},
            {"queryType", ev->Get()->EmptyQueryTypeCount},
            {"success", ev->Get()->Success});
    }

    void HandleStartWarmup(TEvStartWarmup::TPtr& ev) {
        const auto discoveredNodes = ev->Get()->DiscoveredNodesCount;
        NodeIds = ev->Get()->NodeIds;

        YDB_LOG_INFO("Received TEvStartWarmup, nodeIds scheduling soft",
            {"logPrefix", LogPrefix()},
            {"discoveredNodes", discoveredNodes},
            {"count", NodeIds.size()},
            {"maxNodesToQuery", Config.MaxNodesToRequest},
            {"deadline", Config.SoftDeadline});
        RescheduleSoftDeadlineForFetch();
        StartFetch();
    }

    void HandleCheckTopology() {
        auto rm = TryGetKqpResourceManager(SelfId().NodeId());
        if (!rm || !rm->GetInitialBoardSyncDone()) {
            Schedule(TopologyCheckInterval, new TEvPrivate::TEvCheckTopology());
            return;
        }

        // RM board is only used for single-node fast-skip — multi-node still waits
        // for KqpProxy's TEvStartWarmup (otherwise the sysview scan sees [self]).
        auto boardNodeIds = rm->GetInitialBoardNodeIds();
        const ui32 selfNodeId = SelfId().NodeId();
        ui32 peerCount = 0;
        for (auto nodeId : boardNodeIds) {
            if (nodeId != selfNodeId) {
                ++peerCount;
            }
        }

        if (peerCount == 0) {
            YDB_LOG_INFO("No peers in initial kqpexch+ board sync, skipping warmup",
                {"logPrefix", LogPrefix()},
                {"boardSize", boardNodeIds.size()});
            Complete(true, "Skipped: no peers in initial kqpexch+ board sync");
            return;
        }

        SoftDeadlineCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
        Schedule(Config.SoftDeadline, new TEvPrivate::TEvSoftDeadline(), SoftDeadlineCookieHolder.Get());

        YDB_LOG_INFO("Initial board sync delivered",
            {"logPrefix", LogPrefix()},
            {"peerCount", peerCount});
    }

    void RescheduleSoftDeadlineForFetch() {
        SoftDeadlineCookieHolder.Detach();
        SoftDeadlineCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
        Schedule(Config.SoftDeadline, new TEvPrivate::TEvSoftDeadline(), SoftDeadlineCookieHolder.Get());
    }

    void StartFetch() {
        if (NodeIds.empty()) {
            YDB_LOG_WARN("StartFetch called with empty NodeIds, skipping warmup",
                {"logPrefix", LogPrefix()});
            Complete(true, "Skipped: empty NodeIds");
            return;
        }

        ++FetchAttempts;

        const ui32 maxNodesToQuery = Config.MaxNodesToRequest;
        if (maxNodesToQuery > 0 && maxNodesToQuery < NodeIds.size()) {
            // Contiguous slice of sorted NodeIds: see BuildNodeIdInClause for why.
            // Random start clamped to tail so the window always fits.
            std::sort(NodeIds.begin(), NodeIds.end());
            const ui32 total = NodeIds.size();
            ui32 start = AppData()->RandomProvider->GenRand() % total;
            if (start + maxNodesToQuery > total) {
                start = total - maxNodesToQuery;
            }
            NodeIds.erase(NodeIds.begin(), NodeIds.begin() + start);
            NodeIds.resize(maxNodesToQuery);
        }

        YDB_LOG_INFO("Spawning fetch cache actor, filtering by nodes",
            {"logPrefix", LogPrefix()},
            {"nodeIdsCount", NodeIds.size()});
        const ui64 maxCompilationMs = Config.MaxCompilationDurationMs > 0
            ? Config.MaxCompilationDurationMs
            : Config.SoftDeadline.MilliSeconds() / 2;
        Register(new TFetchCacheActor(Database, Config.MaxQueriesToLoad, maxCompilationMs, NodeIds));
        Register(new TFetchTruncatedCountActor(Database, NodeIds));
        Become(&TThis::StateFetching);
    }

    void HandleFetchResult(TEvPrivate::TEvFetchCacheResult::TPtr& ev) {
        auto* result = ev->Get();

        if (!result->Success) {
            // DB may not be resolvable yet this early after start; retry before giving up.
            if (!SoftDeadlineReached && FetchAttempts < MaxFetchAttempts) {
                YDB_LOG_WARN("Fetch failed, retrying",
                    {"logPrefix", LogPrefix()},
                    {"fetchAttempts", FetchAttempts},
                    {"maxFetchAttempts", MaxFetchAttempts},
                    {"fetchRetryDelay", FetchRetryDelay},
                    {"error", result->Error});
                Schedule(FetchRetryDelay, new TEvPrivate::TEvRetryFetch());
                return;
            }
            YDB_LOG_WARN("Fetch failed (no compile cache nodes responded), skipping",
                {"logPrefix", LogPrefix()},
                {"warmup", result->Error});
            Complete(false, "Fetch failed: " + result->Error);
            return;
        }

        if (!result->Warnings.empty()) {
            YDB_LOG_WARN("Fetch completed with",
                {"logPrefix", LogPrefix()},
                {"warnings", result->Warnings});
        }

        QueriesToCompile = std::move(result->Queries);
        YDB_LOG_INFO("Fetched queries from compile cache",
            {"logPrefix", LogPrefix()},
            {"queriesToCompileCount", QueriesToCompile.size()});

        if (Counters) {
            Counters->WarmupQueriesFetched->Add(QueriesToCompile.size());
        }

        // Skip legacy PG-syntax queries from the compile cache.
        std::erase_if(QueriesToCompile, [](const TEvPrivate::TQueryToCompile& q) {
            return ParseSyntax(q.Syntax, q.QueryType) == Ydb::Query::SYNTAX_PG;
        });

        if (QueriesToCompile.empty()) {
            Complete(true, "No queries to warm up");
            return;
        }
        // sort to compile the longest queries first
        std::sort(QueriesToCompile.begin(), QueriesToCompile.end(),
            [](const TEvPrivate::TQueryToCompile& a, const TEvPrivate::TQueryToCompile& b) {
                return a.CompilationDurationMs > b.CompilationDurationMs;
            });

        Become(&TThis::StateCompiling);
        StartCompilations();
    }

    void HandleQueryResponse(TEvKqp::TEvQueryResponse::TPtr& ev) {
        PendingCompilations--;

        const auto& record = ev->Get()->Record;
        bool success = (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS);

        ui64 cookie = ev->Cookie;

        if (auto it = PendingQueriesByCookie.find(cookie); it != PendingQueriesByCookie.end()) {
            auto& query = it->second;
            if (success) {
                YDB_LOG_INFO("Query compiled successfully",
                    {"logPrefix", LogPrefix()},
                    {"user", query.UserSID},
                    {"hasMetadata", !query.Metadata.empty()},
                    {"queryPreview", query.QueryText.substr(0, 200)},
                    {"queryTruncated", query.QueryText.size() > 200});
            } else {
                TString errorMsg;
                const auto& issues = record.GetResponse().GetQueryIssues();
                if (issues.size() > 0) {
                    for (const auto& issue : issues) {
                        if (!errorMsg.empty()) {
                            errorMsg += "; ";
                        }
                        errorMsg += issue.message();
                    }
                }
                YDB_LOG_WARN("Query compilation failed",
                    {"logPrefix", LogPrefix()},
                    {"user", query.UserSID},
                    {"hasMetadata", !query.Metadata.empty()},
                    {"status", Ydb::StatusIds::StatusCode_Name(record.GetYdbStatus())},
                    {"error", errorMsg},
                    {"queryPreview", query.QueryText.substr(0, 200)},
                    {"queryTruncated", query.QueryText.size() > 200});
            }
            PendingQueriesByCookie.erase(it);
        } else {
            YDB_LOG_WARN("Received response for unknown",
                {"logPrefix", LogPrefix()},
                {"cookie", cookie},
                {"success", success});
        }

        if (success) {
            EntriesLoaded++;
            if (Counters) {
                Counters->WarmupQueriesCompiled->Inc();
            }
        } else {
            EntriesFailed++;
        }

        StartCompilations();
    }

    static NKikimrKqp::EQueryType ParseQueryType(const TString& queryTypeStr) {
        if (queryTypeStr.empty()) {
            return NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY;
        }
        NKikimrKqp::EQueryType result;
        if (NKikimrKqp::EQueryType_Parse(queryTypeStr, &result)) {
            return result;
        }
        return NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY;
    }

    static Ydb::Query::Syntax ParseSyntax(const TString& syntaxStr, const TString& queryTypeStr) {
        if (!syntaxStr.empty()) {
            Ydb::Query::Syntax result;
            if (Ydb::Query::Syntax_Parse(syntaxStr, &result)) {
                return result;
            }
        }
        // Backward compatibility: older nodes don't store Syntax, infer from QueryType
        auto queryType = ParseQueryType(queryTypeStr);
        if (queryType == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY ||
            queryType == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT ||
            queryType == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY) {
            return Ydb::Query::SYNTAX_YQL_V1;
        }
        return Ydb::Query::SYNTAX_UNSPECIFIED;
    }

    static std::unique_ptr<TEvKqp::TEvQueryRequest> CreatePrepareRequest(
        const TString& database,
        const TString& queryText,
        const TString& userSid,
        TDuration timeout,
        const TString& metadata,
        const TString& queryTypeStr,
        const TString& syntaxStr)
    {
        auto queryEv = std::make_unique<TEvKqp::TEvQueryRequest>();
        auto& record = queryEv->Record;
        if (!userSid.empty()) {
            auto userToken = MakeIntrusive<NACLib::TUserToken>(userSid, TVector<NACLib::TSID>{});
            record.SetUserToken(userToken->SerializeAsString());
        }

        auto& request = *record.MutableRequest();
        request.SetDatabase(database);
        request.SetQuery(queryText);
        request.SetAction(NKikimrKqp::QUERY_ACTION_PREPARE);
        request.SetType(ParseQueryType(queryTypeStr));
        request.SetTimeoutMs(timeout.MilliSeconds());
        request.SetIsInternalCall(false);
        request.SetIsWarmupCompilation(true);
        request.SetSyntax(ParseSyntax(syntaxStr, queryTypeStr));

        if (!metadata.empty()) {
            FillYdbParametersFromMetadata(metadata, *request.MutableYdbParameters());
        }

        return queryEv;
    }

    void SendPrepareRequest(const TEvPrivate::TQueryToCompile& query) {
        ui64 cookie = NextCookie++;
        PendingQueriesByCookie[cookie] = query;

        auto remaining = HardDeadlineTimestamp - TActivationContext::Now();
        auto timeout = std::min(Config.SoftDeadline, remaining);
        if (timeout <= TDuration::Zero()) {
            timeout = TDuration::MilliSeconds(100);
        }

        YDB_LOG_DEBUG("Sending PREPARE request for query",
            {"logPrefix", LogPrefix()},
            {"user", query.UserSID},
            {"length", query.QueryText.size()},
            {"hasMetadata", !query.Metadata.empty()},
            {"timeout", timeout});

        auto request = CreatePrepareRequest(Database, query.QueryText, query.UserSID,
                                            timeout, query.Metadata, query.QueryType, query.Syntax);
        request->Record.MutableRequest()->SetKeepSession(false);

        Send(MakeKqpProxyID(SelfId().NodeId()), request.release(), 0, cookie);
    }

    void StartCompilations() {
        while (!SoftDeadlineReached && PendingCompilations < MaxConcurrentCompilations && !QueriesToCompile.empty()) {
            auto query = std::move(QueriesToCompile.front());
            QueriesToCompile.pop_front();

            SendPrepareRequest(query);
            PendingCompilations++;
        }

        if (PendingCompilations == 0 && QueriesToCompile.empty()) {
            YDB_LOG_INFO("All compilations finished",
                {"logPrefix", LogPrefix()},
                {"loaded", EntriesLoaded},
                {"failed", EntriesFailed});
            TString msg = TStringBuilder() << "Compiled " << EntriesLoaded << " queries"
                << (SoftDeadlineReached ? " (soft deadline)" : "");
            Complete(true, msg);
        }
    }


    void HandleHardDeadline() {
        YDB_LOG_WARN("Hard deadline reached",
            {"logPrefix", LogPrefix()},
            {"compiled", EntriesLoaded},
            {"failed", EntriesFailed},
            {"pending", PendingCompilations});

        PendingQueriesByCookie.clear();
        PendingCompilations = 0;

        Complete(false, "Hard deadline exceeded");
    }

    void HandleSoftDeadline() {
        SoftDeadlineReached = true;
        QueriesToCompile.clear();

        YDB_LOG_INFO("Soft deadline reached",
            {"logPrefix", LogPrefix()},
            {"compiled", EntriesLoaded},
            {"failed", EntriesFailed},
            {"pending", PendingCompilations});

        if (PendingCompilations == 0) {
            Complete(true, TStringBuilder() << "Soft deadline: compiled " << EntriesLoaded << " queries");
        } else {
            YDB_LOG_INFO("Waiting for in-flight compilations to finish",
                {"logPrefix", LogPrefix()},
                {"pendingCompilations", PendingCompilations});
        }
    }

    void HandleSoftDeadlineInTopology() {
        // No peer NodeIds yet → can only read self (useless on warm restart).
        YDB_LOG_WARN("Soft deadline reached while waiting for topology, skipping warmup",
            {"logPrefix", LogPrefix()});
        Complete(true, "Skipped: topology not delivered before soft deadline");
    }

    void HandlePoison() {
        YDB_LOG_DEBUG("Received poison, stop warmup",
            {"logPrefix", LogPrefix()});
        PassAway();
    }

    void Complete(bool success, const TString& message) {
        if (Completed) {
            return;
        }
        Completed = true;

        YDB_LOG_INFO("Warmup finished",
            {"logPrefix", LogPrefix()},
            {"success", success},
            {"message", message});

        for (const auto& actorId : NotifyActorIds) {
            Send(actorId, new TEvKqpWarmupComplete(success, message, EntriesLoaded, EntriesFailed));
        }

        PassAway();
    }

    static constexpr TDuration TopologyCheckInterval = TDuration::MilliSeconds(500);
    static constexpr ui32 MaxFetchAttempts = 3;
    static constexpr TDuration FetchRetryDelay = TDuration::Seconds(1);

    const TKqpWarmupConfig Config;

    const TString Database;
    const TString Cluster;

    const TVector<NActors::TActorId> NotifyActorIds;

    TIntrusivePtr<TKqpCounters> Counters;

    std::deque<TEvPrivate::TQueryToCompile> QueriesToCompile;
    THashMap<ui64, TEvPrivate::TQueryToCompile> PendingQueriesByCookie;
    ui64 NextCookie = 0;
    ui32 PendingCompilations = 0;
    ui32 FetchAttempts = 0;
    ui32 EntriesLoaded = 0;
    ui32 EntriesFailed = 0;
    ui32 MaxConcurrentCompilations = 1;
    bool Completed = false;
    bool SoftDeadlineReached = false;
    NActors::TSchedulerCookieHolder SoftDeadlineCookieHolder;
    TInstant HardDeadlineTimestamp;
    TString SkipReason;
    TVector<ui32> NodeIds;
};

NActors::IActor* CreateKqpWarmupActor(
    const TKqpWarmupConfig& config,
    const TString& database,
    const TString& cluster,
    TVector<NActors::TActorId> notifyActorIds)
{
    return new TKqpCompileCacheWarmupActor(config, database, cluster, std::move(notifyActorIds));
}

} // namespace NKikimr::NKqp
