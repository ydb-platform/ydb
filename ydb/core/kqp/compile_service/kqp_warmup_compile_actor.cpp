#include "kqp_warmup_compile_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
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
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_COMPILE_SERVICE


namespace NKikimr::NKqp {

struct TEvPrivate {
    enum EEv {
        EvFetchCacheResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvDelayedComplete,
        EvHardDeadline,
        EvSoftDeadline,
        EvTruncatedCountResult,
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
        std::deque<TQueryToCompile> Queries;

        TEvFetchCacheResult(bool success, TString error = {})
            : Success(success)
            , Error(std::move(error))
        {}
    };

    struct TEvDelayedComplete : public NActors::TEventLocal<TEvDelayedComplete, EvDelayedComplete> {};
    struct TEvHardDeadline : public NActors::TEventLocal<TEvHardDeadline, EvHardDeadline> {};
    struct TEvSoftDeadline : public NActors::TEventLocal<TEvSoftDeadline, EvSoftDeadline> {};

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

/*
    Helper actor to fetch and parse data from /.sys/compile_cache_queries.
    See TFetchCacheActor::OnRunQuery() for query constraints on cache records.
*/
class TFetchCacheActor : public TQueryBase {
public:
    TFetchCacheActor(const TString& database, ui32 maxQueriesToLoad, ui64 maxCompilationDurationMs, const TVector<ui32>& nodeIds, ui32 maxNodesToQuery)
        : TQueryBase(NKikimrServices::KQP_COMPILE_SERVICE, {}, database, true, true)
        , MaxQueriesToLoad(maxQueriesToLoad)
        , MaxCompilationDurationMs(maxCompilationDurationMs)
        , NodeIds(nodeIds)
        , MaxNodesToRequest(maxNodesToQuery)
    {}

    void OnRunQuery() override {
        const auto limit = std::max<ui32>(1u, MaxQueriesToLoad);
        TStringBuilder sql;
        sql << "SELECT Query, UserSID, MAX(Metadata) AS Metadata, SUM(AccessCount) AS AccessCount, "
            << "MAX(CompilationDurationMs) AS CompilationDurationMs, MAX(QueryType) AS QueryType, MAX(Syntax) AS Syntax"
            << " FROM `" << Database << "/.sys/compile_cache_queries` "
            << "WHERE IsTruncated = false "
            << "AND AccessCount > 0 "
            << "AND QueryType IS NOT NULL AND QueryType != '' "
            << "AND CompilationDurationMs < " << MaxCompilationDurationMs;

        if (!NodeIds.empty()) {
            ui32 nodesToQuery = std::min<ui32>(MaxNodesToRequest, NodeIds.size());
            sql << "  AND NodeId IN (";
            for (ui32 i = 0; i < nodesToQuery; ++i) {
                if (i > 0) sql << ", ";
                sql << NodeIds[i];
            }
            sql << ")";
        }

        sql << " GROUP BY Query, UserSID, QueryType, Syntax "
            << "ORDER BY AccessCount DESC "
            << "LIMIT " << limit;

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
        }
        Send(Owner, Result.release());
    }

private:
    ui32 MaxQueriesToLoad;
    ui64 MaxCompilationDurationMs;
    TVector<ui32> NodeIds;
    ui32 MaxNodesToRequest;
    std::unique_ptr<TEvPrivate::TEvFetchCacheResult> Result = std::make_unique<TEvPrivate::TEvFetchCacheResult>(false);
};

/*
    Helper actor to count truncated queries in /.sys/compile_cache_queries.
    Runs independently from the main fetch, result is used for observability only.
*/
class TFetchTruncatedCountActor : public TQueryBase {
public:
    TFetchTruncatedCountActor(const TString& database, const TVector<ui32>& nodeIds, ui32 maxNodesToQuery)
        : TQueryBase(NKikimrServices::KQP_COMPILE_SERVICE, {}, database, true, true)
        , NodeIds(nodeIds)
        , MaxNodesToRequest(maxNodesToQuery)
    {}

    void OnRunQuery() override {
        TStringBuilder sql;
        sql << "SELECT SUM(CASE WHEN IsTruncated = true THEN 1 ELSE 0 END) AS TruncatedCount,"
            << " SUM(CASE WHEN QueryType IS NULL OR QueryType = '' THEN 1 ELSE 0 END) AS EmptyQueryTypeCount"
            << " FROM `" << Database << "/.sys/compile_cache_queries`"
            << " WHERE AccessCount > 0";

        if (!NodeIds.empty()) {
            ui32 nodesToQuery = std::min<ui32>(MaxNodesToRequest, NodeIds.size());
            sql << " AND NodeId IN (";
            for (ui32 i = 0; i < nodesToQuery; ++i) {
                if (i > 0) sql << ", ";
                sql << NodeIds[i];
            }
            sql << ")";
        }

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
    ui32 MaxNodesToRequest;
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


/*
    Compile warmup actor runs before the node is registered and ready to serve queries.
    The main goal is to compile popular queries before node starts to avoid execution time drops
    during the first moments of node work.

    Timer logic:
    1. HardDeadline (from Bootstrap): absolute maximum time for actor lifetime across all states.
       Triggered in any state to forcefully terminate warmup (success=false).
    2. SoftDeadline has two roles:
       - (from Bootstrap): timeout for waiting TEvStartWarmup from KqpProxy.
         If peer nodes are not discovered within this time, warmup completes early (success=false).
       - (from HandleStartWarmup): timeout for fetching and compiling queries.
         When reached, stops submitting new compilations but waits for in-flight ones to finish.
         HardDeadline acts as a safety net if in-flight compilations hang.

    Both SoftDeadline and HardDeadline are configured in WarmupConfig.
*/
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

        YDB_LOG_INFO("Warmup actor started,, waiting for TEvStartWarmup from KqpProxy",
            {"LogPrefix", LogPrefix()},
            {"database", Database},
            {"softDeadline", softDeadline},
            {"hardDeadline", hardDeadline},
            {"#_num_0", (Config.HardDeadline < softDeadline ? " (adjusted from " + ToString(Config.HardDeadline) + ")" : "")},
            {"maxConcurrent", MaxConcurrentCompilations},
            {"#_num_1", (Config.MaxConcurrentCompilations == 0 ? " (adjusted from 0)" : "")});

        Schedule(hardDeadline, new TEvPrivate::TEvHardDeadline());
        SoftDeadlineCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
        Schedule(softDeadline, new TEvPrivate::TEvSoftDeadline(), SoftDeadlineCookieHolder.Get());

        if (Database.empty()) {
            YDB_LOG_INFO("Database is empty, skipping warmup",
                {"LogPrefix", LogPrefix()});
            SkipReason = "Skipped: empty database";
            ScheduleComplete();
            return;
        }

        Become(&TThis::StateWaitingStart);
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
            hFunc(TEvPrivate::TEvTruncatedCountResult, HandleTruncatedCount);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            YDB_LOG_WARN("StateWaitingComplete: unexpected event",
                {"LogPrefix", LogPrefix()},
                {"GetTypeRewrite", ev->GetTypeRewrite()});
            break;
        }
    }

    STFUNC(StateWaitingStart) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStartWarmup, HandleStartWarmup);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(TEvPrivate::EvSoftDeadline, HandleSoftDeadlineInWaitingStart);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            YDB_LOG_WARN("StateWaitingStart: unexpected event",
                {"LogPrefix", LogPrefix()},
                {"GetTypeRewrite", ev->GetTypeRewrite()});
            break;
        }
    }

    STFUNC(StateFetching) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvFetchCacheResult, HandleFetchResult);
            hFunc(TEvPrivate::TEvTruncatedCountResult, HandleTruncatedCount);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(TEvPrivate::EvSoftDeadline, HandleSoftDeadline);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            YDB_LOG_WARN("StateFetching: unexpected event",
                {"LogPrefix", LogPrefix()},
                {"GetTypeRewrite", ev->GetTypeRewrite()});
            break;
        }
    }
    STFUNC(StateCompiling) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqp::TEvQueryResponse, HandleQueryResponse);
            hFunc(TEvPrivate::TEvTruncatedCountResult, HandleTruncatedCount);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(TEvPrivate::EvSoftDeadline, HandleSoftDeadline);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            YDB_LOG_WARN("StateCompiling: unexpected event",
                {"LogPrefix", LogPrefix()},
                {"GetTypeRewrite", ev->GetTypeRewrite()});
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
        YDB_LOG_INFO("Truncated queries, empty",
            {"LogPrefix", LogPrefix()},
            {"in_cache", ev->Get()->Count},
            {"QueryType", ev->Get()->EmptyQueryTypeCount},
            {"success", ev->Get()->Success});
    }

    void HandleStartWarmup(TEvStartWarmup::TPtr& ev) {
        const auto discoveredNodes = ev->Get()->DiscoveredNodesCount;
        NodeIds = ev->Get()->NodeIds;
        if (discoveredNodes <= 1) {
            YDB_LOG_INFO("Received TEvStartWarmup with single node, skipping warmup",
                {"LogPrefix", LogPrefix()});
            Complete(true, "Skipped: single node");
            return;
        }

        YDB_LOG_INFO("Received TEvStartWarmup, discovered, nodeIds, scheduling soft",
            {"LogPrefix", LogPrefix()},
            {"nodes", discoveredNodes},
            {"count", NodeIds.size()},
            {"maxNodesToQuery", Config.MaxNodesToRequest},
            {"deadline", Config.SoftDeadline});
        // Cancel bootstrap soft deadline (discovery wait) and schedule compilation soft deadline
        SoftDeadlineCookieHolder.Detach();
        SoftDeadlineCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
        Schedule(Config.SoftDeadline, new TEvPrivate::TEvSoftDeadline(), SoftDeadlineCookieHolder.Get());
        StartFetch();
    }

    void StartFetch() {
        ui32 maxNodesToQuery = Config.MaxNodesToRequest;
        if (maxNodesToQuery == 0) {
            maxNodesToQuery = NodeIds.size();
        }

        if (maxNodesToQuery < NodeIds.size()) {
            PartialShuffle(NodeIds.begin(), NodeIds.end(), maxNodesToQuery, *AppData()->RandomProvider);
        }

        YDB_LOG_INFO("Spawning fetch cache actor, filtering by nodes",
            {"LogPrefix", LogPrefix()},
            {"#_std::min<size_t>(maxNodesToQuery, NodeIds.size())", std::min<size_t>(maxNodesToQuery, NodeIds.size())});
        const ui64 maxCompilationMs = Config.MaxCompilationDurationMs > 0
            ? Config.MaxCompilationDurationMs
            : Config.SoftDeadline.MilliSeconds() / 2;
        Register(new TFetchCacheActor(Database, Config.MaxQueriesToLoad, maxCompilationMs, NodeIds, maxNodesToQuery));
        Register(new TFetchTruncatedCountActor(Database, NodeIds, maxNodesToQuery));
        Become(&TThis::StateFetching);
    }

    void HandleFetchResult(TEvPrivate::TEvFetchCacheResult::TPtr& ev) {
        auto* result = ev->Get();

        if (!result->Success) {
            YDB_LOG_WARN("Fetch failed, skipping",
                {"LogPrefix", LogPrefix()},
                {"warmup", result->Error});
            Complete(false, "Fetch failed: " + result->Error);
            return;
        }

        QueriesToCompile = std::move(result->Queries);
        YDB_LOG_INFO("Fetched queries from compile cache",
            {"LogPrefix", LogPrefix()},
            {"size", QueriesToCompile.size()});

        if (Counters) {
            Counters->WarmupQueriesFetched->Add(QueriesToCompile.size());
        }

        // PG syntax warmup is not supported yet, skip PG queries
        // TODO(anely-d): delete when pg syntax is supported
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
                YDB_LOG_INFO("Query compiled successfully,",
                    {"LogPrefix", LogPrefix()},
                    {"user", query.UserSID},
                    {"has_metadata", !query.Metadata.empty()},
                    {"query", query.QueryText.substr(0, 200)},
                    {"#_num_0", (query.QueryText.size() > 200 ? "..." : "")});
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
                YDB_LOG_WARN("Query compilation failed,",
                    {"LogPrefix", LogPrefix()},
                    {"user", query.UserSID},
                    {"has_metadata", !query.Metadata.empty()},
                    {"status", Ydb::StatusIds::StatusCode_Name(record.GetYdbStatus())},
                    {"error", errorMsg},
                    {"query", query.QueryText.substr(0, 200)},
                    {"#_num_0", (query.QueryText.size() > 200 ? "..." : "")});
            }
            PendingQueriesByCookie.erase(it);
        } else {
            YDB_LOG_WARN("Received response for unknown",
                {"LogPrefix", LogPrefix()},
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

        YDB_LOG_DEBUG("Sending PREPARE request for, query",
            {"LogPrefix", LogPrefix()},
            {"user", query.UserSID},
            {"length", query.QueryText.size()},
            {"has_metadata", !query.Metadata.empty()},
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
            YDB_LOG_INFO("All compilations finished,",
                {"LogPrefix", LogPrefix()},
                {"loaded", EntriesLoaded},
                {"failed", EntriesFailed});
            TString msg = TStringBuilder() << "Compiled " << EntriesLoaded << " queries"
                << (SoftDeadlineReached ? " (soft deadline)" : "");
            Complete(true, msg);
        }
    }


    void HandleHardDeadline() {
        YDB_LOG_INFO("Hard deadline reached,",
            {"LogPrefix", LogPrefix()},
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

        YDB_LOG_INFO("Soft deadline reached,",
            {"LogPrefix", LogPrefix()},
            {"compiled", EntriesLoaded},
            {"failed", EntriesFailed},
            {"pending", PendingCompilations});

        if (PendingCompilations == 0) {
            Complete(true, TStringBuilder() << "Soft deadline: compiled " << EntriesLoaded << " queries");
        } else {
            YDB_LOG_INFO("Waiting for in-flight compilations to finish",
                {"LogPrefix", LogPrefix()},
                {"PendingCompilations", PendingCompilations});
        }
    }

    void HandleSoftDeadlineInWaitingStart() {
        YDB_LOG_INFO("Soft deadline reached while waiting for warmup start signal - no peer nodes discovered, skipping warmup",
            {"LogPrefix", LogPrefix()});
        Complete(false, "Warmup incomplete: no peer nodes discovered within soft deadline");
    }

    void HandlePoison() {
        YDB_LOG_DEBUG("Received poison, stop warmup",
            {"LogPrefix", LogPrefix()});
        PassAway();
    }

    void Complete(bool success, const TString& message) {
        if (Completed) {
            return;
        }
        Completed = true;

        YDB_LOG_INFO("Warmup",
            {"LogPrefix", LogPrefix()},
            {"#_num_0", (success ? "completed" : "finished")},
            {"message", message});

        for (const auto& actorId : NotifyActorIds) {
            Send(actorId, new TEvKqpWarmupComplete(success, message, EntriesLoaded, EntriesFailed));
        }

        PassAway();
    }

    const TKqpWarmupConfig Config;

    const TString Database;
    const TString Cluster;

    const TVector<NActors::TActorId> NotifyActorIds;

    TIntrusivePtr<TKqpCounters> Counters;

    std::deque<TEvPrivate::TQueryToCompile> QueriesToCompile;
    THashMap<ui64, TEvPrivate::TQueryToCompile> PendingQueriesByCookie;
    ui64 NextCookie = 0;
    ui32 PendingCompilations = 0;
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
