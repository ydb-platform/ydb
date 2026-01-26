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

namespace NKikimr::NKqp {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE, LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE, LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE, LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE, LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE, LogPrefix() << stream)

struct TEvPrivate {
    enum EEv {
        EvFetchCacheResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvDelayedComplete,
        EvHardDeadline,
        EvSoftDeadline,
    };

    struct TQueryToCompile {
        TString QueryText;
        TString UserSID;
        ui64 CompilationDurationMs = 0;
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
};

class TFetchCacheActor : public TQueryBase {
public:
    TFetchCacheActor(const TString& database, ui32 selfNodeId, ui32 maxQueriesToLoad, ui64 maxCompilationDurationMs)
        : TQueryBase(NKikimrServices::KQP_COMPILE_SERVICE, {}, database, true, true)
        , SelfNodeId(selfNodeId)
        , MaxQueriesToLoad(maxQueriesToLoad)
        , MaxCompilationDurationMs(maxCompilationDurationMs)
    {}

    void OnRunQuery() override {
        const auto limit = std::max<ui32>(1u, MaxQueriesToLoad);
        TString sql = TStringBuilder()
            << "SELECT Query, UserSID, SUM(AccessCount) AS AccessCount, MAX(CompilationDuration) AS CompilationDuration"
            << " FROM `" << Database << "/.sys/compile_cache_queries` "
            << "WHERE IsTruncated = false "
            << "  AND AccessCount > 0 "
            << "  AND CompilationDuration < " << MaxCompilationDurationMs << " "
            << "GROUP BY Query, UserSID "
            << "ORDER BY AccessCount DESC "
            << "LIMIT " << limit;

        RunStreamQuery(sql);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto queryText = parser.ColumnParser("Query").GetOptionalUtf8();
            auto userSID = parser.ColumnParser("UserSID").GetOptionalUtf8();
            auto compilationDuration = parser.ColumnParser("CompilationDuration").GetOptionalUint64();

            if (queryText && !queryText->empty()) {
                TEvPrivate::TQueryToCompile query;
                query.QueryText = TString(*queryText);
                query.UserSID = userSID ? TString(*userSID) : TString();
                query.CompilationDurationMs = compilationDuration.value_or(0);
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
    [[maybe_unused]] ui32 SelfNodeId;
    ui32 MaxQueriesToLoad;
    ui64 MaxCompilationDurationMs;
    std::unique_ptr<TEvPrivate::TEvFetchCacheResult> Result = std::make_unique<TEvPrivate::TEvFetchCacheResult>(false);
};


class TKqpCompileCacheWarmupActor : public NActors::TActorBootstrapped<TKqpCompileCacheWarmupActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPILE_SERVICE;
    }

    TKqpCompileCacheWarmupActor(const TKqpWarmupConfig& config, const TString& database,
                           const TString& cluster, NActors::TActorId notifyActorId)
        : Config(config)
        , Database(database)
        , Cluster(cluster)
        , NotifyActorId(notifyActorId)
    {}

    void Bootstrap() {
        Counters = MakeIntrusive<TKqpCounters>(AppData()->Counters, &TlsActivationContext->AsActorContext());
        const auto softDeadline = Config.Deadline;
        const auto hardDeadline = std::max(Config.HardDeadline, softDeadline);
        MaxConcurrentCompilations = std::max<ui32>(1u, Config.MaxConcurrentCompilations);

        LOG_I("Warmup actor started, database: " << Database 
              << ", softDeadline: " << softDeadline
              << ", hardDeadline: " << hardDeadline
              << (Config.HardDeadline < softDeadline ? " (adjusted from " + ToString(Config.HardDeadline) + ")" : "")
              << ", maxConcurrent: " << MaxConcurrentCompilations
              << (Config.MaxConcurrentCompilations == 0 ? " (adjusted from 0)" : "")
              << ", waiting for TEvStartWarmup from KqpProxy");

        Schedule(hardDeadline, new TEvPrivate::TEvHardDeadline());

        if (Database.empty()) {
            LOG_I("Database is empty, skipping warmup");
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
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            LOG_W("StateWaitingComplete: unexpected event " << ev->GetTypeRewrite());
            break;
        }
    }

    STFUNC(StateWaitingStart) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStartWarmup, HandleStartWarmup);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            LOG_W("StateWaitingStart: unexpected event " << ev->GetTypeRewrite());
            break;
        }
    }

    STFUNC(StateFetching) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvFetchCacheResult, HandleFetchResult);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(TEvPrivate::EvSoftDeadline, HandleSoftDeadline);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            LOG_W("StateFetching: unexpected event " << ev->GetTypeRewrite());
            break;
        }
    }

    STFUNC(StateCompiling) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqp::TEvQueryResponse, HandleQueryResponse);
            cFunc(TEvPrivate::EvHardDeadline, HandleHardDeadline);
            cFunc(TEvPrivate::EvSoftDeadline, HandleSoftDeadline);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            LOG_W("StateCompiling: unexpected event " << ev->GetTypeRewrite());
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

    void HandleStartWarmup(TEvStartWarmup::TPtr& ev) {
        const auto discoveredNodes = ev->Get()->DiscoveredNodesCount;
        if (discoveredNodes <= 1) {
            LOG_I("Received TEvStartWarmup with single node, skipping warmup");
            Complete(true, "Skipped: single node");
            return;
        }

        LOG_I("Received TEvStartWarmup, discovered nodes: " << discoveredNodes
              << ", scheduling soft deadline: " << Config.Deadline);
        Schedule(Config.Deadline, new TEvPrivate::TEvSoftDeadline());
        StartFetch();
    }

    void StartFetch() {
        LOG_I("Spawning fetch cache actor");
        const ui64 maxCompilationMs = Config.Deadline.MilliSeconds() / 2;
        Register(new TFetchCacheActor(Database, SelfId().NodeId(), Config.MaxQueriesToLoad, maxCompilationMs));
        Become(&TThis::StateFetching);
    }

    void HandleFetchResult(TEvPrivate::TEvFetchCacheResult::TPtr& ev) {
        auto* result = ev->Get();
        
        if (!result->Success) {
            LOG_I("Fetch failed, skipping warmup: " << result->Error);
            Complete(true, "Skipped: fetch failed");
            return;
        }

        QueriesToCompile = std::move(result->Queries);
        LOG_I("Fetched " << QueriesToCompile.size() << " queries from compile cache");

        if (Counters) {
            Counters->WarmupQueriesFetched->Add(QueriesToCompile.size());
        }

        if (QueriesToCompile.empty()) {
            Complete(true, "No queries to warm up");
            return;
        }

        Become(&TThis::StateCompiling);
        StartCompilations();
    }

    void HandleQueryResponse(TEvKqp::TEvQueryResponse::TPtr& ev) {
        PendingCompilations--;
        
        const auto& record = ev->Get()->Record;
        bool success = (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS);
        
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

    static std::unique_ptr<TEvKqp::TEvQueryRequest> CreatePrepareRequest(
        const TString& database,
        const TString& queryText,
        const TString& userSid,
        TDuration timeout)
    {
        auto queryEv = std::make_unique<TEvKqp::TEvQueryRequest>();
        auto& record = queryEv->Record;
        // compile for each user separately
        auto userToken = MakeIntrusive<NACLib::TUserToken>(userSid, TVector<NACLib::TSID>{});
        record.SetUserToken(userToken->SerializeAsString());
        
        auto& request = *record.MutableRequest();
        request.SetDatabase(database);
        request.SetQuery(queryText);
        request.SetAction(NKikimrKqp::QUERY_ACTION_PREPARE);
        request.SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request.SetKeepSession(false);
        request.SetTimeoutMs(timeout.MilliSeconds());
        request.SetIsInternalCall(true);
        
        return queryEv;
    }

    void SendPrepareRequest(const TEvPrivate::TQueryToCompile& query) {
        LOG_T("Sending PREPARE request for user: " << query.UserSID
              << ", query length: " << query.QueryText.size());
        
        Send(MakeKqpProxyID(SelfId().NodeId()), 
             CreatePrepareRequest(Database, query.QueryText, query.UserSID, Config.Deadline).release());
    }

    void StartCompilations() {
        while (PendingCompilations < MaxConcurrentCompilations && !QueriesToCompile.empty()) {
            auto query = std::move(QueriesToCompile.front());
            QueriesToCompile.pop_front();

            SendPrepareRequest(query);
            PendingCompilations++;
        }

        if (PendingCompilations == 0 && QueriesToCompile.empty()) {
            LOG_I("All compilations finished, loaded: " << EntriesLoaded 
                  << ", failed: " << EntriesFailed);
            Complete(true, TStringBuilder() << "Compiled " << EntriesLoaded << " queries");
        }
    }

    void HandleHardDeadline() {
        LOG_I("Hard deadline reached (waited too long for discovery), compiled: " << EntriesLoaded
              << ", failed: " << EntriesFailed
              << ", pending: " << PendingCompilations);
        Complete(false, "Hard deadline exceeded (discovery not ready in time)");
    }

    void HandleSoftDeadline() {
        LOG_I("Soft deadline reached, compiled: " << EntriesLoaded
              << ", failed: " << EntriesFailed
              << ", pending: " << PendingCompilations);
        Complete(false, "Warmup deadline exceeded");
    }

    void HandlePoison() {
        LOG_D("Received poison, stop warmup");
        PassAway();
    }

    void Complete(bool success, const TString& message) {
        if (Completed) {
            return;
        }
        Completed = true;

        LOG_I("Warmup " << (success ? "completed" : "finished") << ": " << message);

        if (Counters) {
            Counters->WarmupFinished->Inc();
        }

        if (NotifyActorId) {
            Send(NotifyActorId, new TEvKqpWarmupComplete(success, message, EntriesLoaded));
        }

        PassAway();
    }


    const TKqpWarmupConfig Config;

    const TString Database;
    const TString Cluster;

    const NActors::TActorId NotifyActorId;

    TIntrusivePtr<TKqpCounters> Counters;

    std::deque<TEvPrivate::TQueryToCompile> QueriesToCompile;
    ui32 PendingCompilations = 0;
    ui32 EntriesLoaded = 0;
    ui32 EntriesFailed = 0;
    ui32 MaxConcurrentCompilations = 1;
    bool Completed = false;
    TString SkipReason;
};

NActors::IActor* CreateKqpWarmupActor(
    const TKqpWarmupConfig& config,
    const TString& database,
    const TString& cluster,
    NActors::TActorId notifyActorId)
{
    return new TKqpCompileCacheWarmupActor(config, database, cluster, notifyActorId);
}

} // namespace NKikimr::NKqp
