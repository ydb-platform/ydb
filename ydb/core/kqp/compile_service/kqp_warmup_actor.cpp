#include "kqp_warmup_actor.h"

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

// Event for child actor completion notification
struct TEvPrivate {
    enum EEv {
        EvCompileResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvFetchCacheResult,
    };

    struct TEvCompileResult : public NActors::TEventLocal<TEvCompileResult, EvCompileResult> {
        bool Success;
        TString Error;

        TEvCompileResult(bool success, TString error = {})
            : Success(success)
            , Error(std::move(error))
        {}
    };

    struct TEvFetchCacheResult : public NActors::TEventLocal<TEvFetchCacheResult, EvFetchCacheResult> {
        bool Success;
        TString Error;
        std::deque<std::pair<TString, TString>> Queries; // QueryText, UserSid

        TEvFetchCacheResult(bool success, TString error = {})
            : Success(success)
            , Error(std::move(error))
        {}
    };
};

// Child actor for fetching compile cache (uses streaming)
class TFetchCacheActor : public TQueryBase {
public:
    TFetchCacheActor(const TString& database, ui32 selfNodeId)
        : TQueryBase(NKikimrServices::KQP_COMPILE_SERVICE, 
                     /*sessionId=*/{}, database, /*isSystemUser=*/true, /*isStreamingMode=*/true)
        , SelfNodeId(selfNodeId)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder()
            << "SELECT Query, UserSID FROM `" << Database << "/.sys/compile_cache_queries` "
            << "WHERE NodeId != " << SelfNodeId
            << " LIMIT 1000";

        RunStreamQuery(sql);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto queryText = parser.ColumnParser("Query").GetOptionalUtf8();
            auto userSid = parser.ColumnParser("UserSID").GetOptionalUtf8();

            if (queryText && !queryText->empty()) {
                Result->Queries.push_back({
                    TString(*queryText),
                    TString(userSid.value_or(""))
                });
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
    ui32 SelfNodeId;
    std::unique_ptr<TEvPrivate::TEvFetchCacheResult> Result = std::make_unique<TEvPrivate::TEvFetchCacheResult>(false);
};

// Child actor for compiling a single query
class TCompileQueryActor : public TQueryBase {
public:
    TCompileQueryActor(const TString& database, const TString& queryText)
        : TQueryBase(NKikimrServices::KQP_COMPILE_SERVICE, 
                     /*sessionId=*/{}, database, /*isSystemUser=*/true)
        , QueryText(queryText)
    {}

    void OnRunQuery() override {
        RunDataQuery(QueryText, nullptr, TTxControl::BeginAndCommitTx());
    }

    void OnQueryResult() override {
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        bool success = (status == Ydb::StatusIds::SUCCESS);
        TString error = success ? TString() : issues.ToString();
        Send(Owner, new TEvPrivate::TEvCompileResult(success, std::move(error)));
    }

private:
    TString QueryText;
};

// Main warmup orchestrator actor
class TKqpCompileCacheWarmup : public NActors::TActorBootstrapped<TKqpCompileCacheWarmup> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPILE_SERVICE;
    }

    TKqpCompileCacheWarmup(const TKqpWarmupConfig& config, NActors::TActorId notifyActorId,
                           const TString& database)
        : Config(config)
        , NotifyActorId(notifyActorId)
        , Database(database)
    {}

    void Bootstrap() {
        LOG_I("Starting warmup, database: " << Database 
              << ", deadline: " << Config.Deadline
              << ", maxConcurrent: " << Config.MaxConcurrentCompilations);

        if (Config.Deadline != TDuration::Zero()) {
            Schedule(Config.Deadline, new NActors::TEvents::TEvWakeup());
        }

        if (!Config.CompileCacheWarmupEnabled) {
            LOG_I("Compile cache warmup disabled");
            Complete(true, "Compile cache warmup disabled");
            return;
        }

        LOG_I("Spawning fetch cache actor");
        Register(new TFetchCacheActor(Database, SelfId().NodeId()));
        Become(&TThis::StateFetching);
    }

private:
    STFUNC(StateFetching) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvFetchCacheResult, HandleFetchResult);
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        default:
            LOG_W("StateFetching: unexpected event " << ev->GetTypeRewrite());
            break;
        }
    }

    STFUNC(StateCompiling) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvCompileResult, HandleCompileResult);
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
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

    void HandleFetchResult(TEvPrivate::TEvFetchCacheResult::TPtr& ev) {
        auto* result = ev->Get();
        
        if (!result->Success) {
            LOG_W("Failed to fetch compile cache: " << result->Error);
            Complete(false, TStringBuilder() << "Fetch cache failed: " << result->Error);
            return;
        }

        QueriesToCompile = std::move(result->Queries);
        LOG_I("Fetched " << QueriesToCompile.size() << " queries from compile cache");
        for (const auto& [queryText, userSid] : QueriesToCompile) {
            LOG_D("Query: " << queryText << ", UserSID: " << userSid);
        }

        if (QueriesToCompile.empty()) {
            Complete(true, "No queries to warm up");
            return;
        }

        Become(&TThis::StateCompiling);
        StartCompilations();
    }

    void HandleCompileResult(TEvPrivate::TEvCompileResult::TPtr& ev) {
        PendingCompilations--;
        
        if (ev->Get()->Success) {
            EntriesLoaded++;
            LOG_D("Query compiled, total: " << EntriesLoaded 
                  << ", pending: " << PendingCompilations
                  << ", remaining: " << QueriesToCompile.size());
        } else {
            EntriesFailed++;
            LOG_D("Query compile failed: " << ev->Get()->Error
                  << ", failed total: " << EntriesFailed);
        }

        StartCompilations();
    }

    void StartCompilations() {
        while (PendingCompilations < Config.MaxConcurrentCompilations && !QueriesToCompile.empty()) {
            auto [queryText, userSid] = std::move(QueriesToCompile.front());
            QueriesToCompile.pop_front();

            LOG_T("Starting compilation, query length: " << queryText.size());
            Register(new TCompileQueryActor(Database, queryText));
            PendingCompilations++;
        }

        if (PendingCompilations == 0 && QueriesToCompile.empty()) {
            LOG_I("All compilations finished, loaded: " << EntriesLoaded << ", failed: " << EntriesFailed);
            Complete(true, TStringBuilder() << "Compiled " << EntriesLoaded << " queries");
        }
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        LOG_W("Warmup deadline reached, compiled: " << EntriesLoaded 
              << ", failed: " << EntriesFailed
              << ", pending: " << PendingCompilations);
        Complete(false, "Warmup deadline exceeded");
    }

    void HandlePoison() {
        LOG_D("Received poison pill");
        PassAway();
    }

    void Complete(bool success, const TString& message) {
        if (Completed) {
            return;
        }
        Completed = true;

        LOG_I("Warmup " << (success ? "completed" : "finished") << ": " << message);

        if (NotifyActorId) {
            Send(NotifyActorId, new TEvKqpWarmupComplete(success, message, EntriesLoaded));
        }

        PassAway();
    }


    const TKqpWarmupConfig Config;
    const NActors::TActorId NotifyActorId;
    const TString Database;

    std::deque<std::pair<TString, TString>> QueriesToCompile; // (QueryText, UserSid)
    ui32 PendingCompilations = 0;
    ui32 EntriesLoaded = 0;
    ui32 EntriesFailed = 0;
    bool Completed = false;
};

NActors::IActor* CreateKqpWarmupActor(
    const TKqpWarmupConfig& config,
    NActors::TActorId notifyActorId,
    const TString& database)
{
    return new TKqpCompileCacheWarmup(config, notifyActorId, database);
}

} // namespace NKikimr::NKqp
