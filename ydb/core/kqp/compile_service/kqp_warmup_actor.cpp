#include "kqp_warmup_actor.h"

#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
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
        EvFetchCacheResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    };

    struct TQueryToCompile {
        TString QueryText;
        TString UserSID;
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
};

class TFetchCacheActor : public TQueryBase {
public:
    TFetchCacheActor(const TString& database, ui32 selfNodeId)
        : TQueryBase(NKikimrServices::KQP_COMPILE_SERVICE, 
                     /*sessionId=*/{}, database, /*isSystemUser=*/true, /*isStreamingMode=*/true)
        , SelfNodeId(selfNodeId)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder()
            << "SELECT Query, UserSID, AccessCount"
            << " FROM `" << Database << "/.sys/compile_cache_queries` "
            // << "WHERE NodeId != " << SelfNodeId << " "
            // << "GROUP BY Query, UserSID"
            << "ORDER BY AccessCount DESC "
            << "LIMIT 1000";

        RunStreamQuery(sql);
    }

    void OnStreamResult(NYdb::TResultSet&& resultSet) override {
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto queryText = parser.ColumnParser("Query").GetOptionalUtf8();
            auto userSID = parser.ColumnParser("UserSID").GetOptionalUtf8();

            if (queryText && !queryText->empty() && userSID && !userSID->empty()) {
                Cerr << "Got Query: " << *queryText << ", UserSID: " << *userSID << Endl;
                TEvPrivate::TQueryToCompile query;
                query.QueryText = TString(*queryText);
                query.UserSID = TString(*userSID);
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
    std::unique_ptr<TEvPrivate::TEvFetchCacheResult> Result = std::make_unique<TEvPrivate::TEvFetchCacheResult>(false);
};


class TKqpCompileCacheWarmup : public NActors::TActorBootstrapped<TKqpCompileCacheWarmup> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPILE_SERVICE;
    }

    TKqpCompileCacheWarmup(const TKqpWarmupConfig& config, NActors::TActorId notifyActorId,
                           const TString& database, const TString& cluster)
        : Config(config)
        , NotifyActorId(notifyActorId)
        , Database(database)
        , Cluster(cluster)
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
            hFunc(TEvKqp::TEvQueryResponse, HandleQueryResponse);
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
        for (const auto& query : QueriesToCompile) {
            LOG_D("Query length: " << query.QueryText.size() << ", UserSID: " << query.UserSID);
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
            LOG_D("Query compiled, total: " << EntriesLoaded 
                  << ", pending: " << PendingCompilations
                  << ", remaining: " << QueriesToCompile.size());
        } else {
            EntriesFailed++;
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
            LOG_D("Query compile failed: " << issues.ToString()
                  << ", failed total: " << EntriesFailed);
        }

        StartCompilations();
    }

    static std::unique_ptr<TEvKqp::TEvQueryRequest> CreatePrepareRequest(
        const TString& database,
        const TString& queryText,
        const TString& userSid)
    {
        auto queryEv = std::make_unique<TEvKqp::TEvQueryRequest>();
        auto& record = queryEv->Record;
        
        auto userToken = MakeIntrusive<NACLib::TUserToken>(userSid, TVector<NACLib::TSID>{});
        record.SetUserToken(userToken->SerializeAsString());
        
        auto& request = *record.MutableRequest();
        request.SetDatabase(database);
        request.SetQuery(queryText);
        request.SetAction(NKikimrKqp::QUERY_ACTION_PREPARE);
        request.SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request.SetKeepSession(false);
        request.SetTimeoutMs(30000);
        
        return queryEv;
    }

    void SendPrepareRequest(const TEvPrivate::TQueryToCompile& query) {
        LOG_T("Sending PREPARE request for user: " << query.UserSID
              << ", query length: " << query.QueryText.size());
        
        Send(MakeKqpProxyID(SelfId().NodeId()), 
             CreatePrepareRequest(Database, query.QueryText, query.UserSID).release());
    }

    void StartCompilations() {
        while (PendingCompilations < Config.MaxConcurrentCompilations && !QueriesToCompile.empty()) {
            auto query = std::move(QueriesToCompile.front());
            QueriesToCompile.pop_front();

            SendPrepareRequest(query);
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
    const TString Cluster;

    std::deque<TEvPrivate::TQueryToCompile> QueriesToCompile;
    ui32 PendingCompilations = 0;
    ui32 EntriesLoaded = 0;
    ui32 EntriesFailed = 0;
    bool Completed = false;
};

NActors::IActor* CreateKqpWarmupActor(
    const TKqpWarmupConfig& config,
    NActors::TActorId notifyActorId,
    const TString& database,
    const TString& cluster)
{
    return new TKqpCompileCacheWarmup(config, notifyActorId, database, cluster);
}

} // namespace NKikimr::NKqp
