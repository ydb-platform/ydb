#include "query_replay.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/json/json_reader.h>

#include <util/digest/city.h>

#include <optional>
#include <unordered_set>

using namespace NYdb;
using namespace NActors;


class TQueryReplayActorRunnerSimple: public TActorBootstrapped<TQueryReplayActorRunnerSimple> {
private:
    std::unordered_set<TActorId, THash<TActorId>> PendingResults;
    std::vector<TString> Queries;
    std::shared_ptr<TQueryReplayStats> Stats;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> ModuleResolverState;

public:
    TQueryReplayActorRunnerSimple(
        std::vector<TString>&& queries,
        std::shared_ptr<TQueryReplayStats> queryReplayStats,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry)
        : Queries(std::move(queries))
        , Stats(queryReplayStats)
        , FunctionRegistry(functionRegistry)
        , ModuleResolverState(MakeIntrusive<NKikimr::NKqp::TModuleResolverState>())
    {
        Y_ABORT_UNLESS(GetYqlDefaultModuleResolver(ModuleResolverState->ExprCtx, ModuleResolverState->ModuleResolver));
    }

    void Bootstrap() {
        Become(&TThis::MainState);
        for(auto& query: Queries) {
            NJson::TJsonValue json = ReadQueryJson(query);
            StartCompilation(std::move(json));
        }
    }

    NJson::TJsonValue ReadQueryJson(const TString& p) {
        static NJson::TJsonReaderConfig readConfig;
        TFileInput in(p);
        NJson::TJsonValue reply;
        NJson::ReadJsonTree(&in, &readConfig, &reply, false);
        return reply;
    }

    void StartCompilation(NJson::TJsonValue&& data) {
        auto actor = CreateQueryCompiler(SelfId(), ModuleResolverState, FunctionRegistry, std::move(data));
        PendingResults.emplace(Register(actor));
    }

    void Handle(TQueryReplayEvents::TEvCompileResponse::TPtr& ev) {
        if (ev->Get()->Success) {
            Stats->OnSuccess();
        } else {
            Stats->OnFailure();
        }
        Stats->AddNew(1);

        auto it = PendingResults.find(ev->Sender);
        Y_ABORT_UNLESS(it != PendingResults.end());
        PendingResults.erase(it);
        if (PendingResults.empty()) {
            Stats->Complete();
        }
    }

    STATEFN(MainState) {
        switch(ev->GetTypeRewrite()) {
           hFunc(TQueryReplayEvents::TEvCompileResponse, Handle);
        }
    }
};

class TQueryReplayActorRunner: public TActorBootstrapped<TQueryReplayActorRunner> {
public:
    TQueryReplayActorRunner(
        NYdb::TDriver& driver, const TString& runId, const TString& queryTablePath, const TString& statsTablePath,
        std::shared_ptr<TQueryReplayStats> queryReplayStats, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        ui32 maxInFlight, ui32 modulo, ui32 shardId)
        : Stats(queryReplayStats)
        , QueryTablePath(queryTablePath)
        , StatsTablePath(statsTablePath)
        , ModuleResolverState(MakeIntrusive<NKikimr::NKqp::TModuleResolverState>())
        , FunctionRegistry(functionRegistry)
        , MaxInFlightSize(maxInFlight)
        , Modulo(modulo)
        , ShardId(shardId)
        , RunId(runId)
    {
        Y_ABORT_UNLESS(GetYqlDefaultModuleResolver(ModuleResolverState->ExprCtx, ModuleResolverState->ModuleResolver));
        Client = std::make_shared<NTable::TTableClient>(driver, NTable::TClientSettings());
    }

    void Bootstrap() {
        Become(&TThis::StateRead);
        StartRead();
    }

    void StartRead() {
        PartIterator.reset();

        TActorId self = SelfId();
        TActorSystem* actorSystem = TlsActivationContext->ExecutorThread.ActorSystem;
        const TString& path = QueryTablePath;
        Client->GetSession().Subscribe([path, self, actorSystem](NTable::TAsyncCreateSessionResult asyncResult) {
            const NTable::TCreateSessionResult& result = asyncResult.GetValue();
            if (!result.IsSuccess()) {
                actorSystem->Send(self, new TQueryReplayEvents::TEvReadFailed());
                return;
            }

            NTable::TSession session = result.GetSession();
            NTable::TReadTableSettings settings = NTable::TReadTableSettings().AppendColumns("query_id").AppendColumns("query_text");
            session.ReadTable(path, settings).Subscribe([self, actorSystem](NTable::TAsyncTablePartIterator asyncResult) {
                NTable::TTablePartIterator partIterator = asyncResult.ExtractValue();
                if (!partIterator.IsSuccess()) {
                    actorSystem->Send(self, new TQueryReplayEvents::TEvReadFailed());
                } else {
                    actorSystem->Send(self, new TQueryReplayEvents::TEvReadStarted(std::move(partIterator)));
                }
            });
        });
    }

    void Continue() {
        TActorId self = SelfId();
        TActorSystem* actorSystem = TlsActivationContext->ExecutorThread.ActorSystem;
        PartIterator->ReadNext().Subscribe([self, actorSystem](NTable::TAsyncSimpleStreamPart<TResultSet> asyncResult) {
            NTable::TSimpleStreamPart<TResultSet> tablePart = asyncResult.ExtractValue();
            if (tablePart.EOS()) {
                actorSystem->Send(self, new TQueryReplayEvents::TEvQueryBatch(std::move(tablePart)));
            } else if (!tablePart.IsSuccess()) {
                actorSystem->Send(self, new TQueryReplayEvents::TEvReadFailed());
            } else {
                actorSystem->Send(self, new TQueryReplayEvents::TEvQueryBatch(std::move(tablePart)));
            }
        });
    }

    void StartQueryReplay(const TString& queryId) {
        NActors::IActor* actor = CreateQueryProcessorActor(Client, RunId, queryId, QueryTablePath, StatsTablePath,
            SelfId(), ModuleResolverState, FunctionRegistry);
        TActorId actorId = Register(actor);
        InFlightQueries.emplace(actorId);
    }

    void ProcessQueriesQueue() {
        while (InFlightQueries.size() < MaxInFlightSize && !WaitingQueriesQueue.empty()) {
            TString queryId = std::move(WaitingQueriesQueue.front());
            WaitingQueriesQueue.pop_front();
            StartQueryReplay(queryId);
        }

        if (InFlightQueries.size() == 0 && CurrentStateFunc() == &TThis::StateWaiting) {
            Stats->Complete();
            PassAway();
            return;
        }
    }

    void Handle(TQueryReplayEvents::TEvQueryBatch::TPtr& ev) {
        if (ev->Get()->TablePart.EOS()) {
            Become(&TThis::StateWaiting);
            ProcessQueriesQueue();
            return;
        }

        const auto& resultSet = ev->Get()->TablePart.GetPart();
        TResultSetParser parser(resultSet);
        Y_ABORT_UNLESS(parser.TryNextRow());

        ui32 NewQueries = 0;
        TValueParser& idParser = parser.ColumnParser("query_id");
        TValueParser& queryText = parser.ColumnParser("query_text");
        do {
            TString queryId = std::move(idParser.GetOptionalString().GetRef());
            if (AllQueries.find(queryId) != AllQueries.end())
                continue;

            ui64 QueryHash = CityHash64(queryText.GetOptionalString().GetRef());

            if (Hashes.find(QueryHash) != Hashes.end() || QueryHash % Modulo != ShardId)
                continue;

            Hashes.emplace(QueryHash);
            ++NewQueries;
            AllQueries.insert(queryId);
            WaitingQueriesQueue.emplace_back(queryId);
        } while (parser.TryNextRow());

        Stats->AddNew(NewQueries);
        Continue();
        ProcessQueriesQueue();
    }

    void Handle(TQueryReplayEvents::TEvReadFailed::TPtr&) {
        StartRead();
    }

    void Handle(TQueryReplayEvents::TEvReadStarted::TPtr& ev) {
        PartIterator = std::move(ev->Get()->PartIterator);
        Continue();
    }

    void Handle(TQueryReplayEvents::TEvQueryProcessorResult::TPtr& ev) {
        if (ev->Get()->Success) {
            Stats->OnSuccess();
        } else {
            Stats->OnFailure();
        }

        InFlightQueries.erase(ev->Sender);
        ProcessQueriesQueue();
    }

    STATEFN(StateRead) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TQueryReplayEvents::TEvQueryBatch, Handle);
            hFunc(TQueryReplayEvents::TEvReadFailed, Handle);
            hFunc(TQueryReplayEvents::TEvReadStarted, Handle);
            hFunc(TQueryReplayEvents::TEvQueryProcessorResult, Handle);
        }
    }

    STATEFN(StateWaiting) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TQueryReplayEvents::TEvQueryProcessorResult, Handle);
        }
    }

private:
    std::shared_ptr<NTable::TTableClient> Client;
    std::deque<TString> WaitingQueriesQueue;
    std::unordered_set<TString> AllQueries;
    std::unordered_set<TActorId, THash<TActorId>> InFlightQueries;
    std::optional<NTable::TTablePartIterator> PartIterator;
    std::shared_ptr<TQueryReplayStats> Stats;
    TString QueryTablePath;
    TString StatsTablePath;
    TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> ModuleResolverState;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    THashSet<ui64> Hashes;
    ui32 MaxInFlightSize;
    ui32 Modulo;
    ui32 ShardId;
    TString RunId;
};

NActors::IActor* CreateQueryReplayActorSimple(
    std::vector<TString>&& queries, std::shared_ptr<TQueryReplayStats> queryReplayStats,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry)
{
    return new TQueryReplayActorRunnerSimple(std::move(queries), queryReplayStats, functionRegistry);
}

NActors::IActor* CreateQueryReplayActor(
    NYdb::TDriver& driver, const TString& runId, const TString& queryTablePath, const TString& statsTablePath,
    std::shared_ptr<TQueryReplayStats> queryReplayStats, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    ui32 maxInFlight, ui32 modulo, ui32 shardId)
{
    return new TQueryReplayActorRunner(driver, runId, queryTablePath, statsTablePath, queryReplayStats, functionRegistry,
        maxInFlight, modulo, shardId);
}
