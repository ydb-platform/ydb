#include "query_replay.h"

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/json/json_reader.h>

#include <util/generic/hash.h>
#include <util/string/escape.h>

using namespace NYdb;
using namespace NActors;
using namespace NKikimr::NKqp;

class TQueryProcessorActor: public TActorBootstrapped<TQueryProcessorActor> {
private:
    TString FetchQuery;
    TString WriteStatsQuery;
    std::shared_ptr<NYdb::NTable::TTableClient> Client;
    TString RunId;
    TString QueryId;
    TActorId OwnerId;
    TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> ModuleResolverState;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;

public:
    TQueryProcessorActor(std::shared_ptr<NYdb::NTable::TTableClient> client, const TString& runId, const TString& queryId, const TString& tablePath,
        const TString& statsTablePath, const TActorId& ownerId, TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> moduleResolverState,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry)
        : Client(client)
        , RunId(runId)
        , QueryId(queryId)
        , OwnerId(ownerId)
        , ModuleResolverState(moduleResolverState)
        , FunctionRegistry(functionRegistry)
    {
        FetchQuery = Sprintf(
            R"(
                --!syntax_v1
                DECLARE $query_id as String;
                SELECT * FROM `%s` WHERE query_id=$query_id;
            )", tablePath.c_str());

        WriteStatsQuery = Sprintf(
            R"(
                DECLARE $run_id as Utf8;
                DECLARE $query_id as Utf8;
                DECLARE $fail_reason as Utf8;
                DECLARE $extra_message as Utf8;
                UPSERT INTO `%s` (RunId, QueryId, Timestamp, FailReason, ExtraMessage)
                    VALUES($run_id, $query_id, YQL::Now(), $fail_reason, $extra_message);
            )", statsTablePath.c_str());
    }

    void Bootstrap() {
        Become(&TThis::StateResolve);
        ResolveQueryData();
    }

    void ResolveQueryData() {
        const TString& query = FetchQuery;
        TActorSystem* actorSystem = TlsActivationContext->ExecutorThread.ActorSystem;
        TActorId self = SelfId();

        NYdb::TParams params = Client->GetParamsBuilder().AddParam("$query_id").String(QueryId).Build().Build();
        Client->RetryOperation([query, params, self, actorSystem](NTable::TSession session) {
            auto txControl = NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx();
            auto queryResult = session.ExecuteDataQuery(query, txControl, std::move(params));

            return queryResult.Apply([self, actorSystem](const NTable::TAsyncDataQueryResult& asyncResult) {
                NTable::TDataQueryResult result = asyncResult.GetValue();
                actorSystem->Send(self, new TQueryReplayEvents::TEvQueryResolveResult(std::move(result)));
                return NThreading::MakeFuture<TStatus>(result);
            });
        });
    }

    void Handle(TQueryReplayEvents::TEvQueryResolveResult::TPtr& ev) {
        if (!ev->Get()->DataQueryResult.IsSuccess()) {
            Cerr << "Failed to resolve query data " << ev->Get()->DataQueryResult.GetIssues().ToString() << Endl;
            Send(OwnerId, new TQueryReplayEvents::TEvQueryProcessorResult(false));
            PassAway();
            return;
        }

        const auto& resultSet = ev->Get()->DataQueryResult.GetResultSet(0);
        TResultSetParser parser(resultSet);
        if (!parser.TryNextRow()) {
            Cout << "Query data not foud " << QueryId << Endl;
            Send(OwnerId, new TQueryReplayEvents::TEvQueryProcessorResult(true));
            PassAway();
            return;
        }

        NJson::TJsonValue json(NJson::JSON_MAP);
        for(auto& col: resultSet.GetColumnsMeta()) {
            if (col.Name == "_logfeller_timestamp")
                continue;

            TString value = parser.ColumnParser(col.Name).GetOptionalString().GetRef();
            json.InsertValue(col.Name, NJson::TJsonValue(std::move(value)));
        }

        auto compiler = CreateQueryCompiler(SelfId(), ModuleResolverState, FunctionRegistry, std::move(json));
        Register(compiler);
        Become(&TThis::StateCompiling);
    }

    void WriteQueryStats(TQueryReplayEvents::TCheckQueryPlanStatus status, const TString& message) {
        TString failReason;
        switch (status) {
            case TQueryReplayEvents::CompileError:
                failReason = "compile_error";
                break;
            case TQueryReplayEvents::CompileTimeout:
                failReason = "compile_timeout";
                break;
            case TQueryReplayEvents::TableMissing:
                failReason = "table_missing";
                break;
            case TQueryReplayEvents::ExtraReadingOldEngine:
                failReason = "extra_reading_old_engine";
                break;
            case TQueryReplayEvents::ExtraReadingNewEngine:
                failReason = "extra_reading_new_engine";
                break;
            case TQueryReplayEvents::ReadTypesMismatch:
                failReason = "read_types_mismatch";
                break;
            case TQueryReplayEvents::ReadLimitsMismatch:
                failReason = "read_limits_mismatch";
                break;
            case TQueryReplayEvents::ReadColumnsMismatch:
                failReason = "read_columns_mismatch";
                break;
            case TQueryReplayEvents::ExtraWriting:
                failReason = "extra_writing";
                break;
            case TQueryReplayEvents::WriteColumnsMismatch:
                failReason = "write_columns_mismatch";
                break;
            case TQueryReplayEvents::UncategorizedPlanMismatch:
                failReason = "uncategorized_plan_mismatch";
                break;
            default:
                failReason = "unspecified";
        }

        NYdb::TParams params = Client->GetParamsBuilder()
            .AddParam("$run_id").Utf8(RunId).Build()
            .AddParam("$query_id").Utf8(QueryId).Build()
            .AddParam("$fail_reason").Utf8(failReason).Build()
            .AddParam("$extra_message").Utf8(message).Build()
        .Build();

        Client->RetryOperation([query = WriteStatsQuery, params](NTable::TSession session) {
            auto txControl = NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx();
            auto queryResult = session.ExecuteDataQuery(query, txControl, std::move(params));

            return queryResult.Apply([](const NTable::TAsyncDataQueryResult& asyncResult) {
                NTable::TDataQueryResult result = asyncResult.GetValue();
                if (!result.IsSuccess()) {
                    Cerr << "Failed to write stats: " << result.GetIssues().ToString() << Endl;
                }
                return NThreading::MakeFuture<TStatus>(result);
            });
        });
    }

    void Handle(TQueryReplayEvents::TEvCompileResponse::TPtr& ev) {
        if (ev->Get()->Status != TQueryReplayEvents::Success) {
            WriteQueryStats(ev->Get()->Status, ev->Get()->Message);
        }

        Send(OwnerId, new TQueryReplayEvents::TEvQueryProcessorResult(ev->Get()->Success));
    }

public:
    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TQueryReplayEvents::TEvQueryResolveResult, Handle);
        }
    }

    STATEFN(StateCompiling) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TQueryReplayEvents::TEvCompileResponse, Handle);
        }
    }
};

NActors::IActor* CreateQueryProcessorActor(std::shared_ptr<NYdb::NTable::TTableClient> client, const TString& runId, const TString& queryId, const TString& tablePath,
                                           const TString& statsTablePath, const TActorId& ownerId, TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> moduleResolverState,
                                           const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry) {
    return new TQueryProcessorActor(client, runId, queryId, tablePath, statsTablePath, ownerId, moduleResolverState, functionRegistry);
}
