#include "query_replay.h"

#include "metadata.h"
#include "plan_check.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <util/string/escape.h>
#include <util/string/strip.h>

#include <ydb/core/client/scheme_cache_lib/yql_db_scheme_resolver.h>

#include <memory>

using namespace NKikimrConfig;
using namespace NThreading;
using namespace NYql;
using namespace NYql::NDq;
using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NKikimr::NQueryReplay;


class TStaticTableMetadataLoader: public NYql::IKikimrGateway::IKqpTableMetadataLoader, public NYql::IDbSchemeResolver {
    TActorSystem* ActorSystem;
    std::shared_ptr<TMetadataInfoHolder> TableMetadata;
    bool IsMissingTableMetadata = false;

public:
    TStaticTableMetadataLoader(TActorSystem* actorSystem, std::shared_ptr<TMetadataInfoHolder>& tableMetadata)
        : ActorSystem(actorSystem)
        , TableMetadata(tableMetadata)
    {}

    NThreading::TFuture<NYql::IKikimrGateway::TTableMetadataResult> LoadTableMetadata(
        const TString& cluster, const TString& table, const NYql::IKikimrGateway::TLoadTableMetadataSettings& settings, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken) override {
        Y_UNUSED(cluster);
        Y_UNUSED(settings);
        Y_UNUSED(database);
        Y_UNUSED(userToken);
        auto ptr = TableMetadata->find(table);
        if (ptr == TableMetadata->end()) {
            IsMissingTableMetadata = true;
        }

        Y_ENSURE(ptr != TableMetadata->end());

        NYql::IKikimrGateway::TTableMetadataResult result;
        result.SetSuccess();
        result.Metadata = ptr->second;
        return MakeFuture<NYql::IKikimrGateway::TTableMetadataResult>(result);
    }

    TTableResult MakeTable(const TTable& table, const NYql::TKikimrTableMetadataPtr meta) const {
        TTableResult reply{TTableResult::Ok};
        reply.Status = TTableResult::Ok;
        reply.Table.TableName = table.TableName;
        reply.CacheGeneration = meta->SchemaVersion;
        reply.TableId = new TTableId(meta->PathId.OwnerId(), meta->PathId.TableId(), meta->SchemaVersion);
        reply.KeyColumnCount = meta->KeyColumnNames.size();
        TVector<TString> ColumnNames;
        ColumnNames.reserve(meta->Columns.size());
        THashMap<TString, i32> KeyPosition;
        i32 idx = 0;
        for (auto& key : meta->KeyColumnNames) {
            KeyPosition[key] = idx;
            ++idx;
        }

        for (auto& [name, column] : meta->Columns) {
            ColumnNames.push_back(name);
            reply.Columns[name] = TTableResult::TColumn{column.Id, KeyPosition[name], column.TypeInfo, 0, EColumnTypeConstraint::Nullable};
        }

        return reply;
    }

    TTableResults Resolve(const TVector<TTable>& tables) const {
        TTableResults results;
        for (auto& table : tables) {
            const NYql::TKikimrTableMetadataPtr* metaIt = TableMetadata->FindPtr(table.TableName);
            if (metaIt != nullptr) {
                results.push_back(MakeTable(table, *metaIt));
                continue;
            }

            results.push_back({TTableResult::LookupError, TStringBuilder() << "Unknow table " << table.TableName});
        }

        return results;
    }

    bool HasMissingTableMetadata() const {
        return IsMissingTableMetadata;
    }

    virtual NThreading::TFuture<TTableResults> ResolveTables(const TVector<TTable>& tables) override {
        return NThreading::MakeFuture(Resolve(tables));
    }

    virtual void ResolveTables(const TVector<TTable>& tables, NActors::TActorId responseTo) override {
        auto results = Resolve(tables);
        ActorSystem->Send(responseTo, new NYql::IDbSchemeResolver::TEvents::TEvResolveTablesResult(std::move(results)));
    }

    TVector<NKikimrKqp::TKqpTableMetadataProto> GetCollectedSchemeData() override {
        return {};
    }
};

class TReplayCompileActor: public TActorBootstrapped<TReplayCompileActor> {
public:
    TReplayCompileActor(TIntrusivePtr<TModuleResolverState> moduleResolverState, const NMiniKQL::IFunctionRegistry* functionRegistry,
        NYql::IHTTPGateway::TPtr httpGateway, bool antlr4ParserIsAmbiguityError)
        : ModuleResolverState(moduleResolverState)
        , KqpSettings()
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , FunctionRegistry(functionRegistry)
        , HttpGateway(std::move(httpGateway))
    {
        Config->SetAntlr4ParserIsAmbiguityError(antlr4ParserIsAmbiguityError);
        Config->SetEnableBatchUpdates(true);
    }

    void Bootstrap() {
        Become(&TThis::StateInit);
    }

    void PassAway() override {
        TActor::PassAway();
    }

private:
    STATEFN(StateInit) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TQueryReplayEvents::TEvCompileRequest, Handle);
                default:
                    Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected event in StateInit");
            }
        } catch (const yexception& e) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, e.what());
        }
    }
    STATEFN(StateCompile) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvContinueProcess, Handle);
                cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
                default:
                    Reply(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected event in CompileState");
            }
        } catch (const yexception& e) {
            Reply(Ydb::StatusIds::INTERNAL_ERROR, e.what());
        }
    }

private:

    TKqpQueryRef MakeQueryRef() {
        return TKqpQueryRef(Query->Text, Query->QueryParameterTypes);
    }

    void StartCompilation() {
        IKqpHost::TPrepareSettings prepareSettings;
        prepareSettings.DocumentApiRestricted = false;

        switch (Query->Settings.QueryType) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
                AsyncCompileResult = KqpHost->PrepareDataQuery(MakeQueryRef(), prepareSettings);
                break;

            case NKikimrKqp::QUERY_TYPE_AST_DML:
                AsyncCompileResult = KqpHost->PrepareDataQueryAst(MakeQueryRef(), prepareSettings);
                break;

            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN:
                AsyncCompileResult = KqpHost->PrepareScanQuery(MakeQueryRef(), Query->IsSql(), prepareSettings);
                break;
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
                AsyncCompileResult = KqpHost->PrepareGenericScript(MakeQueryRef(), prepareSettings);
                break;
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY: {
                prepareSettings.ConcurrentResults = false;
                AsyncCompileResult = KqpHost->PrepareGenericQuery(MakeQueryRef(), prepareSettings, nullptr);
                break;
            }
            case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY: {
                AsyncCompileResult = KqpHost->PrepareGenericQuery(MakeQueryRef(), prepareSettings, nullptr);
                break;
            }

            default:
                YQL_ENSURE(false, "Unexpected query type: " << Query->Settings.QueryType);
        }
    }
    void Continue() {
        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        TActorId selfId = SelfId();
        auto callback = [actorSystem, selfId](const TFuture<bool>& future) {
            bool finished = future.GetValue();
            auto processEv = MakeHolder<TEvKqp::TEvContinueProcess>(0, finished);
            actorSystem->Send(selfId, processEv.Release());
        };

        AsyncCompileResult->Continue().Apply(callback);
    }

    TTableMetadataLookup MetadataLookup() const {
        return [this](const TString& name) {
            return TableMetadata->FindPtr(name);
        };
    }

    void WriteJsonData(const TString& fname, const NJson::TJsonValue& data) {
        TFileOutput out(ReplayDetails["query_id"].GetStringSafe() + fname);
        NJson::WriteJson(&out, &data, true);
    }

    void WriteQueryMismatchInfo(const NJson::TJsonValue& lhs, const NJson::TJsonValue& rhs) {
        Cerr << "Found plan mismatch, query " << ReplayDetails["query_id"].GetStringSafe() << Endl;
        WriteJsonData("-repro.txt", ReplayDetails);
        WriteJsonData("-plan-lhs.txt", lhs);
        WriteJsonData("-plan-rhs.txt", rhs);
    }

    void Reply(const Ydb::StatusIds::StatusCode& status, const TString& message) {
        NYql::TIssue issue(NYql::TPosition(), message);
        Reply(status, {issue});
    }

    void Reply(const Ydb::StatusIds::StatusCode& status, const TIssues& issues, const std::optional<TString>& queryPlan = std::nullopt) {
        std::unique_ptr<TQueryReplayEvents::TEvCompileResponse> ev = std::make_unique<TQueryReplayEvents::TEvCompileResponse>(true);
        Y_UNUSED(queryPlan);
        if (status != Ydb::StatusIds::SUCCESS) {
            ev->Success = false;
            if (!MetadataLoader) {
                ev->Status = TQueryReplayEvents::UncategorizedFailure;
            } else if (MetadataLoader->HasMissingTableMetadata()) {
                ev->Status = TQueryReplayEvents::MissingTableMetadata;
            } else if (status == Ydb::StatusIds::TIMEOUT) {
                ev->Status = TQueryReplayEvents::CompileTimeout;
            } else {
                ev->Status = TQueryReplayEvents::CompileError;
            }
            ev->Message = issues.ToString();
            Cerr << "Failed to compile query: " << ev->Message << Endl;
            WriteJsonData("-repro.txt", ReplayDetails);
        } else {
            Y_ENSURE(queryPlan);
            ev->Plan = *queryPlan;
            const NJson::TJsonValue newEnginePlan = ParseQueryPlan(*queryPlan);
            const NJson::TJsonValue oldEnginePlan = ParseQueryPlan(ReplayDetails["query_plan"].GetStringSafe());
            std::tie(ev->Status, ev->Message) = CheckQueryPlans(
                ReplayDetails["query_plan"].GetStringSafe(),
                newEnginePlan,
                MetadataLookup(),
                &ev->EngineTableStats);
            if (ev->Status != TQueryReplayEvents::Success) {
                WriteQueryMismatchInfo(oldEnginePlan, newEnginePlan);
            }
        }

        Send(Owner, ev.release());
        PassAway();
    }

    void Handle(TQueryReplayEvents::TEvCompileRequest::TPtr& ev) {
        Owner = ev->Sender;

        ReplayDetails = std::move(ev->Get()->ReplayDetails);

        TableMetadata = std::make_shared<TMetadataInfoHolder>(std::move(ExtractStaticMetadata(ReplayDetails)));
        TString queryText = UnescapeC(ReplayDetails["query_text"].GetStringSafe());

        std::map<TString, Ydb::Type> queryParameterTypes;
        if (ReplayDetails.Has("query_parameter_types")) {
            NJson::TJsonValue qpt;
            Y_ENSURE(ReplayDetails["query_parameter_types"].IsString());
            static NJson::TJsonReaderConfig readConfig;
            TStringInput in(ReplayDetails["query_parameter_types"].GetStringSafe());
            NJson::ReadJsonTree(&in, &readConfig, &qpt, false);

            Y_ENSURE(qpt.IsMap());
            for (const auto& [paramName, paramType] : qpt.GetMapSafe()) {
                if (!queryParameterTypes[paramName].ParseFromString(Base64Decode(paramType.GetStringSafe()))) {
                    queryParameterTypes.erase(paramName);
                }
            }
        }

        const google::protobuf::EnumDescriptor *descriptor = NKikimrKqp::EQueryType_descriptor();
        auto queryType = NKikimrKqp::QUERY_TYPE_SQL_DML;
        if (ReplayDetails.Has("query_type")) {
            auto res = descriptor->FindValueByName(ReplayDetails["query_type"].GetStringSafe());
            if (res) {
                queryType = static_cast<NKikimrKqp::EQueryType>(res->number());
            }
        }

        QueryId = ReplayDetails["query_id"].GetStringSafe();

        TKqpQuerySettings settings(queryType);
        const auto& database = ReplayDetails["query_database"].GetStringSafe();
        Query = std::make_unique<NKikimr::NKqp::TKqpQueryId>(
            ReplayDetails["query_cluster"].GetStringSafe(),
            database,
            database,
            queryText,
            settings,
            !queryParameterTypes.empty()
                ? std::make_shared<std::map<TString, Ydb::Type>>(std::move(queryParameterTypes))
                : nullptr,
            GUCSettings ? *GUCSettings : TGUCSettings());

        GUCSettings->ImportFromJson(ReplayDetails);

        Config->Init(KqpSettings.DefaultSettings.GetDefaultSettings(), ReplayDetails["query_cluster"].GetStringSafe(), KqpSettings.Settings, false);
        if (!Query->Database.empty()) {
            Config->_KqpTablePathPrefix = ReplayDetails["query_database"].GetStringSafe();
        }

        ui32 syntax = (ReplayDetails["query_syntax"].GetStringSafe() == "1") ? 1 : 0;
        if (queryType == NKikimrKqp::QUERY_TYPE_SQL_SCAN) {
            syntax = 1;
        }
	    Config->SetSqlVersion(syntax);
        Config->FreezeDefaults();

        MetadataLoader = make_shared<TStaticTableMetadataLoader>(TlsActivationContext->ActorSystem(), TableMetadata);
        std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader> loader = MetadataLoader;

        auto c = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto counters = MakeIntrusive<TKqpRequestCounters>();
        counters->Counters = new TKqpCounters(c);
        counters->TxProxyMon = new NTxProxy::TTxProxyMon(c);

        Gateway = CreateKikimrIcGateway(Query->Cluster, queryType, Query->Database, Query->DatabaseId, std::move(loader),
            TActivationContext::ActorSystem(), SelfId().NodeId(), counters);
        auto federatedQuerySetup = std::make_optional<TKqpFederatedQuerySetup>({nullptr, HttpGateway, nullptr, nullptr, nullptr, {}, {}, {}, nullptr, {}, nullptr, {}, nullptr, {}, nullptr, nullptr});
        KqpHost = CreateKqpHost(Gateway, Query->Cluster, Query->Database, Config, ModuleResolverState->ModuleResolver,
            federatedQuerySetup, nullptr, GUCSettings, NKikimrConfig::TQueryServiceConfig(), Nothing(), FunctionRegistry, false);

        StartCompilation();
        Continue();

        Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
        Become(&TThis::StateCompile);
    }

    void Handle(TEvKqp::TEvContinueProcess::TPtr& ev) {
        Y_ENSURE(!ev->Get()->QueryId);

        if (!ev->Get()->Finished) {
            Continue();
            return;
        }

        auto kqpResult = AsyncCompileResult->GetResult();
        auto status = GetYdbStatus(kqpResult);

        std::optional<TString> queryPlan;

        if (status == Ydb::StatusIds::SUCCESS) {
            queryPlan = std::move(kqpResult.QueryPlan);
        }

        Reply(status, kqpResult.Issues(), queryPlan);
    }

    void HandleTimeout() {
        return Reply(Ydb::StatusIds::TIMEOUT, "Query compilation timed out.");
    }

private:
    TActorId Owner;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    TString QueryId;
    std::unique_ptr<TKqpQueryId> Query;
    TGUCSettings::TPtr GUCSettings = std::make_shared<TGUCSettings>();
    TKqpSettings KqpSettings;
    TKikimrConfiguration::TPtr Config;
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<IKqpHost> KqpHost;
    TIntrusivePtr<IKqpHost::IAsyncQueryResult> AsyncCompileResult;
    const NMiniKQL::IFunctionRegistry* FunctionRegistry;
    std::shared_ptr<TMetadataInfoHolder> TableMetadata;
    TActorId MiniKQLCompileService;
    NJson::TJsonValue ReplayDetails;
    std::shared_ptr<TStaticTableMetadataLoader> MetadataLoader;
    NYql::IHTTPGateway::TPtr HttpGateway;
};

IActor* CreateQueryCompiler(TIntrusivePtr<TModuleResolverState> moduleResolverState,
    const NMiniKQL::IFunctionRegistry* functionRegistry, NYql::IHTTPGateway::TPtr httpGateway, bool antlr4ParserIsAmbiguityError)
{
    return new TReplayCompileActor(moduleResolverState, functionRegistry, httpGateway, antlr4ParserIsAmbiguityError);
}
