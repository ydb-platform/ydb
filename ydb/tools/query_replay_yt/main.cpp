#include "query_replay.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/logger/backend.h>
#include <ydb/library/yql/utils/actor_log/log.h>

#include <util/string/split.h>
#include <util/system/fs.h>

using namespace NActors;

TVector<std::pair<TString, TString>> GetJobFiles(TVector<TString> udfs) {
    TVector<std::pair<TString, TString>> result;

    for(const TString& udf: udfs) {
        TVector<TString> splitResult;
        Split(udf.data(), "/", splitResult);
        while(!splitResult.empty() && splitResult.back().empty()) {
            splitResult.pop_back();
        }

        Y_ENSURE(!splitResult.empty());

        result.push_back(std::make_pair(udf, splitResult.back()));
    }

    return result;
}

class TQueryReplayMapper
    : public NYT::IMapper<NYT::TTableReader<NYT::TNode>, NYT::TTableWriter<NYT::TNode>>
{

    THolder<NActors::TActorSystem> ActorSystem;
    TAutoPtr<TLogBackend> LogBackend;
    std::unique_ptr<NYql::NLog::YqlLoggerScope> YqlLogger;
    TIntrusivePtr<NActors::NLog::TSettings> LogSettings;
    THolder<NKikimr::TAppData> AppData;
    TIntrusivePtr<NKikimr::NScheme::TKikimrTypeRegistry> TypeRegistry;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> ModuleResolverState;

    NYql::IHTTPGateway::TPtr HttpGateway;
    TVector<TString> UdfFiles;
    ui32 ActorSystemThreadsCount = 5;
    NActors::NLog::EPriority YqlLogPriority = NActors::NLog::EPriority::PRI_ERROR;

public:
    static TString GetFailReason(const TQueryReplayEvents::TCheckQueryPlanStatus& status) {
        switch (status) {
            case TQueryReplayEvents::CompileError:
                return "compile_error";
            case TQueryReplayEvents::CompileTimeout:
                return "compile_timeout";
            case TQueryReplayEvents::TableMissing:
                return "table_missing";
            case TQueryReplayEvents::ExtraReadingOldEngine:
                return "extra_reading_old_engine";
            case TQueryReplayEvents::ExtraReadingNewEngine:
                return "extra_reading_new_engine";
            case TQueryReplayEvents::ReadTypesMismatch:
                return "read_types_mismatch";
            case TQueryReplayEvents::ReadLimitsMismatch:
                return "read_limits_mismatch";
            case TQueryReplayEvents::ReadColumnsMismatch:
                return "read_columns_mismatch";
            case TQueryReplayEvents::ExtraWriting:
                return "extra_writing";
            case TQueryReplayEvents::WriteColumnsMismatch:
                return "write_columns_mismatch";
            case TQueryReplayEvents::UncategorizedPlanMismatch:
                return "uncategorized_plan_mismatch";
            case TQueryReplayEvents::MissingTableMetadata:
                return "missing_table_metadata";
            default:
                return "unspecified";
        }
    }

public:
    TQueryReplayMapper() = default;

    Y_SAVELOAD_JOB(UdfFiles, ActorSystemThreadsCount, YqlLogPriority);

    TQueryReplayMapper(TVector<TString> udfFiles, ui32 actorSystemThreadsCount,
        NActors::NLog::EPriority yqlLogPriority = NActors::NLog::EPriority::PRI_ERROR)
        : UdfFiles(udfFiles)
        , ActorSystemThreadsCount(actorSystemThreadsCount)
        , YqlLogPriority(yqlLogPriority)
    {}

    void Start(NYT::TTableWriter<NYT::TNode>*) override {
        YqlLogger = std::make_unique<NYql::NLog::YqlLoggerScope>(&Cerr);
        NYql::NDq::SetYqlLogLevels(YqlLogPriority);

        TypeRegistry.Reset(new NKikimr::NScheme::TKikimrTypeRegistry());
        FunctionRegistry.Reset(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone());
        NKikimr::NMiniKQL::FillStaticModules(*FunctionRegistry);
        NKikimr::NMiniKQL::TUdfModuleRemappings remappings;
        THashSet<TString> usedUdfPaths;

        for(const auto& [realPath, udfPath]: GetJobFiles(UdfFiles)) {
            if (usedUdfPaths.insert(udfPath).second) {
                if (NFs::Exists(udfPath)) {
                    FunctionRegistry->LoadUdfs(udfPath, remappings, 0);
                    continue;
                }

                if (NFs::Exists(realPath)) {
                    FunctionRegistry->LoadUdfs(realPath, remappings, 0);
                    continue;
                }
            }
        }

        AppData.Reset(new NKikimr::TAppData(0, 0, 0, 0, {}, TypeRegistry.Get(), FunctionRegistry.Get(), nullptr, nullptr));
        AppData->Counters = MakeIntrusive<NMonitoring::TDynamicCounters>(new NMonitoring::TDynamicCounters());
        auto setup = BuildActorSystemSetup(ActorSystemThreadsCount);
        ActorSystem.Reset(new TActorSystem(setup, AppData.Get()));
        ActorSystem->Start();
        ActorSystem->Register(NKikimr::NKqp::CreateKqpResourceManagerActor({}, nullptr));
        ModuleResolverState = MakeIntrusive<NKikimr::NKqp::TModuleResolverState>();
        HttpGateway = NYql::IHTTPGateway::Make();
        Y_ABORT_UNLESS(GetYqlDefaultModuleResolver(ModuleResolverState->ExprCtx, ModuleResolverState->ModuleResolver));
    }

    THolder<TQueryReplayEvents::TEvCompileResponse> RunReplay(NJson::TJsonValue&& json) {
        TString queryType = json["query_type"].GetStringSafe();
        if (queryType == "QUERY_TYPE_AST_SCAN") {
            return nullptr;
        }

        if (queryType == "QUERY_TYPE_SQL_GENERIC_SCRIPT") {
            return nullptr;
        }

        auto compileActorId = ActorSystem->Register(CreateQueryCompiler(ModuleResolverState, FunctionRegistry.Get(), HttpGateway));

        auto future = ActorSystem->Ask<TQueryReplayEvents::TEvCompileResponse>(
            compileActorId,
            THolder(new TQueryReplayEvents::TEvCompileRequest(std::move(json))),
            TDuration::Seconds(100));

        return future.ExtractValueSync();
    }

    void Do(NYT::TTableReader<NYT::TNode>* in, NYT::TTableWriter<NYT::TNode>* out) override {
        for (; in->IsValid(); in->Next()) {
            const auto& row = in->GetRow();
            NJson::TJsonValue json(NJson::JSON_MAP);
            for (const auto& [key, child]: row.AsMap()) {
                if (key == "_logfeller_timestamp")
                    continue;
                json.InsertValue(key, NJson::TJsonValue(child.AsString()));
            }

            auto response = RunReplay(std::move(json));
            if (response == nullptr)
                continue;

            auto status = response.Get()->Status;

            TString failReason = GetFailReason(status);

            if (failReason == "unspecified" || status == TQueryReplayEvents::MissingTableMetadata) {
                continue;
            }

            NYT::TNode result;
            result = result("query_id", row["query_id"].AsString());

            result = result("created_at", row["created_at"].AsString());
            result = result("query_cluster", row["query_cluster"].AsString());
            result = result("query_database", row["query_database"].AsString());

            result = result("query_plan", row["query_plan"].AsString());
            result = result("query_syntax", row["query_syntax"].AsString());
            result = result("query_text", row["query_text"].AsString());

            result = result("query_type", row["query_type"].AsString());
            result = result("table_metadata", row["table_metadata"].AsString());
            result = result("version", row["version"].AsString());

            result = result("fail_reason", failReason);
            result = result("extra_message", response.Get()->Message);
            result = result("new_query_plan", response.Get()->Plan);

            out->AddRow(result);
        }
    }

    void Finish(NYT::TTableWriter<NYT::TNode>*) override {
        ActorSystem->Stop();
    }
};

static NYT::TTableSchema OutputSchema() {
    NYT::TTableSchema schema;
    schema.AddColumn(NYT::TColumnSchema().Name("fail_reason").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("extra_message").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("new_query_plan").Type(NYT::VT_STRING));

    schema.AddColumn(NYT::TColumnSchema().Name("query_id").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("created_at").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("query_cluster").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("query_database").Type(NYT::VT_STRING));

    schema.AddColumn(NYT::TColumnSchema().Name("query_plan").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("query_syntax").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("query_text").Type(NYT::VT_STRING));

    schema.AddColumn(NYT::TColumnSchema().Name("query_type").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("table_metadata").Type(NYT::VT_STRING));
    schema.AddColumn(NYT::TColumnSchema().Name("version").Type(NYT::VT_STRING));
    return schema;
}

REGISTER_NAMED_MAPPER("Query replay mapper", TQueryReplayMapper);

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    TQueryReplayConfig config;
    config.ParseConfig(argc, argv);
    if (config.QueryFile) {
        auto fakeMapper = TQueryReplayMapper(config.UdfFiles, config.ActorSystemThreadsCount, config.YqlLogLevel);
        fakeMapper.Start(nullptr);
        Y_DEFER {
            fakeMapper.Finish(nullptr);
        };

        NJson::TJsonValue queryJson;
        {
            static NJson::TJsonReaderConfig readConfig;
            TFileInput in(config.QueryFile);
            NJson::ReadJsonTree(&in, &readConfig, &queryJson, false);
        }

        auto result = fakeMapper.RunReplay(std::move(queryJson));

        auto status = result.Get()->Status;
        TString failReason = TQueryReplayMapper::GetFailReason(status);
        Cerr << failReason << Endl;
        return 0;
    }

    if (config.Cluster.empty() || config.SrcPath.empty() || config.DstPath.empty()) {
        Cerr << "Non local execution requires Cluster and SrcPath and DstPath options to be specified.";
        return EXIT_FAILURE;
    }

    auto client = NYT::CreateClient(config.Cluster);
    NYT::SetLogger(NYT::CreateStdErrLogger(NYT::ILogger::ELevel::INFO));

    NYT::TMapOperationSpec spec;
    spec.AddInput<NYT::TNode>(config.SrcPath);
    spec.AddOutput<NYT::TNode>(NYT::TRichYPath(config.DstPath).Schema(OutputSchema()));

    auto userJobSpec = NYT::TUserJobSpec();
    userJobSpec.MemoryLimit(5_GB);

    for(const auto& [udf, udfInJob]: GetJobFiles(config.UdfFiles)) {
        userJobSpec.AddLocalFile(udf, NYT::TAddLocalFileOptions().PathInJob(udfInJob));
    }

    spec.MapperSpec(userJobSpec);
    if (!config.CoreTablePath.empty()) {
        spec.CoreTablePath(config.CoreTablePath);
    }

    client->Map(spec, new TQueryReplayMapper(config.UdfFiles, config.ActorSystemThreadsCount, config.YqlLogLevel));

    return EXIT_SUCCESS;
}
