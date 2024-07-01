#include "query_replay.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

using namespace NActors;

class TQueryReplayMapper
    : public NYT::IMapper<NYT::TTableReader<NYT::TNode>, NYT::TTableWriter<NYT::TNode>>
{

    THolder<NActors::TActorSystem> ActorSystem;
    THolder<NKikimr::TAppData> AppData;
    TIntrusivePtr<NKikimr::NScheme::TKikimrTypeRegistry> TypeRegistry;
    TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<NKikimr::NKqp::TModuleResolverState> ModuleResolverState;

    TQueryReplayConfig Config;

    TString GetFailReason(const TQueryReplayEvents::TCheckQueryPlanStatus& status) {
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
            default:
                return "unspecified";
        }
    }

public:
    TQueryReplayMapper() = default;
    TQueryReplayMapper(const TQueryReplayConfig& config) : Config(config) {
    }

    void Start(NYT::TTableWriter<NYT::TNode>*) override {
        TypeRegistry.Reset(new NKikimr::NScheme::TKikimrTypeRegistry());
        FunctionRegistry.Reset(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone());
        NKikimr::NMiniKQL::FillStaticModules(*FunctionRegistry);
        AppData.Reset(new NKikimr::TAppData(0, 0, 0, 0, {}, TypeRegistry.Get(), FunctionRegistry.Get(), nullptr, nullptr));
        AppData->Counters = MakeIntrusive<NMonitoring::TDynamicCounters>(new NMonitoring::TDynamicCounters());
        auto setup = BuildActorSystemSetup(Config.ActorSystemThreadsCount);
        ActorSystem.Reset(new TActorSystem(setup, AppData.Get()));
        ActorSystem->Start();
        ActorSystem->Register(NKikimr::NKqp::CreateKqpResourceManagerActor({}, nullptr));
        ModuleResolverState = MakeIntrusive<NKikimr::NKqp::TModuleResolverState>();
        Y_ABORT_UNLESS(GetYqlDefaultModuleResolver(ModuleResolverState->ExprCtx, ModuleResolverState->ModuleResolver));
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

            auto compileActorId = ActorSystem->Register(CreateQueryCompiler(ModuleResolverState, FunctionRegistry.Get()));

            auto future = ActorSystem->Ask<TQueryReplayEvents::TEvCompileResponse>(
                compileActorId,
                THolder(new TQueryReplayEvents::TEvCompileRequest(std::move(json))),
                TDuration::Seconds(100));

            auto response = future.ExtractValueSync();
            auto status = response.Get()->Status;

            TString failReason = GetFailReason(status);

            if (failReason == "unspecified") {
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

    auto client = NYT::CreateClient(config.Cluster);
    NYT::SetLogger(NYT::CreateStdErrLogger(NYT::ILogger::ELevel::INFO));

    NYT::TMapOperationSpec spec;
    spec.AddInput<NYT::TNode>(config.SrcPath);
    spec.AddOutput<NYT::TNode>(NYT::TRichYPath(config.DstPath).Schema(OutputSchema()));
    spec.MapperSpec(NYT::TUserJobSpec().MemoryLimit(5_GB));

    client->Map(spec, new TQueryReplayMapper(config));

    return EXIT_SUCCESS;
}
