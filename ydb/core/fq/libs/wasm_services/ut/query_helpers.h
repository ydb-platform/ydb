#pragma once
#include "helpers.h"

#include <ydb/core/fq/libs/common/external_service.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/fq/libs/wasm_services/query/module_source.h>
#include <ydb/core/fq/libs/wasm_services/query/query.h>

#include <ydb/library/protobuf_printer/security_printer.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/function/proto/dq_function.pb.h>
#include <ydb/services/udf_store/wasm/single_module_loader.h>

#include <yql/essentials/minikql/mkql_node_serialization.h>

namespace NFq::NWasmServices::NTestHelpers {
struct TQueryOutput {
    std::mutex Mutex;
    TVector<TProfile> Rows;
    TVector<TString> Events;
    std::atomic<bool> Blocked = false;
    std::atomic<bool> Finished = false;
    std::atomic<ui32> BlockAfterRows = 0;
};

class TProfileConsumer final: public NYql::NDq::IDqOutputConsumer {
public:
    explicit TProfileConsumer(std::shared_ptr<TQueryOutput> output)
        : Output(std::move(output))
    {
    }

    NYql::NDq::EDqFillLevel GetFillLevel() const override {
        return Output->Blocked ? NYql::NDq::HardLimit : NYql::NDq::NoLimit;
    }

    void Consume(NYql::NUdf::TUnboxedValue&& row) override {
        TProfile profile;
        profile.Id = row.GetElement(0).Get<ui64>();
        const auto nameValue = row.GetElement(1);
        const auto name = nameValue.AsStringRef();
        profile.NameBytes = name.Size();
        std::memcpy(profile.Name, name.Data(), name.Size());
        profile.Score = row.GetElement(2).Get<ui32>();
        std::lock_guard lock(Output->Mutex);
        Output->Rows.push_back(profile);
        Output->Events.push_back(TStringBuilder() << "row:" << profile.Id);
        if (Output->BlockAfterRows && Output->Rows.size() >= Output->BlockAfterRows) {
            Output->Blocked = true;
        }
    }

    void WideConsume(NYql::NUdf::TUnboxedValue[], ui32) override {
        UNIT_FAIL("Unexpected wide Profile output");
    }

    void Consume(NYql::NDqProto::TCheckpoint&& checkpoint) override {
        std::lock_guard lock(Output->Mutex);
        Output->Events.push_back(TStringBuilder() << "checkpoint:" << checkpoint.GetId());
    }

    void Consume(NYql::NDqProto::TWatermark&& watermark) override {
        std::lock_guard lock(Output->Mutex);
        Output->Events.push_back(TStringBuilder() << "watermark:" << watermark.GetTimestampUs());
    }

    void Finish() override {
        std::lock_guard lock(Output->Mutex);
        Output->Events.push_back("finish");
        Output->Finished = true;
    }

    void Flush() override {
    }

    bool IsFinished() const override {
        return Output->Finished;
    }

    bool IsEarlyFinished() const override {
        return false;
    }

private:
    std::shared_ptr<TQueryOutput> Output;
};

inline FederatedQuery::Connection TestServiceConnection(const TString& name = "service") {
    FederatedQuery::Connection connection;
    connection.mutable_meta()->set_id(name);
    connection.mutable_content()->set_name(name);
    auto* service = connection.mutable_content()->mutable_setting()->mutable_external_service();
    service->set_protocol(FederatedQuery::ExternalService::HTTP);
    service->set_endpoint("http://unused");
    service->set_insecure(true);
    service->mutable_auth()->mutable_none();
    return connection;
}

inline THashMap<TString, FederatedQuery::Connection> TestServiceConnections(const TString& name = "service") {
    return { { name, TestServiceConnection(name) } };
}

struct TQueryTransformEnv {
    TTempFile Module;
    TTempFile Manifest;
    bool UsesStore = false;
    NYql::NDq::TDqAsyncIoFactory Factory;
    std::shared_ptr<TQueryOutput> Output = std::make_shared<TQueryOutput>();
    NYql::NDq::TFakeCASetup Setup;
    NYql::NDq::IDqOutputChannel::TPtr RealChannel;

    TQueryTransformEnv(const TBinding& binding, ui32 timeoutMs = 5000, ui32 batchRows = 1, ui32 batchBytes = 0, ui32 maxBufferedRows = 65536,
        ui64 maxBufferedBytes = 0, bool grpcInsecure = true, bool useStore = false, bool realChannel = false,
        std::function<void(NFq::NConfig::TWasmServicesConfig&)> configure = {}, std::optional<ui32> batchSize = {})
        : Module(MakeTempName())
        , Manifest(MakeTempName())
    {
        TFileOutput(Module.Name()).Write(NResource::Find("/fq_transport_coroutine.wasm"));
        TFileOutput(Manifest.Name()).Write(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
        NFq::NConfig::TWasmServicesConfig config;
        config.SetEnabled(true);
        config.SetModuleSource(NFq::NConfig::TWasmServicesConfig::LOCAL_FILES);
        auto* module = config.AddModules();
        module->SetModulePath(Module.Name());
        module->SetManifestPath(Manifest.Name());
        config.SetCallTimeoutMs(timeoutMs);
        config.SetMaxBatchRows(batchRows);
        config.SetMaxBatchBytes(batchBytes);
        config.SetMaxBufferedRows(maxBufferedRows);
        config.SetMaxBufferedBytes(maxBufferedBytes);
        if (configure) {
            configure(config);
        }
        auto connection = TestServiceConnection("profiles");
        auto* entry = connection.mutable_content()->mutable_setting()->mutable_external_service();
        entry->set_endpoint(binding.Endpoint);
        entry->set_ca_certificate(binding.CaCertificate);
        if (!binding.CaFile.empty()) {
            entry->set_ca_certificate(TFileInput(binding.CaFile).ReadAll());
        }
        entry->set_insecure(binding.Protocol == EProtocol::Grpc ? grpcInsecure : binding.Endpoint.StartsWith("http://"));
        for (const auto& [key, value] : binding.Headers) {
            if (key == "Authorization" || key == "authorization") {
                entry->mutable_auth()->mutable_token()->set_token(value.substr(7));
            } else {
                (*entry->mutable_headers())[key] = value;
            }
        }
        if (binding.Protocol == EProtocol::Grpc) {
            entry->set_protocol(FederatedQuery::ExternalService::GRPC);
        }
        UsesStore = useStore;
        if (useStore) {
            config.SetModuleSource(NFq::NConfig::TWasmServicesConfig::UDF_STORE);
            config.ClearModules();
            // Initialize the executor factory before the module exists: UDF Store
            // loads modules asynchronously after ydbd/FQ startup.
            RegisterServiceTransforms(Factory, config, binding.TvmProvider, 1);
            const auto bytes = NResource::Find("/fq_transport_coroutine.wasm");
            NKikimr::NUdfStore::NWasm::TWasmLoadParams params;
            params.Manifest = NKikimr::NUdfStore::NWasm::ParseManifest(NResource::Find("/ydb/udfs/wasm/profile/manifest.json"));
            params.ModuleWasmData = bytes;
            params.ModuleObjectCode = NKikimr::NUdfStore::NWasm::CompileModuleObjectCode(bytes, NYdb::NWasm::EBytecodeFormat::Binary);
            NKikimr::NUdfStore::NWasm::LoadWasmFromManifest(params);
        }
        auto gateway = CreateServiceGatewayFactory(config, { { "profiles", connection } });
        auto description =
            gateway->CreateDqFunctionGateway(TString(ProfileTransformType), {}, "profiles")->ResolveFunction({}, "Profile").GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(description.InvokeUrl, "profiles");
        UNIT_ASSERT_EXCEPTION(gateway->CreateDqFunctionGateway(TString(ProfileTransformType), {}, "missing"), yexception);
        if (binding.TvmProvider) {
            entry->clear_auth();
            auto* tvm = entry->mutable_tvm_auth();
            tvm->set_source_id(binding.TvmCredentials.SourceId);
            tvm->set_destination_id(binding.TvmCredentials.DestinationId);
            tvm->set_secret(binding.TvmCredentials.Secret);
        }
        if (!useStore) {
            RegisterServiceTransforms(Factory, config, binding.TvmProvider, 1);
        }
        Setup.Execute([&](NYql::NDq::TFakeActor& actor) {
            using namespace NKikimr::NMiniKQL;
            auto* input = TStructTypeBuilder(actor.TypeEnv).Add("id", TDataType::Create(NYql::NUdf::TDataType<ui64>::Id, actor.TypeEnv)).Build();
            auto* output = TStructTypeBuilder(actor.TypeEnv)
                               .Add("id", TDataType::Create(NYql::NUdf::TDataType<ui64>::Id, actor.TypeEnv))
                               .Add("name", TDataType::Create(NYql::NUdf::TDataType<NYql::NUdf::TUtf8>::Id, actor.TypeEnv))
                               .Add("score", TDataType::Create(NYql::NUdf::TDataType<ui32>::Id, actor.TypeEnv))
                               .Build();
            NYql::NDqProto::TTaskOutput desc;
            desc.MutableTransform()->SetType(TString(ServiceTransformType));
            desc.MutableTransform()->SetInputType(SerializeNode(input, actor.TypeEnv));
            desc.MutableTransform()->SetOutputType(SerializeNode(output, actor.TypeEnv));
            NYql::NProto::TFunctionTransform settings;
            settings.SetInvokeUrl(description.InvokeUrl);
            if (batchSize) {
                settings.SetBatchSize(*batchSize);
            }
            desc.MutableTransform()->MutableSettings()->PackFrom(settings);
            THashMap<TString, TString> params;
            params[ServiceConnectionKey("profiles")] = PrepareServiceConnection(connection, {});
            NYql::NDq::IDqOutputConsumer::TPtr consumer = new TProfileConsumer(Output);
            if (realChannel) {
                RealChannel = NYql::NDq::CreateDqOutputChannel({ .RowType = output, .HolderFactory = &actor.HolderFactory, .ChannelId = 1 });
                consumer = NYql::NDq::CreateOutputMapConsumer(RealChannel);
            }
            auto [sink, sinkActor] = Factory.CreateDqOutputTransform(
                { .OutputDesc = desc, .OutputIndex = 0, .StatsLevel = NYql::NDq::None, .TxId = {}, .TaskId = 1, .TransformOutput = consumer,
                    .Callback = &actor.GetAsyncOutputCallbacks(),
                    .SecureParams = params,
                    .TaskParams = params,
                    .TypeEnv = actor.TypeEnv,
                    .HolderFactory = actor.HolderFactory,
                    .Alloc = std::shared_ptr<TScopedAlloc>(&actor.Alloc, [](auto*) {
                    }),
                    .TraceId = {} });
            actor.InitAsyncOutput(sink, sinkActor);
        });
    }

    ~TQueryTransformEnv() {
        // The channel owns values allocated by the fake compute actor.
        if (RealChannel) {
            Setup.Execute([&](auto&) {
                RealChannel.Reset();
            });
        }
        if (UsesStore) {
            NKikimr::NUdfStore::NWasm::GetWasmModuleCatalog().Unregister(TString(ProfileTransformType));
        }
    }

    void Start(TVector<ui64> ids = { 42 }, bool finished = true) {
        Setup.AsyncOutputWrite([&](NKikimr::NMiniKQL::THolderFactory& holders) {
            NKikimr::NMiniKQL::TUnboxedValueBatch batch;
            for (const auto id : ids) {
                NYql::NUdf::TUnboxedValue* members;
                auto row = holders.CreateDirectArrayHolder(1, members);
                members[0] = NYql::NUdf::TUnboxedValuePod(id);
                batch.emplace_back(std::move(row));
            }
            return batch;
        }, Nothing(), finished);
    }
};

}   // namespace NFq::NWasmServices::NTestHelpers
