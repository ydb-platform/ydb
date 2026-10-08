#include "query.h"

#include <ydb/core/fq/libs/wasm_services/profile.h>
#include <ydb/core/fq/libs/wasm_services/transport.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/providers/function/proto/dq_function.pb.h>
#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <util/generic/deque.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <cstring>

namespace NFq::NWasmServices {
namespace {

using namespace NYql::NDq;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdfStore::NWasm;
namespace NUdf = NYql::NUdf;

void ValidateConfig(const NConfig::TWasmServicesConfig& config) {
    Y_ENSURE(config.GetEnabled(), "WASM Profile adapter is disabled");
    Y_ENSURE(!config.GetModulePath().empty(), "WASM Profile module path is required");
    Y_ENSURE(config.BindingsSize(), "WASM Profile bindings are required");
    THashSet<TString> aliases;
    for (const auto& binding : config.GetBindings()) {
        Y_ENSURE(!binding.GetAlias().empty() && aliases.insert(binding.GetAlias()).second, "Invalid or duplicate WASM Profile alias");
        Y_ENSURE(!binding.GetEndpoint().empty(), "WASM Profile endpoint is required");
        Y_ENSURE(binding.GetProtocol() == NConfig::TWasmServicesConfig::TBinding::HTTP ||
                     binding.GetProtocol() == NConfig::TWasmServicesConfig::TBinding::GRPC, "Invalid WASM Profile protocol");
    }
}

class TProfileGateway final : public NYql::IDqFunctionGateway {
  public:
    explicit TProfileGateway(TString alias)
        : Alias(std::move(alias))
    {
    }

    NThreading::TFuture<NYql::NDqFunction::TDqFunctionDescription> ResolveFunction(const TString&, const TString& name) override {
        Y_ENSURE(name == "Profile", "Unknown WASM Profile function");
        return NThreading::MakeFuture(NYql::NDqFunction::TDqFunctionDescription{
            .Type = TString(ProfileTransformType), .FunctionName = name, .Connection = Alias, .InvokeUrl = Alias});
    }

  private:
    TString Alias;
};

struct TPreparedConfig {
    NYdb::NWasm::TModuleBytecode Module;
    TVector<TBinding> Bindings;
    THashMap<TString, ui32> Aliases;
    ui32 MaxRows = 65536;
    TDuration Timeout = TDuration::Seconds(30);
};

ui32 CheckMember(TStructType* type, TStringBuf name, ui32 scheme) {
    const auto index = type->FindMemberIndex(name);
    Y_ENSURE(index && type->GetMemberType(*index)->GetKind() == TType::EKind::Data &&
                 AS_TYPE(TDataType, type->GetMemberType(*index))->GetSchemeType() == scheme, "Invalid WASM Profile row type");
    return *index;
}

class TProfileTransform final : public NActors::TActorBootstrapped<TProfileTransform>, public IDqComputeActorAsyncOutput {
    struct TEvReady : NActors::TEventLocal<TEvReady, NActors::TEvents::ES_PRIVATE << 16> {};

  public:
    TProfileTransform(std::shared_ptr<const TPreparedConfig> config, ui32 binding, IDqAsyncIoFactory::TOutputTransformArguments&& args)
        : Config(std::move(config)), Binding(binding), Index(args.OutputIndex), Output(args.TransformOutput), Callback(args.Callback),
          Alloc(std::move(args.Alloc)), HolderFactory(args.HolderFactory)
    {
        Stats.Level = args.StatsLevel;
        const auto& desc = args.OutputDesc.GetTransform();
        auto* input = AS_TYPE(TStructType, static_cast<TType*>(DeserializeNode(desc.GetInputType(), args.TypeEnv)));
        auto* output = AS_TYPE(TStructType, static_cast<TType*>(DeserializeNode(desc.GetOutputType(), args.TypeEnv)));
        Y_ENSURE(input->GetMembersCount() == 1 && output->GetMembersCount() == 3, "Invalid WASM Profile row shape");
        InputId = CheckMember(input, "id", NUdf::TDataType<ui64>::Id);
        OutputId = CheckMember(output, "id", NUdf::TDataType<ui64>::Id);
        OutputName = CheckMember(output, "name", NUdf::TDataType<NUdf::TUtf8>::Id);
        OutputScore = CheckMember(output, "score", NUdf::TDataType<ui32>::Id);
    }

    ~TProfileTransform() override {
        Cleanup();
    }

    void Bootstrap() {
        Become(&TProfileTransform::StateWork);
        try {
            auto query = std::make_unique<TQueryCompartmentHandle>();
            query->Generation = 1;
            query->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(query->Generation);
            query->Compartment = CreateRegistryCompartment({});
            AddPrecompiledModule(query->Compartment.get(), Config->Module, "FqProfileAdapter");
            query->Resident = std::make_unique<TCompartmentResidentCache>(query->Compartment.get());
            Transport = std::make_shared<TTransport>(Config->Bindings);
            auto wasmAlloc = std::make_shared<TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), false);
            Runtime = std::make_unique<NAsync::TRuntime>(std::move(query), std::move(wasmAlloc), Transport, NAsync::TLimits{},
                                                         [system = NActors::TActivationContext::ActorSystem(), owner = SelfId()] {
                                                             system->Send(new NActors::IEventHandle(owner, owner, new TEvReady));
                                                         });
            Pump();
        } catch (...) {
            Fail("WASM Profile initialization failed");
        }
    }

    ui64 GetOutputIndex() const override {
        return Index;
    }
    i64 GetFreeSpace() const override {
        return Failed || Finished ? 0 : static_cast<i64>(Config->MaxRows - Pending.size() - bool(Call) - bool(Result)) * sizeof(ui64);
    }
    const TDqAsyncStats& GetEgressStats() const override {
        return Stats;
    }

    void SendData(TUnboxedValueBatch&& batch, i64, const TMaybe<NYql::NDqProto::TCheckpoint>& checkpoint, bool finished) override {
        Y_ENSURE(!checkpoint, "WASM Profile prototype does not support checkpoints");
        Y_ENSURE(!Finished && !Failed && !batch.IsWide(), "Invalid WASM Profile input batch");
        Y_ENSURE(batch.RowCount() <= Config->MaxRows - Pending.size() - bool(Call) - bool(Result), "WASM Profile row quota exceeded");
        batch.ForEachRow([&](const NUdf::TUnboxedValue& row) {
            const auto id = row.GetElement(InputId).Get<ui64>();
            Y_ENSURE(id, "WASM Profile id must be nonzero");
            Pending.push_back(id);
        });
        Finished = finished;
        Send(SelfId(), new TEvReady);
    }

    void CommitState(const NYql::NDqProto::TCheckpoint&) override {
        Y_ENSURE(false, "WASM Profile checkpoints are unsupported");
    }
    void LoadState(const TSinkState&, const NYql::NDqProto::TCheckpoint&) override {
        Y_ENSURE(false, "WASM Profile checkpoints are unsupported");
    }
    void OnOutputConsumerReady() override {
        Send(SelfId(), new TEvReady);
    }
    void PassAway() override {
        Cleanup();
        TActorBootstrapped::PassAway();
    }

  private:
    STRICT_STFUNC(StateWork, hFunc(TEvReady, Handle); hFunc(NActors::TEvents::TEvWakeup, Handle);
                  cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);)

    void Handle(TEvReady::TPtr&) {
        Pump();
    }
    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        if (Runtime && Call && ev->Get()->Tag == Call) {
            Runtime->Expire(TInstant::Now());
            Pump();
        }
    }

    void Pump() {
        if (Failed || !Runtime || !Output)
            return;
        try {
            // All WASM entries happen on this mailbox. Native I/O only sends TEvReady.
            if (Call) {
                Runtime->TakeReady();
                const auto reply = Runtime->Poll(Call);
                if (reply.Status == NAsync::ECallStatus::Waiting || reply.Status == NAsync::ECallStatus::Runnable)
                    return;
                Runtime->Drop(std::exchange(Call, 0));
                if (reply.Status != NAsync::ECallStatus::Completed || reply.Data.size() != sizeof(TProfileResults)) {
                    Fail("WASM Profile execution failed or deadline expired");
                    return;
                }
                TProfileResults decoded;
                std::memcpy(&decoded, reply.Data.data(), sizeof(decoded));
                if (decoded.Version != ProfileVersion || decoded.Count != 1 || decoded.Items[0].Error != EServiceError::None ||
                    !decoded.Items[0].Profile.Id || !decoded.Items[0].Profile.NameBytes ||
                    decoded.Items[0].Profile.NameBytes > MaxProfileNameBytes || decoded.Items[0].Profile.Score > 100) {
                    Fail(TStringBuilder() << "WASM Profile service failed: error=" << static_cast<ui32>(decoded.Items[0].Error)
                                          << ", client_error=" << static_cast<ui32>(decoded.Items[0].ClientError));
                    return;
                }
                Result = decoded.Items[0].Profile;
            }
            if (Result) {
                auto guard = Guard(*Alloc);
                if (Output->GetFillLevel() == HardLimit)
                    return;
                NUdf::TUnboxedValue* members;
                auto row = HolderFactory.CreateDirectArrayHolder(3, members);
                members[OutputId] = NUdf::TUnboxedValuePod(Result->Id);
                members[OutputName] = MakeString(TStringBuf(Result->Name, Result->NameBytes));
                members[OutputScore] = NUdf::TUnboxedValuePod(Result->Score);
                Output->Consume(std::move(row));
                Output->Flush();
                Stats.Bytes += sizeof(ui64) + sizeof(ui32) + Result->NameBytes;
                ++Stats.Rows;
                Result.Clear();
                Callback->ResumeExecution();
            }
            if (!Pending.empty()) {
                const auto id = Pending.front();
                Pending.pop_front();
                const auto mode = Config->Bindings[Binding].Protocol == EProtocol::Http ? EProfileMode::Http : EProfileMode::Grpc;
                const auto bytes = Encode(TArgumentsHeader{static_cast<ui64>(mode), Binding, Binding, sizeof(id)},
                                          std::string_view(reinterpret_cast<const char*>(&id), sizeof(id)));
                Call = Runtime->Start(TStringBuf(bytes.data(), bytes.size()), TInstant::Now() + Config->Timeout);
                Schedule(Config->Timeout, new NActors::TEvents::TEvWakeup(Call));
                Send(SelfId(), new TEvReady);
            } else if (Finished && !Acknowledged) {
                auto guard = Guard(*Alloc);
                Output->Finish();
                Acknowledged = true;
                Callback->OnAsyncOutputFinished(Index);
            }
        } catch (...) {
            Fail("WASM Profile execution failed");
        }
    }

    void Fail(const TString& message) {
        if (std::exchange(Failed, true))
            return;
        Runtime.reset();
        Pending.clear();
        Result.Clear();
        Call = 0;
        auto guard = Guard(*Alloc);
        Callback->OnAsyncOutputError(Index, NYql::TIssues{NYql::TIssue(message)}, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
    }

    void Cleanup() {
        Runtime.reset();
        Transport.reset();
        auto guard = Guard(*Alloc);
        Output.Reset();
    }

    const std::shared_ptr<const TPreparedConfig> Config;
    const ui32 Binding;
    const ui64 Index;
    IDqOutputConsumer::TPtr Output;
    ICallbacks* const Callback;
    const std::shared_ptr<TScopedAlloc> Alloc;
    const THolderFactory& HolderFactory;
    ui32 InputId, OutputId, OutputName, OutputScore;
    TDqAsyncStats Stats;
    TDeque<ui64> Pending;
    TMaybe<TProfile> Result;
    NAsync::THandle Call = 0;
    bool Finished = false, Acknowledged = false, Failed = false;
    std::shared_ptr<TTransport> Transport;
    std::unique_ptr<NAsync::TRuntime> Runtime;
};

} // namespace

NYql::TDqFunctionGatewayFactory::TPtr CreateProfileGatewayFactory(const NConfig::TWasmServicesConfig& config) {
    ValidateConfig(config);
    THashSet<TString> aliases;
    for (const auto& binding : config.GetBindings())
        aliases.insert(binding.GetAlias());
    auto factory = MakeIntrusive<NYql::TDqFunctionGatewayFactory>();
    factory->Register(TString(ProfileTransformType), [aliases = std::move(aliases)](const auto&, const TString& connection) {
        Y_ENSURE(aliases.contains(connection), "Unknown WASM Profile connection alias");
        return std::make_shared<TProfileGateway>(connection);
    });
    return factory;
}

void RegisterProfileTransform(TDqAsyncIoFactory& factory, const NConfig::TWasmServicesConfig& config) {
    ValidateConfig(config);
    EnsureUdfHostIntrinsicsRegistered();
    NAsync::KeepAsyncHostIntrinsicsLinked();
    auto prepared = std::make_shared<TPreparedConfig>();
    const auto bytes = TFileInput(config.GetModulePath()).ReadAll();
    const auto object = CompileModuleObjectCode(bytes, NYdb::NWasm::EBytecodeFormat::Binary);
    prepared->Module = MakeModuleBytecode(bytes, object, NYdb::NWasm::EBytecodeFormat::Binary);
    if (config.GetMaxBufferedRows())
        prepared->MaxRows = config.GetMaxBufferedRows();
    if (config.GetCallTimeoutMs())
        prepared->Timeout = TDuration::MilliSeconds(config.GetCallTimeoutMs());
    for (const auto& entry : config.GetBindings()) {
        TBinding binding;
        binding.Protocol = entry.GetProtocol() == NConfig::TWasmServicesConfig::TBinding::HTTP ? EProtocol::Http : EProtocol::Grpc;
        binding.Endpoint = entry.GetEndpoint();
        binding.Method = entry.GetMethod().empty() ? "POST" : entry.GetMethod();
        binding.CaFile = entry.GetCaFile();
        for (const auto& [key, value] : entry.GetHeaders())
            binding.Headers.emplace_back(key, value);
        if (binding.Protocol == EProtocol::Grpc) {
            Y_ENSURE(!entry.GetMethod().empty(), "WASM Profile gRPC method is required");
            if (entry.GetGrpcInsecure()) {
                binding.GrpcCredentials = grpc::InsecureChannelCredentials();
            } else {
                grpc::SslCredentialsOptions options;
                if (!entry.GetCaFile().empty())
                    options.pem_root_certs = TFileInput(entry.GetCaFile()).ReadAll();
                binding.GrpcCredentials = grpc::SslCredentials(options);
            }
        }
        prepared->Aliases.emplace(entry.GetAlias(), prepared->Bindings.size());
        prepared->Bindings.push_back(std::move(binding));
    }
    factory.RegisterOutputTransform<NYql::NProto::TFunctionTransform>(
        TString(ProfileTransformType),
        [prepared](NYql::NProto::TFunctionTransform&& settings, IDqAsyncIoFactory::TOutputTransformArguments&& args) {
            const auto binding = prepared->Aliases.find(settings.GetInvokeUrl());
            Y_ENSURE(binding != prepared->Aliases.end(), "Unknown WASM Profile connection alias");
            auto* actor = new TProfileTransform(prepared, binding->second, std::move(args));
            return std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
        });
}

} // namespace NFq::NWasmServices
