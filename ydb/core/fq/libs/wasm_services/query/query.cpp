#include "query.h"
#include "manifest.h"

#include <ydb/core/fq/libs/wasm_services/transport.h>
#include <ydb/core/fq/libs/common/external_service.h>
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
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <google/protobuf/util/json_util.h>
#include <util/charset/utf8.h>
#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NFq::NWasmServices {
namespace {

using namespace NYql::NDq;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm::NServices;
namespace NUdf = NYql::NUdf;

struct TModuleDescription {
    TString Path;
    TServiceManifest Manifest;
};

TVector<TModuleDescription> DescribeModules(const NConfig::TWasmServicesConfig& config) {
    Y_ENSURE(config.GetEnabled(), "WASM services are disabled");
    Y_ENSURE(config.GetMaxBatchRows() <= MaxServiceBatchRows, "WASM service batch row limit exceeds 64");
    const auto bytes = config.GetMaxBatchBytes();
    Y_ENSURE(!bytes || (bytes >= sizeof(TServiceRequest) && bytes <= MaxServiceBatchBytes), "Invalid WASM service batch byte limit");
    Y_ENSURE(config.GetModulePath().empty() || !config.ModulesSize(), "Use either ModulePath or Modules for WASM services");
    TVector<TModuleDescription> modules;
    THashSet<TString> names;
    auto add = [&](const TString& path, const TString& manifestPath) {
        Y_ENSURE(!path.empty() && !manifestPath.empty(), "WASM service module and manifest paths are required");
        const TFile manifestFile(manifestPath, RdOnly);
        Y_ENSURE(manifestFile.GetLength() <= 65536, "WASM service manifest exceeds byte limit");
        auto manifest = ParseServiceManifest(TFileInput(manifestFile).ReadAll());
        Y_ENSURE(names.insert(manifest.Name).second, "Duplicate WASM service module name");
        modules.push_back({path, std::move(manifest)});
    };
    if (!config.GetModulePath().empty())
        add(config.GetModulePath(), config.GetModulePath() + ".manifest.json");
    for (const auto& module : config.GetModules())
        add(module.GetModulePath(), module.GetManifestPath());
    Y_ENSURE(!modules.empty(), "WASM service modules are required");
    return modules;
}

TString Invocation(const TString& module, const TString& method, const TString& connection) {
    NJson::TJsonValue value(NJson::JSON_MAP);
    value["module"] = module;
    value["method"] = method;
    value["connection_id"] = connection;
    return NJson::WriteJson(value, false);
}

void ValidateMethod(const TServiceMethod& method, ui32 bytes, ui32 rows, ui64 bufferedBytes) {
    Y_ENSURE(sizeof(TServiceRequest) + method.MaxInputRowBytes <= bytes && sizeof(TServiceResult) + method.MaxOutputRowBytes <= bytes,
             "WASM service row cannot fit byte limit");
    Y_ENSURE(rows <= 1 || method.Batch, "WASM service method does not support batching");
    Y_ENSURE(bufferedBytes >= std::max(method.MaxInputRowBytes, method.MaxOutputRowBytes),
             "WASM service row cannot fit buffered byte limit");
}

class TServiceGateway final : public NYql::IDqFunctionGateway {
  public:
    TServiceGateway(std::shared_ptr<const TServiceManifest> manifest, TString alias, TString id, ui32 bytes, ui32 rows, ui64 bufferedBytes)
        : Manifest(std::move(manifest)), Alias(std::move(alias)), Id(std::move(id)), Bytes(bytes), Rows(rows), BufferedBytes(bufferedBytes)
    {
    }

    NThreading::TFuture<NYql::NDqFunction::TDqFunctionDescription> ResolveFunction(const TString&, const TString& name) override {
        Y_ENSURE(Manifest->Methods.contains(name), "Unknown WASM service method");
        ValidateMethod(Manifest->Methods.at(name), Bytes, Rows, BufferedBytes);
        return NThreading::MakeFuture(NYql::NDqFunction::TDqFunctionDescription{
            .Type = Manifest->Name, .FunctionName = name, .Connection = Alias, .InvokeUrl = Invocation(Manifest->Name, name, Id)});
    }

  private:
    const std::shared_ptr<const TServiceManifest> Manifest;
    const TString Alias;
    const TString Id;
    const ui32 Bytes, Rows;
    const ui64 BufferedBytes;
};

struct TPreparedConfig {
    NYdb::NWasm::TModuleBytecode Module;
    TServiceManifest Manifest;
    ui32 MaxRows = 65536;
    ui64 MaxBufferedBytes = 64 << 20;
    ui32 BatchRows = 1;
    ui32 BatchBytes = MaxServiceBatchBytes;
    TDuration Timeout = TDuration::Seconds(30);
};

ui32 Scheme(EValueType type) {
    switch (type) {
    case EValueType::Uint64:
        return NUdf::TDataType<ui64>::Id;
    case EValueType::Uint32:
        return NUdf::TDataType<ui32>::Id;
    case EValueType::Int64:
        return NUdf::TDataType<i64>::Id;
    case EValueType::Bool:
        return NUdf::TDataType<bool>::Id;
    case EValueType::String:
        return NUdf::TDataType<char*>::Id;
    case EValueType::Utf8:
        return NUdf::TDataType<NUdf::TUtf8>::Id;
    }
    Y_ENSURE(false, "Unsupported WASM service field type");
    return 0;
}

TVector<ui32> BindFields(TType* type, const TVector<TServiceField>& fields) {
    Y_ENSURE(type->GetKind() == TType::EKind::Struct, "WASM service row must be a struct");
    const auto* row = AS_TYPE(TStructType, type);
    Y_ENSURE(row->GetMembersCount() == fields.size(), "Invalid WASM service row shape");
    TVector<ui32> positions;
    for (const auto& field : fields) {
        const auto index = row->FindMemberIndex(field.Name);
        Y_ENSURE(index && row->GetMemberType(*index)->GetKind() == TType::EKind::Data &&
                     AS_TYPE(TDataType, row->GetMemberType(*index))->GetSchemeType() == Scheme(field.Type),
                 "Invalid WASM service row type");
        positions.push_back(*index);
    }
    return positions;
}

bool ReadRow(TRowReader& reader, const TVector<TServiceField>& fields, NUdf::TUnboxedValue* values = nullptr,
             const TVector<ui32>* positions = nullptr) {
    for (size_t i = 0; i < fields.size(); ++i) {
        NUdf::TUnboxedValuePod value;
        switch (fields[i].Type) {
        case EValueType::Uint64: {
            ui64 v;
            if (!reader.Get(v))
                return false;
            value = NUdf::TUnboxedValuePod(v);
            break;
        }
        case EValueType::Uint32: {
            ui32 v;
            if (!reader.Get(v))
                return false;
            value = NUdf::TUnboxedValuePod(v);
            break;
        }
        case EValueType::Int64: {
            i64 v;
            if (!reader.Get(v))
                return false;
            value = NUdf::TUnboxedValuePod(v);
            break;
        }
        case EValueType::Bool: {
            ui8 v;
            if (!reader.Get(v) || v > 1)
                return false;
            value = NUdf::TUnboxedValuePod(bool(v));
            break;
        }
        case EValueType::String:
        case EValueType::Utf8: {
            std::string_view v;
            if (!reader.String(v, fields[i].MaxBytes) || (fields[i].Type == EValueType::Utf8 && !IsUtf(v.data(), v.size())))
                return false;
            if (values)
                values[(*positions)[i]] = MakeString(TStringBuf(v.data(), v.size()));
            continue;
        }
        }
        if (values)
            values[(*positions)[i]] = value;
    }
    return true;
}

class TServiceTransform final : public NActors::TActorBootstrapped<TServiceTransform>, public IDqComputeActorAsyncOutput {
    struct TEvReady : NActors::TEventLocal<TEvReady, NActors::TEvents::ES_PRIVATE << 16> {};

  public:
    TServiceTransform(std::shared_ptr<const TPreparedConfig> config, const TServiceMethod& method, TBinding binding,
                      IDqAsyncIoFactory::TOutputTransformArguments&& args)
        : Config(std::move(config)), Method(method), Binding(std::move(binding)), Index(args.OutputIndex), Output(args.TransformOutput),
          Callback(args.Callback), Alloc(std::move(args.Alloc)), HolderFactory(args.HolderFactory)
    {
        Stats.Level = args.StatsLevel;
        ValidateMethod(Method, Config->BatchBytes, Config->BatchRows, Config->MaxBufferedBytes);
        const auto& desc = args.OutputDesc.GetTransform();
        InputPositions = BindFields(static_cast<TType*>(DeserializeNode(desc.GetInputType(), args.TypeEnv)), Method.Input);
        OutputPositions = BindFields(static_cast<TType*>(DeserializeNode(desc.GetOutputType(), args.TypeEnv)), Method.Output);
        const auto reservation = std::max(Method.MaxInputRowBytes, Method.MaxOutputRowBytes);
        MaxRows = std::min<ui64>(Config->MaxRows, Config->MaxBufferedBytes / reservation);
        DispatchRows = std::min({Config->BatchRows, Method.MaxBatchRows,
                                 (Config->BatchBytes - ui32(sizeof(TServiceRequest))) / Method.MaxInputRowBytes,
                                 (Config->BatchBytes - ui32(sizeof(TServiceResult))) / Method.MaxOutputRowBytes});
    }

    ~TServiceTransform() override {
        Cleanup();
    }

    void Bootstrap() {
        Become(&TServiceTransform::StateWork);
        try {
            auto query = std::make_unique<TQueryCompartmentHandle>();
            query->Generation = 1;
            query->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(query->Generation);
            query->Compartment = CreateRegistryCompartment({});
            AddPrecompiledModule(query->Compartment.get(), Config->Module, Config->Manifest.Name);
            query->Resident = std::make_unique<TCompartmentResidentCache>(query->Compartment.get());
            TTransportLimits limits;
            limits.MaxPayloadBytes = Config->BatchBytes;
            Transport = std::make_shared<TTransport>(TVector<TBinding>{Binding}, limits);
            auto wasmAlloc = std::make_shared<TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), false);
            Runtime = std::make_unique<NAsync::TRuntime>(std::move(query), std::move(wasmAlloc), Transport, NAsync::TLimits{},
                                                         [system = NActors::TActivationContext::ActorSystem(), owner = SelfId()] {
                                                             system->Send(new NActors::IEventHandle(owner, owner, new TEvReady));
                                                         });
            Pump();
        } catch (...) {
            Fail("WASM service initialization failed");
        }
    }

    ui64 GetOutputIndex() const override {
        return Index;
    }
    i64 GetFreeSpace() const override {
        return Failed || Finished ? 0 : static_cast<i64>(MaxRows - BufferedRows()) * Method.MinInputRowBytes;
    }
    const TDqAsyncStats& GetEgressStats() const override {
        return Stats;
    }

    void SendData(TUnboxedValueBatch&& batch, i64, const TMaybe<NYql::NDqProto::TCheckpoint>& checkpoint, bool finished) override {
        try {
            Y_ENSURE(!checkpoint, "WASM services do not support checkpoints");
            Y_ENSURE(!Finished && !Failed && !batch.IsWide(), "Invalid WASM service input batch");
            Y_ENSURE(batch.RowCount() <= MaxRows - BufferedRows(), "WASM service row/byte quota exceeded");
            batch.ForEachRow([&](const NUdf::TUnboxedValue& row) {
                TString bytes(Method.MaxInputRowBytes, '\0');
                TRowWriter writer(bytes.Detach(), bytes.size());
                for (size_t i = 0; i < Method.Input.size(); ++i) {
                    const auto value = row.GetElement(InputPositions[i]);
                    bool valid = false;
                    switch (Method.Input[i].Type) {
                    case EValueType::Uint64:
                        valid = writer.Put(value.Get<ui64>());
                        break;
                    case EValueType::Uint32:
                        valid = writer.Put(value.Get<ui32>());
                        break;
                    case EValueType::Int64:
                        valid = writer.Put(value.Get<i64>());
                        break;
                    case EValueType::Bool:
                        valid = writer.Put(ui8(value.Get<bool>()));
                        break;
                    case EValueType::String:
                    case EValueType::Utf8: {
                        const auto string = value.AsStringRef();
                        valid = string.Size() <= Method.Input[i].MaxBytes &&
                                (Method.Input[i].Type != EValueType::Utf8 || IsUtf(string.Data(), string.Size())) &&
                                writer.String({string.Data(), string.Size()});
                        break;
                    }
                    }
                    Y_ENSURE(valid, "Invalid WASM service input field or byte limit");
                }
                bytes.resize(writer.Size());
                Pending.push_back(std::move(bytes));
            });
            Finished = finished;
            Send(SelfId(), new TEvReady);
        } catch (...) {
            Fail("Invalid WASM service input or row/byte quota exceeded");
        }
    }

    void CommitState(const NYql::NDqProto::TCheckpoint&) override {
        Y_ENSURE(false, "WASM service checkpoints are unsupported");
    }
    void LoadState(const TSinkState&, const NYql::NDqProto::TCheckpoint&) override {
        Y_ENSURE(false, "WASM service checkpoints are unsupported");
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
    size_t BufferedRows() const {
        return Pending.size() + ActiveCount + Results.size();
    }

    bool DecodeResults(TStringBuf bytes) {
        if (bytes.size() > Config->BatchBytes)
            return false;
        TRowReader reader({bytes.data(), bytes.size()});
        TServiceResult header;
        if (!reader.Get(header) || header.Version != ServiceVersion)
            return false;
        if (header.Error) {
            Fail(TStringBuilder() << "WASM service failed: error=" << header.Error << ", detail=" << header.Detail);
            return false;
        }
        if (header.Count != ActiveCount || header.Detail)
            return false;
        TDeque<TString> results;
        for (ui32 i = 0; i < header.Count; ++i) {
            const auto before = reader.Remaining();
            if (!ReadRow(reader, Method.Output))
                return false;
            const auto size = before.size() - reader.Remaining().size();
            if (size > Method.MaxOutputRowBytes)
                return false;
            results.emplace_back(before.data(), size);
        }
        if (!reader.Remaining().empty())
            return false;
        ActiveCount = 0;
        Results = std::move(results);
        return true;
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
                if (reply.Status != NAsync::ECallStatus::Completed) {
                    Fail("WASM service execution failed or deadline expired");
                    return;
                }
                if (!DecodeResults(TStringBuf(reply.Data.data(), reply.Data.size()))) {
                    if (!Failed)
                        Fail("Invalid WASM service result framing");
                    return;
                }
            }
            while (!Results.empty()) {
                auto guard = Guard(*Alloc);
                if (Output->GetFillLevel() == HardLimit)
                    return;
                NUdf::TUnboxedValue* members;
                auto row = HolderFactory.CreateDirectArrayHolder(Method.Output.size(), members);
                TRowReader reader({Results.front().data(), Results.front().size()});
                Y_ENSURE(ReadRow(reader, Method.Output, members, &OutputPositions), "Invalid WASM service output row");
                Output->Consume(std::move(row));
                Output->Flush();
                Stats.Bytes += Results.front().size();
                ++Stats.Rows;
                Results.pop_front();
                Callback->ResumeExecution();
            }
            if (!Pending.empty()) {
                {
                    auto guard = Guard(*Alloc);
                    if (Output->GetFillLevel() == HardLimit)
                        return;
                }
                ActiveCount = std::min<size_t>(DispatchRows, Pending.size());
                TString rows;
                for (ui32 i = 0; i < ActiveCount; ++i) {
                    rows += Pending.front();
                    Pending.pop_front();
                }
                const TServiceRequest header{ServiceMagic, ServiceVersion, Method.Id, 0,
                                             Binding.Protocol == EProtocol::Http ? 0u : 1u,
                                             ActiveCount,
                                             Config->BatchBytes,
                                             Config->BatchRows > 1 ? 1u : 0u};
                const auto bytes = Encode(header, {rows.data(), rows.size()});
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
            Fail("WASM service execution failed");
        }
    }

    void Fail(const TString& message) {
        if (std::exchange(Failed, true))
            return;
        Runtime.reset();
        Pending.clear();
        Results.clear();
        ActiveCount = 0;
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
    const TServiceMethod& Method;
    const TBinding Binding;
    const ui64 Index;
    IDqOutputConsumer::TPtr Output;
    ICallbacks* const Callback;
    const std::shared_ptr<TScopedAlloc> Alloc;
    const THolderFactory& HolderFactory;
    TVector<ui32> InputPositions, OutputPositions;
    ui32 MaxRows, DispatchRows, ActiveCount = 0;
    TDqAsyncStats Stats;
    TDeque<TString> Pending, Results;
    NAsync::THandle Call = 0;
    bool Finished = false, Acknowledged = false, Failed = false;
    std::shared_ptr<TTransport> Transport;
    std::unique_ptr<NAsync::TRuntime> Runtime;
};

} // namespace

TString ServiceConnectionKey(const TString& id) {
    return "fq.external_service:" + id;
}

TString ServiceInvocationConnection(const TString& invocation) {
    NJson::TJsonValue value;
    Y_ENSURE(NJson::ReadJsonTree(invocation, &value, true) && value.IsMap(), "Invalid WASM service invocation");
    const auto& id = value["connection_id"].GetStringSafe();
    Y_ENSURE(!id.empty(), "Missing WASM service connection id");
    return id;
}

TString PrepareServiceConnection(const FederatedQuery::Connection& connection, const TString& currentIamToken) {
    Y_ENSURE(connection.content().setting().has_external_service(), "Invalid WASM service connection type");
    auto service = connection.content().setting().external_service();
    Y_ENSURE(ValidateExternalService(service, false).Empty(), "Invalid external service connection");
    if (service.auth().has_current_iam()) {
        Y_ENSURE(!currentIamToken.empty(), "Current IAM token is unavailable for external service connection");
        service.mutable_auth()->mutable_token()->set_token(currentIamToken);
    }
    // SecureParams travels through protobuf string fields, so keep its payload UTF-8.
    TString payload;
    Y_ENSURE(google::protobuf::util::MessageToJsonString(service, &payload).ok(),
             "Cannot prepare external service connection");
    return payload;
}

NYql::TDqFunctionGatewayFactory::TPtr CreateServiceGatewayFactory(
    const NConfig::TWasmServicesConfig& config, const THashMap<TString, FederatedQuery::Connection>& connections) {
    auto modules = DescribeModules(config);
    THashMap<TString, TString> aliases;
    for (const auto& [id, connection] : connections) {
        if (connection.content().setting().has_external_service()) {
            Y_ENSURE(!id.empty() && aliases.emplace(connection.content().name(), id).second,
                     "Invalid or duplicate external service connection");
        }
    }
    auto factory = MakeIntrusive<NYql::TDqFunctionGatewayFactory>();
    const ui32 bytes = config.GetMaxBatchBytes() ? config.GetMaxBatchBytes() : MaxServiceBatchBytes;
    const ui32 rows = config.GetMaxBatchRows() ? config.GetMaxBatchRows() : 1;
    const ui64 bufferedBytes = config.GetMaxBufferedBytes() ? config.GetMaxBufferedBytes() : 64 << 20;
    for (auto& module : modules) {
        auto manifest = std::make_shared<TServiceManifest>(std::move(module.Manifest));
        factory->Register(manifest->Name, [manifest, aliases, bytes, rows, bufferedBytes](const auto&, const TString& connection) {
            const auto it = aliases.find(connection);
            Y_ENSURE(it != aliases.end(), "Unknown or inaccessible external service connection");
            return std::make_shared<TServiceGateway>(manifest, connection, it->second, bytes, rows, bufferedBytes);
        });
    }
    return factory;
}

void RegisterServiceTransforms(TDqAsyncIoFactory& factory, const NConfig::TWasmServicesConfig& config) {
    auto modules = DescribeModules(config);
    EnsureUdfHostIntrinsicsRegistered();
    NAsync::KeepAsyncHostIntrinsicsLinked();
    for (auto& module : modules) {
        auto prepared = std::make_shared<TPreparedConfig>();
        const auto bytes = TFileInput(module.Path).ReadAll();
        const auto object = CompileModuleObjectCode(bytes, NYdb::NWasm::EBytecodeFormat::Binary);
        prepared->Module = MakeModuleBytecode(bytes, object, NYdb::NWasm::EBytecodeFormat::Binary);
        prepared->Manifest = std::move(module.Manifest);
        if (config.GetMaxBufferedRows())
            prepared->MaxRows = config.GetMaxBufferedRows();
        if (config.GetMaxBufferedBytes())
            prepared->MaxBufferedBytes = config.GetMaxBufferedBytes();
        if (config.GetCallTimeoutMs())
            prepared->Timeout = TDuration::MilliSeconds(config.GetCallTimeoutMs());
        if (config.GetMaxBatchRows())
            prepared->BatchRows = config.GetMaxBatchRows();
        if (config.GetMaxBatchBytes())
            prepared->BatchBytes = config.GetMaxBatchBytes();
        factory.RegisterOutputTransform<NYql::NProto::TFunctionTransform>(
            prepared->Manifest.Name,
            [prepared](NYql::NProto::TFunctionTransform&& settings, IDqAsyncIoFactory::TOutputTransformArguments&& args) {
                NJson::TJsonValue invocation;
                Y_ENSURE(NJson::ReadJsonTree(settings.GetInvokeUrl(), &invocation, true) && invocation.IsMap(),
                         "Invalid WASM service invocation");
                Y_ENSURE(invocation["module"].GetStringSafe() == prepared->Manifest.Name, "Invalid WASM service module");
                const auto method = prepared->Manifest.Methods.find(invocation["method"].GetStringSafe());
                Y_ENSURE(method != prepared->Manifest.Methods.end(), "Unknown WASM service method");
                const auto entry = args.SecureParams.find(ServiceConnectionKey(ServiceInvocationConnection(settings.GetInvokeUrl())));
                Y_ENSURE(entry != args.SecureParams.end(), "External service connection is unavailable");
                FederatedQuery::ExternalService service;
                Y_ENSURE(google::protobuf::util::JsonStringToMessage(entry->second, &service).ok() && ValidateExternalService(service, true).Empty(),
                         "Invalid external service connection");
                TBinding binding;
                binding.Protocol = service.protocol() == FederatedQuery::ExternalService::HTTP ? EProtocol::Http : EProtocol::Grpc;
                binding.Endpoint = service.endpoint();
                binding.Method = service.method().empty() ? "POST" : service.method();
                binding.CaCertificate = service.ca_certificate();
                for (const auto& [key, value] : service.headers())
                    binding.Headers.emplace_back(key, value);
                if (service.auth().has_token())
                    binding.Headers.emplace_back(binding.Protocol == EProtocol::Http ? "Authorization" : "authorization",
                                                 "Bearer " + service.auth().token().token());
                if (binding.Protocol == EProtocol::Grpc) {
                    grpc::SslCredentialsOptions options;
                    options.pem_root_certs = service.ca_certificate();
                    binding.GrpcCredentials = service.insecure() ? grpc::InsecureChannelCredentials() : grpc::SslCredentials(options);
                }
                auto* actor = new TServiceTransform(prepared, method->second, std::move(binding), std::move(args));
                return std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
            });
    }
}

} // namespace NFq::NWasmServices
