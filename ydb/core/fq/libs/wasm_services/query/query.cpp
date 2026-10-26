#include "auth.h"
#include "execution.h"
#include "module_source.h"

#include <ydb/core/fq/libs/wasm_services/manifest/manifest.h>
#include <ydb/core/fq/libs/wasm_services/transport.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/providers/function/proto/dq_function.pb.h>
#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <util/charset/utf8.h>
#include <util/generic/deque.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <mutex>

namespace NFq::NWasmServices {
namespace {

using namespace NYql::NDq;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm::NServices;
namespace NUdf = NYql::NUdf;

struct TSharedTransport {
    TTransportExecutorLimits Limits;
    std::shared_ptr<TRequestMetrics> Metrics;
    ui32 UserPool = 0;
    std::mutex Mutex;
    std::shared_ptr<TTransportExecutor> Executor;

    std::shared_ptr<TTransportExecutor> Get() {
        std::lock_guard lock(Mutex);
        if (!Executor) {
            Executor = TTransportExecutor::Create(NActors::TActivationContext::ActorSystem(), UserPool, Limits, Metrics);
        }
        return Executor;
    }
};

TString Invocation(const TString& module, const TString& method, const TResolvedServiceConnection& connection) {
    NJson::TJsonValue value(NJson::JSON_MAP);
    value["module"] = module;
    value["method"] = method;
    value["connection_id"] = connection.Id;
    if (connection.Settings) {
        value["connection_settings"] = connection.Settings;
    }
    return NJson::WriteJson(value, false);
}

void ValidateMethod(const TServiceMethod& method, ui32 bytes, ui32 rows, ui64 bufferedBytes) {
    Y_ENSURE(rows >= 1 && rows <= MaxServiceBatchRows, "WASM service BATCH_SIZE must be between 1 and 64");
    Y_ENSURE(sizeof(TServiceRequest) + method.MaxInputRowBytes <= bytes && sizeof(TServiceResult) + method.MaxOutputRowBytes <= bytes,
        "WASM service row cannot fit byte limit");
    Y_ENSURE(rows <= 1 || method.Batch, "WASM service method does not support batching");
    Y_ENSURE(bufferedBytes >= std::max(method.MaxInputRowBytes, method.MaxOutputRowBytes), "WASM service row cannot fit buffered byte limit");
}

class TServiceGateway final: public NYql::IDqFunctionGateway {
public:
    TServiceGateway(std::shared_ptr<const TServiceManifest> manifest, TString alias, NThreading::TFuture<TResolvedServiceConnection> connection,
        ui32 bytes, ui64 bufferedBytes)
        : Manifest(std::move(manifest))
        , Alias(std::move(alias))
        , Connection(std::move(connection))
        , Bytes(bytes)
        , BufferedBytes(bufferedBytes)
    {
    }

    NThreading::TFuture<NYql::NDqFunction::TDqFunctionDescription> ResolveFunction(const TString&, const TString& name) override {
        Y_ENSURE(Manifest->Methods.contains(name), "Unknown WASM service method");
        // Batch size belongs to the SQL invocation, not to a cached connection
        // description. Validate batch support after applying that invocation's
        // settings against the manifest actually loaded by the executor.
        ValidateMethod(Manifest->Methods.at(name), Bytes, 1, BufferedBytes);
        return Connection.Apply([module = Manifest->Name, alias = Alias, name](const auto& future) {
            const auto& connection = future.GetValue();
            Y_ENSURE(connection.Id, "Missing WASM service connection id");
            return NYql::NDqFunction::TDqFunctionDescription{ .Type = module, .FunctionName = name, .Connection = alias,
                .InvokeUrl = Invocation(module, name, connection),
                .TransformType = TString(ServiceTransformType) };
        });
    }

private:
    const std::shared_ptr<const TServiceManifest> Manifest;
    const TString Alias;
    const NThreading::TFuture<TResolvedServiceConnection> Connection;
    const ui32 Bytes;
    const ui64 BufferedBytes;
};

struct TPreparedConfig {
    NYdb::NWasm::TModuleBytecode Module;
    TVector<TNamedModuleBytecode> Libraries;
    TServiceManifest Manifest;
    std::optional<TAuthModuleConfig> Auth;
    TString AuthAlias;
    ui32 MaxRows = 65536;
    ui64 MaxBufferedBytes = 64 << 20;
    ui32 MaxTraceSpans = 4096;
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
                     AS_TYPE(TDataType, row->GetMemberType(*index))->GetSchemeType() == Scheme(field.Type), "Invalid WASM service row type");
        positions.push_back(*index);
    }
    return positions;
}

bool ReadRow(
    TRowReader& reader, const TVector<TServiceField>& fields, NUdf::TUnboxedValue* values = nullptr, const TVector<ui32>* positions = nullptr) {
    for (size_t i = 0; i < fields.size(); ++i) {
        NUdf::TUnboxedValuePod value;
        switch (fields[i].Type) {
            case EValueType::Uint64: {
                ui64 v;
                if (!reader.Get(v)) {
                    return false;
                }
                value = NUdf::TUnboxedValuePod(v);
                break;
            }
            case EValueType::Uint32: {
                ui32 v;
                if (!reader.Get(v)) {
                    return false;
                }
                value = NUdf::TUnboxedValuePod(v);
                break;
            }
            case EValueType::Int64: {
                i64 v;
                if (!reader.Get(v)) {
                    return false;
                }
                value = NUdf::TUnboxedValuePod(v);
                break;
            }
            case EValueType::Bool: {
                ui8 v;
                if (!reader.Get(v) || v > 1) {
                    return false;
                }
                value = NUdf::TUnboxedValuePod(bool(v));
                break;
            }
            case EValueType::String:
            case EValueType::Utf8: {
                std::string_view v;
                if (!reader.String(v, fields[i].MaxBytes) || (fields[i].Type == EValueType::Utf8 && !IsUtf(v.data(), v.size()))) {
                    return false;
                }
                if (values) {
                    values[(*positions)[i]] = MakeString(TStringBuf(v.data(), v.size()));
                }
                continue;
            }
        }
        if (values) {
            values[(*positions)[i]] = value;
        }
    }
    return true;
}

class TServiceTransform final: public NActors::TActorBootstrapped<TServiceTransform>, public IDqComputeActorAsyncOutput {
    struct TEvReady: NActors::TEventLocal<TEvReady, NActors::TEvents::ES_PRIVATE << 16> {};

public:
    TServiceTransform(std::shared_ptr<const TPreparedConfig> config, const TServiceMethod& method, TBinding binding,
        std::shared_ptr<TSharedTransport> sharedTransport, IDqAsyncIoFactory::TOutputTransformArguments&& args)
        : Config(std::move(config))
        , Method(method)
        , Binding(std::move(binding))
        , SharedTransport(std::move(sharedTransport))
        , Index(args.OutputIndex)
        , Output(args.TransformOutput)
        , Callback(args.Callback)
        , Alloc(std::move(args.Alloc))
        , HolderFactory(args.HolderFactory)
    {
        TraceContext = TTraceContext::Create(std::move(args.TraceId), NActors::TActivationContext::ActorSystem(), Config->MaxTraceSpans);
        TransformTrace = TCallTrace::Start(TraceContext, "WASM transform");
        if (TransformTrace) {
            TransformTrace->Attribute("module", Config->Manifest.Name.substr(0, 128));
            TransformTrace->Attribute("service_method", Method.Name.substr(0, 128));
            TransformTrace->Attribute("service_method_id", i64(Method.Id));
            TransformTrace->Attribute("task_id", i64(args.TaskId));
            for (const char* key : { "fq.query_id", "fq.job_id", "fq.restart_count" }) {
                if (auto it = args.TaskParams.find(key); it != args.TaskParams.end()) {
                    TransformTrace->Attribute(key, it->second.substr(0, 128));
                }
            }
            TraceContext = TransformTrace->Context();
        }
        Stats.Level = args.StatsLevel;
        ValidateMethod(Method, Config->BatchBytes, Config->BatchRows, Config->MaxBufferedBytes);
        const auto& desc = args.OutputDesc.GetTransform();
        InputPositions = BindFields(static_cast<TType*>(DeserializeNode(desc.GetInputType(), args.TypeEnv)), Method.Input);
        OutputPositions = BindFields(static_cast<TType*>(DeserializeNode(desc.GetOutputType(), args.TypeEnv)), Method.Output);
        const auto reservation = std::max(Method.MaxInputRowBytes, Method.MaxOutputRowBytes);
        MaxRows = std::min<ui64>(Config->MaxRows, Config->MaxBufferedBytes / reservation);
        DispatchRows =
            std::min({ Config->BatchRows, Method.MaxBatchRows, (Config->BatchBytes - ui32(sizeof(TServiceRequest))) / Method.MaxInputRowBytes,
                (Config->BatchBytes - ui32(sizeof(TServiceResult))) / Method.MaxOutputRowBytes });
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
            query->Compartment = CreateRegistryCompartment(Config->Libraries);
            AddPrecompiledModule(query->Compartment.get(), Config->Module, Config->Manifest.Name);
            query->Resident = std::make_unique<TCompartmentResidentCache>(query->Compartment.get());
            TTransportLimits limits;
            limits.MaxPayloadBytes = Config->BatchBytes;
            if (Config->Auth) {
                Auth = std::make_shared<TAuthModule>(*query->Compartment, *Config->Auth, SharedTransport->Get(),
                    [system = NActors::TActivationContext::ActorSystem(), owner = SelfId()] {
                        system->Send(new NActors::IEventHandle(owner, owner, new TEvReady));
                    });
                Binding.AuthProvider = Auth;
                Binding.AuthHeader = Config->Auth->Module->Manifest.Auth->Header;
                Binding.AuthAlias = Config->AuthAlias;
                Binding.AuthConnection.reset();
            }
            Transport = std::make_shared<TTransport>(TVector<TBinding>{ Binding }, SharedTransport->Get(), limits);
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
        // DQ's estimated row size is always at least one byte, but can be
        // smaller than the wire framing (e.g. an empty String). Advertising one
        // byte per reserved row also bounds the final whole row returned by Pop.
        // Continue accepting control records after Finish, as DQ checkpoints
        // also traverse finished branches. No rows may follow Finish.
        return Failed || PendingCheckpoint || PendingWatermark ? 0 : static_cast<i64>(MaxRows - BufferedRows());
    }

    bool AllowFreeSpaceOvercommit() const override {
        return false;
    }

    const TDqAsyncStats& GetEgressStats() const override {
        return Stats;
    }

    void SendData(TUnboxedValueBatch&& batch, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>& checkpoint, bool finished) override {
        SendDataWithWatermark(std::move(batch), size, Nothing(), checkpoint, finished);
    }

    void SendDataWithWatermark(TUnboxedValueBatch&& batch, i64, const TMaybe<NYql::NDqProto::TWatermark>& watermark,
        const TMaybe<NYql::NDqProto::TCheckpoint>& checkpoint, bool finished) override {
        try {
            Y_ENSURE(!Failed && !batch.IsWide() && (!Finished || batch.empty()), "Invalid WASM service input batch");
            Y_ENSURE(!PendingCheckpoint && !PendingWatermark, "WASM service control record is still pending");
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
                                    writer.String({ string.Data(), string.Size() });
                            break;
                        }
                    }
                    Y_ENSURE(valid, "Invalid WASM service input field or byte limit");
                }
                bytes.resize(writer.Size());
                Pending.push_back(std::move(bytes));
            });
            PendingWatermark = watermark;
            PendingCheckpoint = checkpoint;
            Finished |= finished;
            Send(SelfId(), new TEvReady);
        } catch (...) {
            Fail("Invalid WASM service input or row/byte quota exceeded");
        }
    }

    // This is a stateless, barrier-forwarding transform, not a transactional
    // sink. DQ restores the input/program; uncheckpointed calls may repeat.
    // It has no separate state and must not call the sink state callbacks.
    void CommitState(const NYql::NDqProto::TCheckpoint&) override {
    }

    void LoadState(const TSinkState& state, const NYql::NDqProto::TCheckpoint&) override {
        Y_ENSURE(state.Data.Blob.empty(), "Unexpected WASM service checkpoint state");
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
        if (ev->Get()->Tag == 0 && Auth) {
            AuthDeadline = TInstant::Max();
            Pump();
        } else if (Runtime && Call && ev->Get()->Tag == Call) {
            Runtime->Expire(TInstant::Now());
            Pump();
        }
    }

    size_t BufferedRows() const {
        return Pending.size() + ActiveCount + Results.size();
    }

    bool DecodeResults(TStringBuf bytes) {
        if (bytes.size() > Config->BatchBytes) {
            return false;
        }
        TRowReader reader({ bytes.data(), bytes.size() });
        TServiceResult header;
        if (!reader.Get(header) || header.Version != ServiceVersion) {
            return false;
        }
        if (header.Error) {
            Fail(TStringBuilder() << "WASM service failed: error=" << header.Error << ", detail=" << header.Detail);
            return false;
        }
        if (header.Count != ActiveCount || header.Detail) {
            return false;
        }
        TDeque<TString> results;
        for (ui32 i = 0; i < header.Count; ++i) {
            const auto before = reader.Remaining();
            if (!ReadRow(reader, Method.Output)) {
                return false;
            }
            const auto size = before.size() - reader.Remaining().size();
            if (size > Method.MaxOutputRowBytes) {
                return false;
            }
            results.emplace_back(before.data(), size);
        }
        if (!reader.Remaining().empty()) {
            return false;
        }
        ActiveCount = 0;
        Results = std::move(results);
        return true;
    }

    void Pump() {
        if (Failed || !Runtime || !Output) {
            return;
        }
        try {
            if (Auth) {
                Auth->Process();
                const auto deadline = Auth->NextDeadline();
                if (deadline < AuthDeadline) {
                    AuthDeadline = deadline;
                    Schedule(std::max(TDuration::Zero(), deadline - TInstant::Now()), new NActors::TEvents::TEvWakeup(0));
                }
            }
            // All WASM entries happen on this mailbox. Native I/O only sends TEvReady.
            if (Call) {
                Runtime->TakeReady();
                const auto reply = Runtime->Poll(Call);
                if (reply.Status == NAsync::ECallStatus::Waiting || reply.Status == NAsync::ECallStatus::Runnable) {
                    return;
                }
                Runtime->Drop(std::exchange(Call, 0));
                if (reply.Status != NAsync::ECallStatus::Completed) {
                    Fail("WASM service execution failed or deadline expired");
                    return;
                }
                if (!DecodeResults(TStringBuf(reply.Data.data(), reply.Data.size()))) {
                    if (!Failed) {
                        Fail("Invalid WASM service result framing");
                    }
                    return;
                }
                if (InvocationTrace) {
                    InvocationTrace->Complete(EMetricOutcome::Success);
                }
                InvocationTrace.reset();
            }
            while (!Results.empty()) {
                auto guard = Guard(*Alloc);
                if (Output->GetFillLevel() == HardLimit) {
                    return;
                }
                NUdf::TUnboxedValue* members;
                auto row = HolderFactory.CreateDirectArrayHolder(Method.Output.size(), members);
                TRowReader reader({ Results.front().data(), Results.front().size() });
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
                    if (Output->GetFillLevel() == HardLimit) {
                        return;
                    }
                }
                ActiveCount = std::min<size_t>(DispatchRows, Pending.size());
                TString rows;
                for (ui32 i = 0; i < ActiveCount; ++i) {
                    rows += Pending.front();
                    Pending.pop_front();
                }
                const TServiceRequest header{ ServiceMagic, ServiceVersion, Method.Id, 0, Binding.Protocol == EProtocol::Http ? 0u : 1u,
                    ActiveCount, Config->BatchBytes, Config->BatchRows > 1 ? 1u : 0u };
                const auto bytes = Encode(header, { rows.data(), rows.size() });
                InvocationTrace = TCallTrace::Start(TraceContext, "WASM invocation");
                if (InvocationTrace) {
                    InvocationTrace->Attribute("invocation_id", i64(++InvocationSequence));
                    InvocationTrace->Attribute("batch_rows", i64(ActiveCount));
                }
                Transport->SetTraceContext(InvocationTrace ? InvocationTrace->Context() : TTraceContext{});
                if (Auth) {
                    Auth->SetTraceContext(InvocationTrace ? InvocationTrace->Context() : TTraceContext{});
                }
                InvocationDeadline = TInstant::Now() + Config->Timeout;
                Call = Runtime->Start(TStringBuf(bytes.data(), bytes.size()), InvocationDeadline);
                Schedule(Config->Timeout, new NActors::TEvents::TEvWakeup(Call));
                Send(SelfId(), new TEvReady);
            } else {
                auto guard = Guard(*Alloc);
                if (PendingWatermark || PendingCheckpoint) {
                    if (Output->GetFillLevel() == HardLimit) {
                        return;
                    }
                    if (PendingWatermark) {
                        Output->Consume(std::move(*PendingWatermark));
                        PendingWatermark.Clear();
                    }
                    if (PendingCheckpoint) {
                        Output->Consume(std::move(*PendingCheckpoint));
                        PendingCheckpoint.Clear();
                    }
                    // Consumers must retain the barrier position: FIFO buffers
                    // queue it, while v1 channels report HardLimit until it drains.
                    Output->Flush();
                    Callback->ResumeExecution();
                }
                if (!Finished || Acknowledged) {
                    return;
                }
                Output->Finish();
                Acknowledged = true;
                if (TransformTrace) {
                    TransformTrace->Attribute("detail_spans_dropped", i64(TraceContext.Budget->Dropped.load()));
                    TransformTrace->Complete(EMetricOutcome::Success);
                }
                Callback->OnAsyncOutputFinished(Index);
            }
        } catch (...) {
            Fail("WASM service execution failed");
        }
    }

    void Fail(const TString& message) {
        if (std::exchange(Failed, true)) {
            return;
        }
        const auto outcome = InvocationTrace && TInstant::Now() >= InvocationDeadline ? EMetricOutcome::Timeout : EMetricOutcome::Error;
        if (InvocationTrace) {
            InvocationTrace->Complete(outcome);
        }
        if (TransformTrace) {
            TransformTrace->Complete(outcome);
        }
        if (Auth) {
            Auth->Close();
        }
        Runtime.reset();
        Pending.clear();
        Results.clear();
        ActiveCount = 0;
        Call = 0;
        auto guard = Guard(*Alloc);
        Callback->OnAsyncOutputError(Index, NYql::TIssues{ NYql::TIssue(message) }, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
    }

    void Cleanup() {
        const auto outcome = InvocationTrace && TInstant::Now() >= InvocationDeadline ? EMetricOutcome::Timeout : EMetricOutcome::Cancelled;
        if (InvocationTrace) {
            InvocationTrace->Complete(outcome);
        }
        if (TransformTrace) {
            TransformTrace->Complete(outcome);
        }
        if (Auth) {
            Auth->Close();
        }
        Runtime.reset();
        Transport.reset();
        auto guard = Guard(*Alloc);
        Output.Reset();
    }

    const std::shared_ptr<const TPreparedConfig> Config;
    const TServiceMethod& Method;
    TBinding Binding;
    std::shared_ptr<TAuthModule> Auth;
    TInstant AuthDeadline = TInstant::Max();
    const std::shared_ptr<TSharedTransport> SharedTransport;
    const ui64 Index;
    IDqOutputConsumer::TPtr Output;
    ICallbacks* const Callback;
    const std::shared_ptr<TScopedAlloc> Alloc;
    const THolderFactory& HolderFactory;
    TVector<ui32> InputPositions, OutputPositions;
    ui32 MaxRows, DispatchRows, ActiveCount = 0;
    TDqAsyncStats Stats;
    TDeque<TString> Pending, Results;
    TMaybe<NYql::NDqProto::TWatermark> PendingWatermark;
    TMaybe<NYql::NDqProto::TCheckpoint> PendingCheckpoint;
    TTraceContext TraceContext;
    std::shared_ptr<TCallTrace> TransformTrace, InvocationTrace;
    ui64 InvocationSequence = 0;
    TInstant InvocationDeadline;
    NAsync::THandle Call = 0;
    bool Finished = false, Acknowledged = false, Failed = false;
    std::shared_ptr<TTransport> Transport;
    std::unique_ptr<NAsync::TRuntime> Runtime;
};

}   // namespace

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

TString ServiceInvocationSettings(const TString& invocation) {
    NJson::TJsonValue value;
    Y_ENSURE(NJson::ReadJsonTree(invocation, &value, true) && value.IsMap(), "Invalid WASM service invocation");
    return value.Has("connection_settings") ? value["connection_settings"].GetStringSafe() : TString();
}

NYql::TDqFunctionGatewayFactory::TPtr CreateServiceGatewayFactoryWithResolver(
    const NConfig::TWasmServicesConfig& config, TServiceConnectionResolver resolver) {
    Y_ENSURE(resolver, "Missing WASM service connection resolver");
    auto source = CreateServiceModuleSource(config);
    auto factory = MakeIntrusive<NYql::TDqFunctionGatewayFactory>();
    const ui32 bytes = config.GetMaxBatchBytes() ? config.GetMaxBatchBytes() : MaxServiceBatchBytes;
    const ui64 bufferedBytes = config.GetMaxBufferedBytes() ? config.GetMaxBufferedBytes() : 64 << 20;
    for (auto& module : source->Describe()) {
        auto manifest = std::make_shared<TServiceManifest>(std::move(module));
        factory->Register(manifest->Name, [manifest, resolver, bytes, bufferedBytes](const auto&, const TString& connection) {
            return std::make_shared<TServiceGateway>(manifest, connection, resolver(connection), bytes, bufferedBytes);
        });
    }
    return factory;
}

void RegisterServiceTransformsWithResolver(TDqAsyncIoFactory& factory, const NConfig::TWasmServicesConfig& config,
    TServiceBindingResolver resolver, ui32 userPool, NMonitoring::TDynamicCounterPtr counters) {
    Y_ENSURE(resolver, "Missing WASM service binding resolver");
    auto source = CreateServiceModuleSource(config);
    // Compile local modules at startup, never on a transform's user-pool turn.
    // Store modules arrive asynchronously and are already compiled by UDF Store.
    if (config.GetModuleSource() == NConfig::TWasmServicesConfig::LOCAL_FILES) {
        for (const auto& manifest : source->Describe()) {
            source->Load(manifest.Name);
        }
    }
    ValidateAuthProviders(config, *source);
    auto sharedTransport = std::make_shared<TSharedTransport>();
    sharedTransport->UserPool = userPool;
    if (counters) {
        sharedTransport->Metrics = std::make_shared<TRequestMetrics>(counters->GetSubgroup("component", "requests"));
    }
    if (config.GetMaxTransportOperations()) {
        sharedTransport->Limits.MaxOperations = config.GetMaxTransportOperations();
    }
    if (config.GetMaxTransportBytes()) {
        sharedTransport->Limits.MaxReservedBytes = config.GetMaxTransportBytes();
    }
    EnsureUdfHostIntrinsicsRegistered();
    NAsync::KeepAsyncHostIntrinsicsLinked();
    factory.RegisterOutputTransform<NYql::NProto::TFunctionTransform>(
        TString(ServiceTransformType), [source, config, resolver, sharedTransport](
                                           NYql::NProto::TFunctionTransform&& settings, IDqAsyncIoFactory::TOutputTransformArguments&& args) {
            NJson::TJsonValue invocation;
            Y_ENSURE(NJson::ReadJsonTree(settings.GetInvokeUrl(), &invocation, true) && invocation.IsMap(), "Invalid WASM service invocation");
            const auto module = source->Load(invocation["module"].GetStringSafe());
            Y_ENSURE(module->Manifest.Services, "Auth modules cannot be invoked as application services");
            auto prepared = std::make_shared<TPreparedConfig>();
            if (config.GetMaxTraceSpansPerTransform()) {
                prepared->MaxTraceSpans = config.GetMaxTraceSpansPerTransform();
            }
            prepared->Module = module->ModuleBytecode;
            prepared->Libraries = module->Libraries;
            prepared->Manifest = *module->Manifest.Services;
            if (config.GetMaxBufferedRows()) {
                prepared->MaxRows = config.GetMaxBufferedRows();
            }
            if (config.GetMaxBufferedBytes()) {
                prepared->MaxBufferedBytes = config.GetMaxBufferedBytes();
            }
            if (config.GetCallTimeoutMs()) {
                prepared->Timeout = TDuration::MilliSeconds(config.GetCallTimeoutMs());
            }
            if (config.GetMaxBatchRows()) {
                prepared->BatchRows = config.GetMaxBatchRows();
            }
            if (settings.HasBatchSize()) {
                prepared->BatchRows = settings.GetBatchSize();
            }
            if (config.GetMaxBatchBytes()) {
                prepared->BatchBytes = config.GetMaxBatchBytes();
            }
            const auto method = prepared->Manifest.Methods.find(invocation["method"].GetStringSafe());
            Y_ENSURE(method != prepared->Manifest.Methods.end(), "Unknown WASM service method");
            ValidateMethod(method->second, prepared->BatchBytes, prepared->BatchRows, prepared->MaxBufferedBytes);
            auto binding = resolver(settings.GetInvokeUrl(), args.SecureParams);
            if (binding.AuthConnection) {
                prepared->Auth = PrepareAuthModule(config, *source, *binding.AuthConnection);
                prepared->AuthAlias = "auth.wasm";
                for (const auto& provider : config.GetAuthProviders()) {
                    if (provider.GetProvider() == binding.AuthConnection->Provider && !provider.GetAlias().empty()) {
                        prepared->AuthAlias = provider.GetAlias();
                    }
                }
            }
            auto* actor = new TServiceTransform(prepared, method->second, std::move(binding), sharedTransport, std::move(args));
            return std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
        });
}

}   // namespace NFq::NWasmServices
