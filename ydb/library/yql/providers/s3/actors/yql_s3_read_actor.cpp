#include "yql_s3_read_actor.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/providers/s3/proto/range.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actor_coroutine.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/generic/size_literals.h>

#include <queue>

namespace NYql::NDq {

using namespace NActors;

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvReadResult = EvBegin,
        EvReadFinished,
        EvReadError,
        EvRetry,
        EvReadDone,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvReadResult : public TEventLocal<TEvReadResult, EvReadResult> {
        TEvReadResult(IHTTPGateway::TContent&& result, size_t pathInd): Result(std::move(result)), PathIndex(pathInd) {}
        IHTTPGateway::TContent Result;
        const size_t PathIndex = 0;
    };

    struct TEvReadFinished : public TEventLocal<TEvReadFinished, EvReadFinished> {
        TEvReadFinished(size_t pathInd) : PathIndex(pathInd) {}
        const size_t PathIndex = 0;
    };

    struct TEvReadError : public TEventLocal<TEvReadError, EvReadError> {
        TEvReadError(TIssues&& error, size_t pathInd) : Error(std::move(error)), PathIndex(pathInd) {}
        const TIssues Error;
        const size_t PathIndex = 0;
    };

    struct TEvRetryEvent : public NActors::TEventLocal<TEvRetryEvent, EvRetry> {
        explicit TEvRetryEvent(size_t pathIndex) : PathIndex(pathIndex) {}
        const size_t PathIndex = 0;
    };

    struct TEvReadDone : public TEventLocal<TEvReadDone, EvReadDone> {};
};

} // namespace

class TS3ReadActor : public TActorBootstrapped<TS3ReadActor>, public IDqSourceActor {
private:
    class TRetryParams {
    public:
        TRetryParams(const std::shared_ptr<NS3::TRetryConfig>& retryConfig) {
            if (retryConfig) {
                DelayMs = retryConfig->GetInitialDelayMs() ? TDuration::MilliSeconds(retryConfig->GetInitialDelayMs()) : DelayMs;
                Epsilon = retryConfig->GetEpsilon() ? retryConfig->GetEpsilon() : Epsilon;
                Y_VERIFY(0.0 < Epsilon && Epsilon < 1.0);
            }
        }

        TDuration GetNextDelay(ui32 maxRetries) {
            if (Retries > maxRetries) return TDuration::Zero();
            return DelayMs = GenerateNextDelay();
        };

        void IncRetries() {
            ++Retries;
        }

    private:
        TDuration GenerateNextDelay() {
            double low = 1 - Epsilon;
            auto jitter = low + std::rand() / (RAND_MAX / (2 * Epsilon));
            return DelayMs * jitter;
        }

        ui32 Retries = 0;
        TDuration DelayMs = TDuration::MilliSeconds(100);
        double Epsilon = 0.1;
    };
public:
using TPath = std::tuple<TString, size_t>;
using TPathList = std::vector<TPath>;

    TS3ReadActor(ui64 inputIndex,
        IHTTPGateway::TPtr gateway,
        const TString& url,
        const TString& token,
        TPathList&& paths,
        const NActors::TActorId& computeActorId,
        const std::shared_ptr<NS3::TRetryConfig>& retryConfig
    )   : Gateway(std::move(gateway))
        , InputIndex(inputIndex)
        , ComputeActorId(computeActorId)
        , ActorSystem(TActivationContext::ActorSystem())
        , Url(url)
        , Headers(MakeHeader(token))
        , Paths(std::move(paths))
        , RetryConfig(retryConfig)
    {
        if (RetryConfig) {
            MaxRetriesPerPath = RetryConfig->GetMaxRetriesPerPath() ? RetryConfig->GetMaxRetriesPerPath() : MaxRetriesPerPath;
        }
    }

    void Bootstrap() {
        Become(&TS3ReadActor::StateFunc);
        RetriesPerPath.resize(Paths.size(), TRetryParams(RetryConfig));
        for (size_t pathInd = 0; pathInd < Paths.size(); ++pathInd) {
            const TPath& path = Paths[pathInd];
            Gateway->Download(Url + std::get<TString>(path),
                Headers, std::get<size_t>(path),
                std::bind(&TS3ReadActor::OnDownloadFinished, ActorSystem, SelfId(), std::placeholders::_1, pathInd));
        };
    }

    static constexpr char ActorName[] = "S3_READ_ACTOR";

private:
    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {}
    void LoadState(const NDqProto::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}
    ui64 GetInputIndex() const final { return InputIndex; }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvReadResult, Handle);
        hFunc(TEvPrivate::TEvReadError, Handle);
        hFunc(TEvPrivate::TEvRetryEvent, HandleRetry);
    )

    static void OnDownloadFinished(TActorSystem* actorSystem, TActorId selfId, IHTTPGateway::TResult&& result, size_t pathInd) {
        switch (result.index()) {
        case 0U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadResult(std::get<IHTTPGateway::TContent>(std::move(result)), pathInd)));
            return;
        case 1U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadError(std::get<TIssues>(std::move(result)), pathInd)));
            return;
        default:
            break;
        }
    }

    i64 GetSourceData(NKikimr::NMiniKQL::TUnboxedValueVector& buffer, bool& finished, i64 freeSpace) final {
        i64 total = 0LL;
        if (!Blocks.empty()) {
            buffer.reserve(buffer.size() + Blocks.size());
            do {
                const auto size = Blocks.front().size();
                buffer.emplace_back(NKikimr::NMiniKQL::MakeString(std::string_view(Blocks.front())));
                Blocks.pop();
                total += size;
                freeSpace -= size;
            } while (!Blocks.empty() && freeSpace > 0LL);
        }

        if (Blocks.empty() && IsDoneCounter == Paths.size()) {
            finished = true;
        }

        return total;
    }


    void Handle(TEvPrivate::TEvReadResult::TPtr& result) {
        ++IsDoneCounter;
        Blocks.emplace(std::move(result->Get()->Result));
        Send(ComputeActorId, new TEvNewSourceDataArrived(InputIndex));
    }

    void HandleRetry(TEvPrivate::TEvRetryEvent::TPtr& ev) {
        const auto pathInd = ev->Get()->PathIndex;
        RetriesPerPath[pathInd].IncRetries();
        Gateway->Download(Url + std::get<TString>(Paths[pathInd]),
            Headers, std::get<size_t>(Paths[pathInd]),
            std::bind(&TS3ReadActor::OnDownloadFinished, ActorSystem, SelfId(), std::placeholders::_1, pathInd));
    }

    void Handle(TEvPrivate::TEvReadError::TPtr& result) {
        const auto pathInd = result->Get()->PathIndex;
        Y_VERIFY(pathInd < RetriesPerPath.size());
        if (auto nextDelayMs = RetriesPerPath[pathInd].GetNextDelay(MaxRetriesPerPath)) {
            Schedule(nextDelayMs, new TEvPrivate::TEvRetryEvent(pathInd));
            return;
        }
        ++IsDoneCounter;
        Send(ComputeActorId, new TEvSourceError(InputIndex, result->Get()->Error, true));
    }

    // IActor & IDqSourceActor
    void PassAway() override { // Is called from Compute Actor
        TActorBootstrapped<TS3ReadActor>::PassAway();
    }

    static IHTTPGateway::THeaders MakeHeader(const TString& token) {
        return token.empty() ? IHTTPGateway::THeaders() : IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

private:
    size_t IsDoneCounter = 0U;

    const IHTTPGateway::TPtr Gateway;

    const ui64 InputIndex;
    const NActors::TActorId ComputeActorId;

    TActorSystem* const ActorSystem;

    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TPathList Paths;

    std::queue<IHTTPGateway::TContent> Blocks;

    std::vector<TRetryParams> RetriesPerPath;
    const std::shared_ptr<NS3::TRetryConfig> RetryConfig;
    ui32 MaxRetriesPerPath = 3;
};

using namespace NKikimr::NMiniKQL;

struct TOutput {
    TUnboxedValueDeque Data;
    using TPtr = std::shared_ptr<TOutput>;
};

class TS3ReadCoroImpl : public TActorCoroImpl {
private:
    class TCoroStreamWrapper: public TMutableComputationNode<TCoroStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TCoroStreamWrapper>;
    public:
        class TStreamValue : public TComputationValue<TStreamValue> {
        public:
            using TBase = TComputationValue<TStreamValue>;

            TStreamValue(TMemoryUsageInfo* memInfo, TS3ReadCoroImpl* coro)
                : TBase(memInfo), Coro(coro)
            {}

        private:
            NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) final {
                return Coro->Next(value) ? NUdf::EFetchStatus::Ok : NUdf::EFetchStatus::Finish;
            }

        private:
            TS3ReadCoroImpl *const Coro;
        };

        TCoroStreamWrapper(TComputationMutables& mutables, TS3ReadCoroImpl* coro)
            : TBaseComputation(mutables), Coro(coro)
        {}

        NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
            return ctx.HolderFactory.Create<TStreamValue>(Coro);
        }

        static IComputationNode* Make(TCallable&, const TComputationNodeFactoryContext& ctx, TS3ReadCoroImpl* coro) {
            return new TCoroStreamWrapper(ctx.Mutables, coro);
        }
    private:
        void RegisterDependencies() const final {}
        TS3ReadCoroImpl *const Coro;
    };

public:
    TS3ReadCoroImpl(const TTypeEnvironment& typeEnv, const IFunctionRegistry& functionRegistry, ui64 inputIndex, const NActors::TActorId& computeActorId, ui64, TString format, TString rowType, TOutput::TPtr outputs)
        : TActorCoroImpl(256_KB), TypeEnv(typeEnv), FunctionRegistry(functionRegistry), InputIndex(inputIndex), Format(std::move(format)), RowType(std::move(rowType)), ComputeActorId(computeActorId), Outputs(std::move(outputs))
    {}

    bool Next(NUdf::TUnboxedValue& value) {
        TypeEnv.GetAllocator().Release();
        const auto ev = WaitForSpecificEvent<TEvPrivate::TEvReadResult, TEvPrivate::TEvReadError, TEvPrivate::TEvReadFinished>();
        TypeEnv.GetAllocator().Acquire();
        switch (const auto etype = ev->GetTypeRewrite()) {
            case TEvPrivate::TEvReadFinished::EventType:
                return false;
            case TEvPrivate::TEvReadError::EventType:
                return false;
            case TEvPrivate::TEvReadResult::EventType:
                value = NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(std::string_view(ev->Get<TEvPrivate::TEvReadResult>()->Result))));
                return true;
            default:
                return false;
        }
    }
private:
    void Run() final {
        try {
            const auto randStub = CreateDeterministicRandomProvider(1);
            const auto timeStub = CreateDeterministicTimeProvider(10000000);

            const auto alloc = TypeEnv.BindAllocator();
            const auto pb = std::make_unique<TProgramBuilder>(TypeEnv, FunctionRegistry);

            TCallableBuilder callableBuilder(TypeEnv, "CoroStream", pb->NewStreamType(pb->NewDataType(NUdf::EDataSlot::String)));

            const auto factory = [this](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
                return callable.GetType()->GetName() == "CoroStream" ?
                    TCoroStreamWrapper::Make(callable, ctx, this) : GetBuiltinFactory()(callable, ctx);
            };

            TRuntimeNode stream(callableBuilder.Build(), false);

            const auto outputItemType = NCommon::ParseTypeFromYson(TStringBuf(RowType), *pb,  Cerr);
            const auto userType = pb->NewTupleType({pb->NewTupleType({pb->NewDataType(NUdf::EDataSlot::String)}), pb->NewStructType({}), outputItemType});
            const auto root = pb->Apply(pb->Udf("ClickHouseClient.ParseFormat", {}, userType, Format), {stream});

            TExploringNodeVisitor explorer;
            explorer.Walk(root.GetNode(), TypeEnv);
            TComputationPatternOpts opts(TypeEnv.GetAllocator().Ref(), TypeEnv, factory, &FunctionRegistry, NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception,  "OFF", EGraphPerProcess::Single);
            const auto pattern = MakeComputationPattern(explorer, root, {}, opts);
            const auto graph = pattern->Clone(opts.ToComputationOptions(*randStub, *timeStub));
            const TBindTerminator bind(graph->GetTerminator());

            const auto output = graph->GetValue();
            for (NUdf::TUnboxedValue v; NUdf::EFetchStatus::Ok == output.Fetch(v);)
                Outputs->Data.emplace_back(std::move(v));

            Outputs.reset();
        } catch (const yexception& err) {
            Cerr << __func__ << " exception " << err.what() << Endl;
            return;
        }

        Y_UNUSED(WaitForSpecificEvent<TEvPrivate::TEvReadDone>());
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle>) final {
        Send(ComputeActorId, new IDqSourceActor::TEvSourceError(InputIndex, TIssues{TIssue("Unexpected event")}, true));
    }
private:
    const TTypeEnvironment& TypeEnv;
    const IFunctionRegistry& FunctionRegistry;
    const ui64 InputIndex;
    const TString Format, RowType, Compression;
    const NActors::TActorId ComputeActorId;
    TOutput::TPtr Outputs;
};

class TS3ReadCoroActor : public TActorCoro, public IDqSourceActor {
public:
    using TPath = std::tuple<TString, size_t>;
    using TPathList = std::vector<TPath>;

    TS3ReadCoroActor(THolder<TS3ReadCoroImpl> impl,
        ui64 inputIndex,
        IHTTPGateway::TPtr gateway,
        const TString& url,
        const TString& token,
        TPathList&& paths,
        TOutput::TPtr outputs)
        : TActorCoro(THolder<TActorCoroImpl>(impl.Release()))
        , InputIndex(inputIndex)
        , Gateway(std::move(gateway))
        , Url(url)
        , Headers(MakeHeader(token))
        , Paths(std::move(paths))
        , Outputs(std::move(outputs))
    {}

    static constexpr char ActorName[] = "S3_READ_ACTOR_CORO";
private:
    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {}
    void LoadState(const NDqProto::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}
    ui64 GetInputIndex() const final { return InputIndex; }

    i64 GetSourceData(NKikimr::NMiniKQL::TUnboxedValueVector& buffer, bool& finished, i64) final {
        i64 total = Outputs->Data.size();
        std::move(Outputs->Data.begin(), Outputs->Data.end(), std::back_inserter(buffer));
        Outputs->Data.clear();

        if (Outputs.unique()) {
            finished = true;
            Send(SelfId(), new TEvPrivate::TEvReadDone);
        }

        return total;
    }

    void PassAway() final {
//Todo  return TActorCoro::PassAway();
    }


    static void OnNewData(TActorSystem* actorSystem, TActorId selfId, IHTTPGateway::TContent&& data, size_t pathInd) {
        actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadResult(std::move(data), pathInd)));
    }

    static void OnDownloadFinished(TActorSystem* actorSystem, TActorId selfId, std::optional<TIssues> result, size_t pathInd) {
        if (result)
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadError(TIssues{*result}, pathInd)));
        else
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadFinished(pathInd)));
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parent) {
        for (size_t pathInd = 0; pathInd < Paths.size(); ++pathInd) {
            const auto& path = Paths[pathInd];
            Gateway->Download(Url + std::get<TString>(path),
                Headers, std::get<size_t>(path),
                std::bind(&TS3ReadCoroActor::OnNewData, TActivationContext::ActorSystem(),  self, std::placeholders::_1, pathInd),
                std::bind(&TS3ReadCoroActor::OnDownloadFinished, TActivationContext::ActorSystem(),  self, std::placeholders::_1, pathInd));
        };

        return TActorCoro::AfterRegister(self, parent);
    }

    static IHTTPGateway::THeaders MakeHeader(const TString& token) {
        return token.empty() ? IHTTPGateway::THeaders() : IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

    const ui64 InputIndex;

    const IHTTPGateway::TPtr Gateway;

    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TPathList Paths;
    const TOutput::TPtr Outputs;
};

std::pair<NYql::NDq::IDqSourceActor*, IActor*> CreateS3ReadActor(
    const TTypeEnvironment& typeEnv,
    const IFunctionRegistry& functionRegistry,
    IHTTPGateway::TPtr gateway,
    NS3::TSource&& params,
    ui64 inputIndex,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const NActors::TActorId& computeActorId,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const std::shared_ptr<NS3::TRetryConfig>& retryConfig)
{
    std::unordered_map<TString, size_t> map(params.GetPath().size());
    for (auto i = 0; i < params.GetPath().size(); ++i)
        map.emplace(params.GetPath().Get(i).GetPath(), params.GetPath().Get(i).GetSize());

    TS3ReadActor::TPathList paths;
    paths.reserve(map.size());
    if (const auto taskParamsIt = taskParams.find(S3ProviderName); taskParamsIt != taskParams.cend()) {
        NS3::TRange range;
        TStringInput input(taskParamsIt->second);
        range.Load(&input);
        for (auto i = 0; i < range.GetPath().size(); ++i) {
            const auto& path = range.GetPath().Get(i);
            paths.emplace_back(path, map[path]);
        }
    } else
        while (auto item = map.extract(map.cbegin()))
            paths.emplace_back(std::move(item.key()), std::move(item.mapped()));

    const auto token = secureParams.Value(params.GetToken(), TString{});
    const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();

    if (params.HasFormat()) {
        auto outputs = std::make_shared<TOutput>();
        auto impl = MakeHolder<TS3ReadCoroImpl>(typeEnv, functionRegistry, inputIndex, computeActorId, paths.size(), params.GetFormat(), params.GetRowType(), outputs);
        const auto actor = new TS3ReadCoroActor(std::move(impl), inputIndex, std::move(gateway), params.GetUrl(), authToken, std::move(paths), outputs);
        return {actor, actor};
    } else {
        const auto actor = new TS3ReadActor(inputIndex, std::move(gateway), params.GetUrl(), authToken, std::move(paths), computeActorId, retryConfig);
        return {actor, actor};
    }
}

} // namespace NYql::NDq
