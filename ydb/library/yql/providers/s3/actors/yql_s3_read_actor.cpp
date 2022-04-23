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

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvReadResult : public TEventLocal<TEvReadResult, EvReadResult> {
        TEvReadResult(IHTTPGateway::TContent&& result, size_t pathInd = 0U): Result(std::move(result)), PathIndex(pathInd) {}
        IHTTPGateway::TContent Result;
        const size_t PathIndex;
    };

    struct TEvReadFinished : public TEventLocal<TEvReadFinished, EvReadFinished> {};

    struct TEvReadError : public TEventLocal<TEvReadError, EvReadError> {
        TEvReadError(TIssues&& error, size_t pathInd = 0U) : Error(std::move(error)), PathIndex(pathInd) {}
        const TIssues Error;
        const size_t PathIndex;
    };

    struct TEvRetryEvent : public NActors::TEventLocal<TEvRetryEvent, EvRetry> {
        explicit TEvRetryEvent(size_t pathIndex) : PathIndex(pathIndex) {}
        const size_t PathIndex;
    };

    struct TEvRetryEventFunc : public NActors::TEventLocal<TEvRetryEventFunc, EvRetry> {
        explicit TEvRetryEventFunc(std::function<void()> functor) : Functor(std::move(functor)) {}
        const std::function<void()> Functor;
    };
};

using TPath = std::tuple<TString, size_t>;
using TPathList = std::vector<TPath>;

class TRetryParams {
public:
    TRetryParams(const std::shared_ptr<NS3::TRetryConfig>& retryConfig)
        : MaxRetries(retryConfig && retryConfig->GetMaxRetriesPerPath() ? retryConfig->GetMaxRetriesPerPath() : 3U)
        , InitDelayMs(retryConfig && retryConfig->GetInitialDelayMs() ? TDuration::MilliSeconds(retryConfig->GetInitialDelayMs()) : TDuration::MilliSeconds(100))
        , InitEpsilon(retryConfig && retryConfig->GetEpsilon() ? retryConfig->GetEpsilon() : 0.1)
    {
        Y_VERIFY(0. < InitEpsilon && InitEpsilon < 1.);
        Reset();
    }

    void Reset() {
        Retries = 0U;
        DelayMs = InitDelayMs;
        Epsilon = InitEpsilon;
    }

    TDuration GetNextDelay() {
        if (++Retries > MaxRetries)
            return TDuration::Zero();
        return DelayMs = GenerateNextDelay();
    }
private:
    TDuration GenerateNextDelay() {
        const auto low = 1. - Epsilon;
        const auto jitter = low + std::rand() / (RAND_MAX / (2. * Epsilon));
        return DelayMs * jitter;
    }

    const ui32 MaxRetries;
    const TDuration InitDelayMs;
    const double InitEpsilon;

    ui32 Retries;
    TDuration DelayMs;
    double Epsilon;
};

class TS3ReadActor : public TActorBootstrapped<TS3ReadActor>, public IDqSourceActor {
public:
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
    {}

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
        Gateway->Download(Url + std::get<TString>(Paths[pathInd]),
            Headers, std::get<size_t>(Paths[pathInd]),
            std::bind(&TS3ReadActor::OnDownloadFinished, ActorSystem, SelfId(), std::placeholders::_1, pathInd));
    }

    void Handle(TEvPrivate::TEvReadError::TPtr& result) {
        const auto pathInd = result->Get()->PathIndex;
        Y_VERIFY(pathInd < RetriesPerPath.size());
        if (auto nextDelayMs = RetriesPerPath[pathInd].GetNextDelay()) {
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
        : TActorCoroImpl(512_KB), TypeEnv(typeEnv), FunctionRegistry(functionRegistry), InputIndex(inputIndex), Format(std::move(format)), RowType(std::move(rowType)), ComputeActorId(computeActorId), Outputs(std::move(outputs))
    {}

    bool Next(NUdf::TUnboxedValue& value) {
        if (Finished)
            return false;

        TAllocState *const allocState = TlsAllocState;
        Y_VERIFY(allocState == AllocState, "Wrong TLS alloc state pre check.");
        TypeEnv.GetAllocator().Release();
        const auto ev = WaitForSpecificEvent<TEvPrivate::TEvReadResult, TEvPrivate::TEvReadError, TEvPrivate::TEvReadFinished>();
        TypeEnv.GetAllocator().Acquire();
        Y_VERIFY(allocState == AllocState, "Wrong TLS alloc state post check.");
        switch (const auto etype = ev->GetTypeRewrite()) {
            case TEvPrivate::TEvReadFinished::EventType:
                Finished = true;
                return false;
            case TEvPrivate::TEvReadError::EventType:
                Send(ComputeActorId, new IDqSourceActor::TEvSourceError(InputIndex, ev->Get<TEvPrivate::TEvReadError>()->Error, true));
                return false;
            case TEvPrivate::TEvReadResult::EventType:
                value = MakeString(NUdf::TStringRef(std::string_view(ev->Get<TEvPrivate::TEvReadResult>()->Result)));
                Send(ComputeActorId, new IDqSourceActor::TEvNewSourceDataArrived(InputIndex));
                return true;
            default:
                return false;
        }
    }
private:
    void Run() final try {
        TOutput::TPtr outputs;
        // reset member to decrement ref, important for UV lifetime
        Outputs.swap(outputs);

        const auto randStub = CreateDeterministicRandomProvider(1);
        const auto timeStub = CreateDeterministicTimeProvider(10000000);

        const auto alloc = TypeEnv.BindAllocator();
        AllocState = TlsAllocState;
        const auto pb = std::make_unique<TProgramBuilder>(TypeEnv, FunctionRegistry);

        TCallableBuilder callableBuilder(TypeEnv, "CoroStream", pb->NewStreamType(pb->NewDataType(NUdf::EDataSlot::String)));

        const auto factory = [this](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            return callable.GetType()->GetName() == "CoroStream" ?
                TCoroStreamWrapper::Make(callable, ctx, this) : GetBuiltinFactory()(callable, ctx);
        };

        TRuntimeNode stream(callableBuilder.Build(), false);

        const auto outputItemType = NCommon::ParseTypeFromYson(TStringBuf(RowType), *pb,  Cerr);
        const auto userType = pb->NewTupleType({pb->NewTupleType({pb->NewDataType(NUdf::EDataSlot::String)}), pb->NewStructType({}), outputItemType});
        const auto root = pb->Apply(pb->Udf("ClickHouseClient.ParseSource", {}, userType, Format), {stream});

        TExploringNodeVisitor explorer;
        explorer.Walk(root.GetNode(), TypeEnv);
        TComputationPatternOpts opts(TypeEnv.GetAllocator().Ref(), TypeEnv, factory, &FunctionRegistry, NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception,  "OFF", EGraphPerProcess::Single);
        const auto pattern = MakeComputationPattern(explorer, root, {}, opts);
        const auto graph = pattern->Clone(opts.ToComputationOptions(*randStub, *timeStub));
        const TBindTerminator bind(graph->GetTerminator());

        const auto output = graph->GetValue();
        for (NUdf::TUnboxedValue v; NUdf::EFetchStatus::Ok == output.Fetch(v);)
            outputs->Data.emplace_back(std::move(v));

        outputs = nullptr;
        Send(ComputeActorId, new IDqSourceActor::TEvNewSourceDataArrived(InputIndex));
    } catch (const std::exception& err) {
        Send(ComputeActorId, new IDqSourceActor::TEvSourceError(InputIndex, TIssues{TIssue(err.what())}, true));
        return;
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
    TAllocState * AllocState = nullptr;
    bool Finished = false;
};

class TS3ReadCoroActor : public TActorCoro {
    struct TRetryStuff {
        using TPtr = std::shared_ptr<TRetryStuff>;

        TRetryStuff(
            IHTTPGateway::TPtr gateway,
            TString url,
            const IHTTPGateway::THeaders& headers,
            const std::shared_ptr<NS3::TRetryConfig>& retryConfig,
            std::size_t expectedSize
        ) : Gateway(std::move(gateway)), Url(std::move(url)), Headers(headers), ExpectedSize(expectedSize), Offset(0U), RetryParams(retryConfig)
        {}

        const IHTTPGateway::TPtr Gateway;
        const TString Url;
        const IHTTPGateway::THeaders Headers;
        const std::size_t ExpectedSize;

        std::size_t Offset = 0U;
        TRetryParams RetryParams;
    };
public:
    TS3ReadCoroActor(THolder<TS3ReadCoroImpl> impl,
        IHTTPGateway::TPtr gateway,
        const TString& url,
        const IHTTPGateway::THeaders& headers,
        const TString& path,
        const std::size_t expectedSize,
        const std::shared_ptr<NS3::TRetryConfig>& retryConfig,
        TOutput::TPtr outputs)
        : TActorCoro(THolder<TActorCoroImpl>(impl.Release()))
        , RetryStuff(std::make_shared<TRetryStuff>(std::move(gateway), url + path, headers, retryConfig, expectedSize))
        , Outputs(std::move(outputs))
    {}
private:
    static void OnNewData(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, const TRetryStuff::TPtr& retryStuff, IHTTPGateway::TContent&& data) {
        retryStuff->Offset += data.size();
        retryStuff->RetryParams.Reset();
        actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvReadResult(std::move(data))));
    }

    static void OnDownloadFinished(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, const TRetryStuff::TPtr& retryStuff, std::optional<TIssues> result) {
        if (result)
            if (const auto nextDelayMs = retryStuff->RetryParams.GetNextDelay())
                actorSystem->Schedule(nextDelayMs, new IEventHandle(parent, self, new TEvPrivate::TEvRetryEventFunc(std::bind(&TS3ReadCoroActor::DownloadStart, retryStuff, self, parent))));
            else
                actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvReadError(TIssues{*result})));
        else
            actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvReadFinished));
    }

    static void DownloadStart(const TRetryStuff::TPtr& retryStuff, const TActorId& self, const TActorId& parent) {
        retryStuff->Gateway->Download(retryStuff->Url,
            retryStuff->Headers, retryStuff->Offset,
            std::bind(&TS3ReadCoroActor::OnNewData, TActivationContext::ActorSystem(), self, parent, retryStuff, std::placeholders::_1),
            std::bind(&TS3ReadCoroActor::OnDownloadFinished, TActivationContext::ActorSystem(), self, parent, retryStuff, std::placeholders::_1));
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parent) {
        DownloadStart(RetryStuff, self, parent);
        return TActorCoro::AfterRegister(self, parent);
    }

    static IHTTPGateway::THeaders MakeHeader(const TString& token) {
        return token.empty() ? IHTTPGateway::THeaders() : IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

    const TRetryStuff::TPtr RetryStuff;
    TOutput::TPtr Outputs;
};

class TS3StreamReadActor : public TActorBootstrapped<TS3StreamReadActor>, public IDqSourceActor {
public:
    TS3StreamReadActor(
        const TTypeEnvironment& typeEnv,
        const IFunctionRegistry& functionRegistry,
        ui64 inputIndex,
        IHTTPGateway::TPtr gateway,
        const TString& url,
        const TString& token,
        TPathList&& paths,
        TString format,
        TString rowType,
        const NActors::TActorId& computeActorId,
        const std::shared_ptr<NS3::TRetryConfig>& retryConfig
    )   : TypeEnv(typeEnv)
        , FunctionRegistry(functionRegistry)
        , Gateway(std::move(gateway))
        , InputIndex(inputIndex)
        , ComputeActorId(computeActorId)
        , Url(url)
        , Headers(MakeHeader(token))
        , Paths(std::move(paths))
        , Format(format)
        , RowType(rowType)
        , RetryConfig(retryConfig)
        , Outputs(std::make_shared<TOutput>())
    {}

    void Bootstrap() {
        Become(&TS3StreamReadActor::StateFunc);
        for (const auto& path : Paths) {
            auto impl = MakeHolder<TS3ReadCoroImpl>(TypeEnv, FunctionRegistry, InputIndex, ComputeActorId, Paths.size(), Format, RowType, Outputs);
            RegisterWithSameMailbox(MakeHolder<TS3ReadCoroActor>(std::move(impl), Gateway, Url, Headers, std::get<TString>(path), std::get<std::size_t>(path), RetryConfig, Outputs).Release());
        }
    }

    static constexpr char ActorName[] = "S3_READ_ACTOR";

private:
    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {}
    void LoadState(const NDqProto::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}
    ui64 GetInputIndex() const final { return InputIndex; }

    i64 GetSourceData(NKikimr::NMiniKQL::TUnboxedValueVector& buffer, bool& finished, i64) final {
        if (!Outputs)
            return 0LL;

        i64 total = Outputs->Data.size();
        std::move(Outputs->Data.begin(), Outputs->Data.end(), std::back_inserter(buffer));
        Outputs->Data.clear();

        if (Outputs.unique()) {
            finished = true;
            Outputs.reset();
        }

        return total;
    }

    // IActor & IDqSourceActor
    void PassAway() override { // Is called from Compute Actor
        Outputs = nullptr;
        TActorBootstrapped<TS3StreamReadActor>::PassAway();
    }

    static IHTTPGateway::THeaders MakeHeader(const TString& token) {
        return token.empty() ? IHTTPGateway::THeaders() : IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvRetryEventFunc, HandleRetry);
    )

    void HandleRetry(TEvPrivate::TEvRetryEventFunc::TPtr& retry) {
        return retry->Get()->Functor();
    }

    const TTypeEnvironment& TypeEnv;
    const IFunctionRegistry& FunctionRegistry;

    const IHTTPGateway::TPtr Gateway;

    const ui64 InputIndex;
    const NActors::TActorId ComputeActorId;

    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TPathList Paths;
    const TString Format, RowType, Compression;
    const std::shared_ptr<NS3::TRetryConfig> RetryConfig;

    TOutput::TPtr Outputs;
};

} // namespace

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

    TPathList paths;
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

    if (params.HasFormat() && params.HasRowType()) {
        const auto actor = new TS3StreamReadActor(typeEnv, functionRegistry, inputIndex, std::move(gateway), params.GetUrl(), authToken, std::move(paths), params.GetFormat(), params.GetRowType(), computeActorId, retryConfig);
        return {actor, actor};
    } else {
        const auto actor = new TS3ReadActor(inputIndex, std::move(gateway), params.GetUrl(), authToken, std::move(paths), computeActorId, retryConfig);
        return {actor, actor};
    }
}

} // namespace NYql::NDq
