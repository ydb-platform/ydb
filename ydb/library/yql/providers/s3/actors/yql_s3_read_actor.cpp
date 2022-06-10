#ifdef __linux__
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeEnum.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypesNumber.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeDate.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeFactory.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeArray.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeNothing.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeTuple.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeNullable.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeString.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeUUID.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/Block.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/ColumnsWithTypeAndName.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatFactory.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/InputStreamFromInputFormat.h>
#endif

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

#include <ydb/library/yql/providers/s3/compressors/factory.h>
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
        EvNextBlock,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvReadResult : public TEventLocal<TEvReadResult, EvReadResult> {
        TEvReadResult(IHTTPGateway::TContent&& result, size_t pathInd = 0U): Result(std::move(result)), PathIndex(pathInd) {}
        IHTTPGateway::TContent Result;
        const size_t PathIndex;
    };

    struct TEvDataPart : public TEventLocal<TEvDataPart, EvReadResult> {
        TEvDataPart(IHTTPGateway::TCountedContent&& data) : Result(std::move(data)) {}
        IHTTPGateway::TCountedContent Result;
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

    struct TEvNextBlock : public NActors::TEventLocal<TEvNextBlock, EvNextBlock> {
        explicit TEvNextBlock(NDB::Block& block) { Block.swap(block); }
        NDB::Block Block;
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

class TS3ReadActor : public TActorBootstrapped<TS3ReadActor>, public IDqComputeActorAsyncInput {
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

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueVector& buffer, bool& finished, i64 freeSpace) final {
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
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
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
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, result->Get()->Error, true));
    }

    // IActor & IDqComputeActorAsyncInput
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

struct TReadSpec {
    using TPtr = std::shared_ptr<TReadSpec>;

    NDB::ColumnsWithTypeAndName Columns;
    NDB::FormatSettings Settings;
    TString Format, Compression;
};

class TS3ReadCoroImpl : public TActorCoroImpl {
private:
    class TReadBufferFromStream : public NDB::ReadBuffer {
    public:
        TReadBufferFromStream(TS3ReadCoroImpl* coro)
            : NDB::ReadBuffer(nullptr, 0ULL), Coro(coro)
        {}
    private:
        bool nextImpl() final {
            if (Coro->Next(Value)) {
                working_buffer = NDB::BufferBase::Buffer(const_cast<char*>(Value.data()), const_cast<char*>(Value.data()) + Value.size());
                return true;
            }

            return false;
        }

        TS3ReadCoroImpl *const Coro;
        TString Value;
    };
public:
    TS3ReadCoroImpl(ui64 inputIndex, const NActors::TActorId& sourceActorId, const NActors::TActorId& computeActorId, const TReadSpec::TPtr& readSpec)
        : TActorCoroImpl(256_KB), InputIndex(inputIndex), ReadSpec(readSpec), SourceActorId(sourceActorId), ComputeActorId(computeActorId)
    {}

    bool Next(TString& value) {
        if (Finished)
            return false;

        const auto ev = WaitForSpecificEvent<TEvPrivate::TEvDataPart, TEvPrivate::TEvReadError, TEvPrivate::TEvReadFinished>();
        switch (const auto etype = ev->GetTypeRewrite()) {
            case TEvPrivate::TEvReadFinished::EventType:
                Finished = true;
                return false;
            case TEvPrivate::TEvReadError::EventType:
                Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, ev->Get<TEvPrivate::TEvReadError>()->Error, true));
                return false;
            case TEvPrivate::TEvDataPart::EventType:
                value = ev->Get<TEvPrivate::TEvDataPart>()->Result.Extract();
                Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
                return true;
            default:
                return false;
        }
    }
private:
    void Run() final try {
        TReadBufferFromStream buffer(this);
        const auto decompress(MakeDecompressor(buffer, ReadSpec->Compression));
        YQL_ENSURE(ReadSpec->Compression.empty() == !decompress, "Unsupported " <<ReadSpec->Compression << " compression.");
        NDB::InputStreamFromInputFormat stream(NDB::FormatFactory::instance().getInputFormat(ReadSpec->Format, decompress ? *decompress : buffer, NDB::Block(ReadSpec->Columns), nullptr, 1_MB, ReadSpec->Settings));

        while (auto block = stream.read())
            Send(SourceActorId, new TEvPrivate::TEvNextBlock(block));

        Send(SourceActorId, new TEvPrivate::TEvReadFinished);
    } catch (const std::exception& err) {
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, TIssues{TIssue(err.what())}, true));
        return;
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle>) final {
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, TIssues{TIssue("Unexpected event")}, true));
    }
private:
    const ui64 InputIndex;
    const TReadSpec::TPtr ReadSpec;
    const TString Format, RowType, Compression;
    const NActors::TActorId SourceActorId;
    const NActors::TActorId ComputeActorId;
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
        const std::shared_ptr<NS3::TRetryConfig>& retryConfig)
        : TActorCoro(THolder<TActorCoroImpl>(impl.Release()))
        , RetryStuff(std::make_shared<TRetryStuff>(std::move(gateway), url + path, headers, retryConfig, expectedSize))
    {}
private:
    static void OnNewData(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, const TRetryStuff::TPtr& retryStuff, IHTTPGateway::TCountedContent&& data) {
        retryStuff->Offset += data.size();
        retryStuff->RetryParams.Reset();
        actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvDataPart(std::move(data))));
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
};

class TS3StreamReadActor : public TActorBootstrapped<TS3StreamReadActor>, public IDqComputeActorAsyncInput {
public:
    TS3StreamReadActor(
        ui64 inputIndex,
        IHTTPGateway::TPtr gateway,
        const TString& url,
        const TString& token,
        TPathList&& paths,
        const TReadSpec::TPtr& readSpec,
        const NActors::TActorId& computeActorId,
        const std::shared_ptr<NS3::TRetryConfig>& retryConfig
    )   : Gateway(std::move(gateway))
        , InputIndex(inputIndex)
        , ComputeActorId(computeActorId)
        , Url(url)
        , Headers(MakeHeader(token))
        , Paths(std::move(paths))
        , ReadSpec(readSpec)
        , RetryConfig(retryConfig)
        , Count(Paths.size())
    {}

    void Bootstrap() {
        Become(&TS3StreamReadActor::StateFunc);
        for (const auto& path : Paths) {
            auto impl = MakeHolder<TS3ReadCoroImpl>(InputIndex, SelfId(), ComputeActorId, ReadSpec);
            RegisterWithSameMailbox(MakeHolder<TS3ReadCoroActor>(std::move(impl), Gateway, Url, Headers, std::get<TString>(path), std::get<std::size_t>(path), RetryConfig).Release());
        }
    }

    static constexpr char ActorName[] = "S3_READ_ACTOR";

private:
    class TBoxedBlock : public NUdf::TBoxedValueBase {
    public:
        TBoxedBlock(NDB::Block& block) {
            Block.swap(block);
        }
    private:
        NUdf::TStringRef GetResourceTag() const final {
            return NUdf::TStringRef::Of("ClickHouseClient.Block");
        }

        void* GetResource() final {
            return &Block;
        }

        NDB::Block Block;
    };

    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {}
    void LoadState(const NDqProto::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}
    ui64 GetInputIndex() const final { return InputIndex; }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueVector& output, bool& finished, i64 free) final {
        i64 total = 0LL;
        if (!Blocks.empty()) do {
            const i64 s = Blocks.front().bytes();
            free -= s;
            total += s;
            output.emplace_back(NUdf::TUnboxedValuePod(new TBoxedBlock(Blocks.front())));
            Blocks.pop_front();
        } while (!Blocks.empty() && free > 0LL && Blocks.front().bytes() <= size_t(free));

        finished = Blocks.empty() && !Count;
        return total;
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        TActorBootstrapped<TS3StreamReadActor>::PassAway();
    }

    static IHTTPGateway::THeaders MakeHeader(const TString& token) {
        return token.empty() ? IHTTPGateway::THeaders() : IHTTPGateway::THeaders{TString("X-YaCloud-SubjectToken:") += token};
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvRetryEventFunc, HandleRetry);
        hFunc(TEvPrivate::TEvNextBlock, HandleNextBlock);
        cFunc(TEvPrivate::EvReadFinished, HandleReadFinished);
    )

    void HandleRetry(TEvPrivate::TEvRetryEventFunc::TPtr& retry) {
        return retry->Get()->Functor();
    }

    void HandleNextBlock(TEvPrivate::TEvNextBlock::TPtr& next) {
        Blocks.emplace_back();
        Blocks.back().swap(next->Get()->Block);
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
    }

    void HandleReadFinished() {
        Y_VERIFY(Count);
        --Count;
    }

    const IHTTPGateway::TPtr Gateway;

    const ui64 InputIndex;
    const NActors::TActorId ComputeActorId;


    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TPathList Paths;
    const TReadSpec::TPtr ReadSpec;
    const std::shared_ptr<NS3::TRetryConfig> RetryConfig;
    std::deque<NDB::Block> Blocks;
    ui32 Count;
};

using namespace NKikimr::NMiniKQL;

NDB::DataTypePtr MetaToClickHouse(const TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::EmptyList:
            return std::make_shared<NDB::DataTypeArray>(std::make_shared<NDB::DataTypeNothing>());
        case TType::EKind::Optional:
            return makeNullable(MetaToClickHouse(static_cast<const TOptionalType*>(type)->GetItemType()));
        case TType::EKind::List:
            return std::make_shared<NDB::DataTypeArray>(MetaToClickHouse(static_cast<const TListType*>(type)->GetItemType()));
        case TType::EKind::Tuple: {
            const auto tupleType = static_cast<const TTupleType*>(type);
            NDB::DataTypes elems;
            elems.reserve(tupleType->GetElementsCount());
            for (auto i = 0U; i < tupleType->GetElementsCount(); ++i)
                elems.emplace_back(MetaToClickHouse(tupleType->GetElementType(i)));
            return std::make_shared<NDB::DataTypeTuple>(elems);
        }
        case TType::EKind::Data: {
            const auto dataType = static_cast<const TDataType*>(type);
            switch (const auto slot = *dataType->GetDataSlot()) {
            case NUdf::EDataSlot::Int8:
                return std::make_shared<NDB::DataTypeInt8>();
            case NUdf::EDataSlot::Bool:
            case NUdf::EDataSlot::Uint8:
                return std::make_shared<NDB::DataTypeUInt8>();
            case NUdf::EDataSlot::Int16:
                return std::make_shared<NDB::DataTypeInt16>();
            case NUdf::EDataSlot::Uint16:
                return std::make_shared<NDB::DataTypeUInt16>();
            case NUdf::EDataSlot::Int32:
                return std::make_shared<NDB::DataTypeInt32>();
            case NUdf::EDataSlot::Uint32:
                return std::make_shared<NDB::DataTypeUInt32>();
            case NUdf::EDataSlot::Int64:
                return std::make_shared<NDB::DataTypeInt64>();
            case NUdf::EDataSlot::Uint64:
                return std::make_shared<NDB::DataTypeUInt64>();
            case NUdf::EDataSlot::Float:
                return std::make_shared<NDB::DataTypeFloat32>();
            case NUdf::EDataSlot::Double:
                return std::make_shared<NDB::DataTypeFloat64>();
            case NUdf::EDataSlot::String:
            case NUdf::EDataSlot::Utf8:
            case NUdf::EDataSlot::Json:
                return std::make_shared<NDB::DataTypeString>();
            case NUdf::EDataSlot::Date:
            case NUdf::EDataSlot::TzDate:
                return std::make_shared<NDB::DataTypeDate>();
            case NUdf::EDataSlot::Datetime:
            case NUdf::EDataSlot::TzDatetime:
                return std::make_shared<NDB::DataTypeDateTime>();
            case NUdf::EDataSlot::Uuid:
                return std::make_shared<NDB::DataTypeUUID>();
            default:
                break;
            }
        }
        default:
            break;
    }
    return nullptr;
}

} // namespace

using namespace NKikimr::NMiniKQL;

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateS3ReadActor(
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
        const auto pb = std::make_unique<TProgramBuilder>(typeEnv, functionRegistry);
        const auto outputItemType = NCommon::ParseTypeFromYson(TStringBuf(params.GetRowType()), *pb,  Cerr);
        const auto structType = static_cast<TStructType*>(outputItemType);

        const auto readSpec = std::make_shared<TReadSpec>();
        readSpec->Columns.resize(structType->GetMembersCount());
        for (ui32 i = 0U; i < structType->GetMembersCount(); ++i) {
            auto& colsumn = readSpec->Columns[i];
            colsumn.type = MetaToClickHouse(structType->GetMemberType(i));
            colsumn.name = structType->GetMemberName(i);
        }
        readSpec->Format = params.GetFormat();

        const auto& settings = params.GetSettings();
        if (const auto it = settings.find("compression"); settings.cend() != it)
            readSpec->Compression = it->second;

#define SUPPORTED_FLAGS(xx) \
        xx(skip_unknown_fields, true) \
        xx(import_nested_json, true) \
        xx(with_names_use_header, true) \
        xx(null_as_default, true) \

#define SET_FLAG(flag, def) \
        if (const auto it = settings.find(#flag); settings.cend() != it) \
            readSpec->Settings.flag = FromString<bool>(it->second); \
        else \
            readSpec->Settings.flag = def;

        SUPPORTED_FLAGS(SET_FLAG)

#undef SET_FLAG
#undef SUPPORTED_FLAGS

        const auto actor = new TS3StreamReadActor(inputIndex, std::move(gateway), params.GetUrl(), authToken, std::move(paths), readSpec, computeActorId, retryConfig);
        return {actor, actor};
    } else {
        const auto actor = new TS3ReadActor(inputIndex, std::move(gateway), params.GetUrl(), authToken, std::move(paths), computeActorId, retryConfig);
        return {actor, actor};
    }
}

} // namespace NYql::NDq
