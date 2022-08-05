#ifdef __linux__
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeArray.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeDate.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeEnum.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeFactory.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeInterval.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeNothing.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeNullable.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeString.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeTuple.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeUUID.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypesNumber.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/Block.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/ColumnsWithTypeAndName.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatFactory.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Processors/Formats/InputStreamFromInputFormat.h>
#endif

#include "yql_s3_read_actor.h"
#include "yql_s3_retry_policy.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/providers/s3/compressors/factory.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/providers/s3/serializations/serialization_interval.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actor_coroutine.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/generic/size_literals.h>

#include <queue>

namespace NYql::NDq {

using namespace ::NActors;
using namespace ::NYql::NS3Details;

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
        TEvReadResult(IHTTPGateway::TContent&& result, size_t pathInd): Result(std::move(result)), PathIndex(pathInd) {}
        IHTTPGateway::TContent Result;
        const size_t PathIndex;
    };

    struct TEvDataPart : public TEventLocal<TEvDataPart, EvReadResult> {
        TEvDataPart(IHTTPGateway::TCountedContent&& data) : Result(std::move(data)) {}
        IHTTPGateway::TCountedContent Result;
    };

    struct TEvReadFinished : public TEventLocal<TEvReadFinished, EvReadFinished> {
        explicit TEvReadFinished(long httpResponseCode) : HttpResponseCode(httpResponseCode) {}
        const long HttpResponseCode;
    };

    struct TEvReadError : public TEventLocal<TEvReadError, EvReadError> {
        TEvReadError(TIssues&& error, size_t pathInd = std::numeric_limits<size_t>::max()) : Error(std::move(error)), PathIndex(pathInd) {}
        const TIssues Error;
        const size_t PathIndex;
    };

    struct TEvRetryEventFunc : public NActors::TEventLocal<TEvRetryEventFunc, EvRetry> {
        explicit TEvRetryEventFunc(std::function<void()> functor) : Functor(std::move(functor)) {}
        const std::function<void()> Functor;
    };

    struct TEvNextBlock : public NActors::TEventLocal<TEvNextBlock, EvNextBlock> {
        TEvNextBlock(NDB::Block& block, size_t pathInd) : PathIndex(pathInd) { Block.swap(block); }
        NDB::Block Block;
        const size_t PathIndex;
    };
};

using namespace NKikimr::NMiniKQL;

class TS3ReadActor : public TActorBootstrapped<TS3ReadActor>, public IDqComputeActorAsyncInput {
public:
    TS3ReadActor(ui64 inputIndex,
        IHTTPGateway::TPtr gateway,
        const THolderFactory& holderFactory,
        const TString& url,
        const TString& token,
        TPathList&& paths,
        bool addPathIndex,
        ui64 startPathIndex,
        const NActors::TActorId& computeActorId
    )   : Gateway(std::move(gateway))
        , HolderFactory(holderFactory)
        , InputIndex(inputIndex)
        , ComputeActorId(computeActorId)
        , ActorSystem(TActivationContext::ActorSystem())
        , Url(url)
        , Headers(MakeHeader(token))
        , Paths(std::move(paths))
        , AddPathIndex(addPathIndex)
        , StartPathIndex(startPathIndex)
    {}

    void Bootstrap() {
        Become(&TS3ReadActor::StateFunc);
        for (size_t pathInd = 0; pathInd < Paths.size(); ++pathInd) {
            const TPath& path = Paths[pathInd];
            Gateway->Download(Url + std::get<TString>(path),
                Headers, std::get<size_t>(path),
                std::bind(&TS3ReadActor::OnDownloadFinished, ActorSystem, SelfId(), std::placeholders::_1, pathInd + StartPathIndex), {}, GetS3RetryPolicy());
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

    i64 GetAsyncInputData(TUnboxedValueVector& buffer, bool& finished, i64 freeSpace) final {
        i64 total = 0LL;
        if (!Blocks.empty()) {
            buffer.reserve(buffer.size() + Blocks.size());
            do {
                auto& content = std::get<IHTTPGateway::TContent>(Blocks.front());
                const auto size = content.size();
                auto value = MakeString(std::string_view(content));
                if (AddPathIndex) {
                    NUdf::TUnboxedValue* tupleItems = nullptr;
                    auto tuple = ContainerCache.NewArray(HolderFactory, 2, tupleItems);
                    *tupleItems++ = value;
                    *tupleItems++ = NUdf::TUnboxedValuePod(std::get<ui64>(Blocks.front()));
                    value = tuple;
                }

                buffer.emplace_back(std::move(value));
                Blocks.pop();
                total += size;
                freeSpace -= size;
            } while (!Blocks.empty() && freeSpace > 0LL);
        }

        if (Blocks.empty() && IsDoneCounter == Paths.size()) {
            finished = true;
            ContainerCache.Clear();
        }

        return total;
    }


    void Handle(TEvPrivate::TEvReadResult::TPtr& result) {
        ++IsDoneCounter;
        Blocks.emplace(std::make_tuple(std::move(result->Get()->Result), result->Get()->PathIndex));
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }

    void Handle(TEvPrivate::TEvReadError::TPtr& result) {
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
    const THolderFactory& HolderFactory;
    TPlainContainerCache ContainerCache;

    const ui64 InputIndex;
    const NActors::TActorId ComputeActorId;

    TActorSystem* const ActorSystem;

    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TPathList Paths;
    const bool AddPathIndex;
    const ui64 StartPathIndex;

    std::queue<std::tuple<IHTTPGateway::TContent, ui64>> Blocks;
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
    TS3ReadCoroImpl(ui64 inputIndex, const NActors::TActorId& sourceActorId, const NActors::TActorId& computeActorId, const TReadSpec::TPtr& readSpec, size_t pathIndex, const TString& path)
        : TActorCoroImpl(256_KB), InputIndex(inputIndex), ReadSpec(readSpec), SourceActorId(sourceActorId), ComputeActorId(computeActorId), PathIndex(pathIndex), Path(path)
    {}

    bool Next(TString& value) {
        if (HttpResponseCode)
            return false;

        const auto ev = WaitForSpecificEvent<TEvPrivate::TEvDataPart, TEvPrivate::TEvReadError, TEvPrivate::TEvReadFinished>();
        switch (const auto etype = ev->GetTypeRewrite()) {
            case TEvPrivate::TEvReadFinished::EventType:
                HttpResponseCode = ev->Get<TEvPrivate::TEvReadFinished>()->HttpResponseCode;
                return false;
            case TEvPrivate::TEvReadError::EventType:
                HttpResponseCode = 0L;
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
    void WaitFinish() {
        if (HttpResponseCode)
            return;

        const auto ev = WaitForSpecificEvent<TEvPrivate::TEvReadError, TEvPrivate::TEvReadFinished>();
        switch (const auto etype = ev->GetTypeRewrite()) {
            case TEvPrivate::TEvReadFinished::EventType:
                HttpResponseCode = ev->Get<TEvPrivate::TEvReadFinished>()->HttpResponseCode;
                break;
            case TEvPrivate::TEvReadError::EventType:
                HttpResponseCode = 0L;
                Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, ev->Get<TEvPrivate::TEvReadError>()->Error, true));
                break;
            default:
                break;
        }
    }

    void Run() final try {
        TReadBufferFromStream buffer(this);
        const auto decompress(MakeDecompressor(buffer, ReadSpec->Compression));
        YQL_ENSURE(ReadSpec->Compression.empty() == !decompress, "Unsupported " << ReadSpec->Compression << " compression.");
        NDB::InputStreamFromInputFormat stream(NDB::FormatFactory::instance().getInputFormat(ReadSpec->Format, decompress ? *decompress : buffer, NDB::Block(ReadSpec->Columns), nullptr, 1_MB, ReadSpec->Settings));

        while (auto block = stream.read())
            Send(SourceActorId, new TEvPrivate::TEvNextBlock(block, PathIndex));

        WaitFinish();

        if (*HttpResponseCode)
            Send(SourceActorId, new TEvPrivate::TEvReadFinished(*HttpResponseCode));
    } catch (const TDtorException&) {
       return;
    } catch (const std::exception& err) {
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, TIssues{TIssue(TStringBuilder() << "Error while reading file " << Path << ", details: " << err.what())}, true));
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
    const size_t PathIndex;
    const TString Path;
    std::optional<long> HttpResponseCode;
};

class TS3ReadCoroActor : public TActorCoro {
    struct TRetryStuff {
        using TPtr = std::shared_ptr<TRetryStuff>;

        TRetryStuff(
            IHTTPGateway::TPtr gateway,
            TString url,
            const IHTTPGateway::THeaders& headers,
            std::size_t expectedSize
        ) : Gateway(std::move(gateway)), Url(std::move(url)), Headers(headers), ExpectedSize(expectedSize), Offset(0U), RetryState(GetS3RetryPolicy()->CreateRetryState())
        {}

        const IHTTPGateway::TPtr Gateway;
        const TString Url;
        const IHTTPGateway::THeaders Headers;
        const std::size_t ExpectedSize;

        std::size_t Offset = 0U;
        const IRetryPolicy<long>::IRetryState::TPtr RetryState;
    };
public:
    TS3ReadCoroActor(THolder<TS3ReadCoroImpl> impl,
        IHTTPGateway::TPtr gateway,
        const TString& url,
        const IHTTPGateway::THeaders& headers,
        const TString& path,
        const std::size_t expectedSize)
        : TActorCoro(THolder<TActorCoroImpl>(impl.Release()))
        , RetryStuff(std::make_shared<TRetryStuff>(std::move(gateway), url + path, headers, expectedSize))
    {}
private:
    static void OnNewData(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, const TRetryStuff::TPtr& retryStuff, IHTTPGateway::TCountedContent&& data) {
        retryStuff->Offset += data.size();
        actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvDataPart(std::move(data))));
    }

    static void OnDownloadFinished(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, const TRetryStuff::TPtr& retryStuff, std::variant<long, TIssues> result) {
        switch (result.index()) {
            case 0U:
                if (const auto httpCode = std::get<long>(result); const auto nextDelayMs = retryStuff->RetryState->GetNextRetryDelay(httpCode))
                    actorSystem->Schedule(*nextDelayMs, new IEventHandle(parent, self, new TEvPrivate::TEvRetryEventFunc(std::bind(&TS3ReadCoroActor::DownloadStart, retryStuff, self, parent))));
                else
                    actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvReadFinished(httpCode)));
                break;
            case 1U:
                if (const auto nextDelayMs = retryStuff->RetryState->GetNextRetryDelay(0L))
                    actorSystem->Schedule(*nextDelayMs, new IEventHandle(parent, self, new TEvPrivate::TEvRetryEventFunc(std::bind(&TS3ReadCoroActor::DownloadStart, retryStuff, self, parent))));
                else
                    actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvReadError(std::get<TIssues>(std::move(result)))));
                break;
        }
    }

    static void DownloadStart(const TRetryStuff::TPtr& retryStuff, const TActorId& self, const TActorId& parent) {
        retryStuff->Gateway->Download(retryStuff->Url,
            retryStuff->Headers, retryStuff->Offset,
            std::bind(&TS3ReadCoroActor::OnNewData, TActivationContext::ActorSystem(), self, parent, retryStuff, std::placeholders::_1),
            std::bind(&TS3ReadCoroActor::OnDownloadFinished, TActivationContext::ActorSystem(), self, parent, retryStuff, std::placeholders::_1));
    }

    void Registered(TActorSystem* sys, const TActorId& parent) override {
        TActorCoro::Registered(sys, parent); // Calls TActorCoro::OnRegister and sends bootstrap event to ourself.
        DownloadStart(RetryStuff, SelfId(), parent);
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
        const THolderFactory& holderFactory,
        const TString& url,
        const TString& token,
        TPathList&& paths,
        bool addPathIndex,
        ui64 startPathIndex,
        const TReadSpec::TPtr& readSpec,
        const NActors::TActorId& computeActorId
    )   : Gateway(std::move(gateway))
        , HolderFactory(holderFactory)
        , InputIndex(inputIndex)
        , ComputeActorId(computeActorId)
        , Url(url)
        , Headers(MakeHeader(token))
        , Paths(std::move(paths))
        , AddPathIndex(addPathIndex)
        , StartPathIndex(startPathIndex)
        , ReadSpec(readSpec)
        , Count(Paths.size())
    {}

    void Bootstrap() {
        Become(&TS3StreamReadActor::StateFunc);
        for (size_t pathInd = 0; pathInd < Paths.size(); ++pathInd) {
            const TPath& path = Paths[pathInd];
            auto impl = MakeHolder<TS3ReadCoroImpl>(InputIndex, SelfId(), ComputeActorId, ReadSpec, pathInd + StartPathIndex, std::get<TString>(path));
            RegisterWithSameMailbox(MakeHolder<TS3ReadCoroActor>(std::move(impl), Gateway, Url, Headers, std::get<TString>(path), std::get<std::size_t>(path)).Release());
        }
    }

    static constexpr char ActorName[] = "S3_READ_ACTOR";

private:
    class TBoxedBlock : public TComputationValue<TBoxedBlock> {
    public:
        TBoxedBlock(TMemoryUsageInfo* memInfo, NDB::Block& block)
            : TComputationValue(memInfo)
        {
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

    i64 GetAsyncInputData(TUnboxedValueVector& output, bool& finished, i64 free) final {
        i64 total = 0LL;
        if (!Blocks.empty()) do {
            auto& block = std::get<NDB::Block>(Blocks.front());
            const i64 s = block.bytes();

            auto value = HolderFactory.Create<TBoxedBlock>(block);
            if (AddPathIndex) {
                NUdf::TUnboxedValue* tupleItems = nullptr;
                auto tuple = ContainerCache.NewArray(HolderFactory, 2, tupleItems);
                *tupleItems++ = value;
                *tupleItems++ = NUdf::TUnboxedValuePod(std::get<ui64>(Blocks.front()));
                value = tuple;
            }

            free -= s;
            total += s;
            output.emplace_back(std::move(value));
            Blocks.pop_front();
        } while (!Blocks.empty() && free > 0LL && std::get<NDB::Block>(Blocks.front()).bytes() <= size_t(free));

        finished = Blocks.empty() && !Count;
        if (finished) {
            ContainerCache.Clear();
        }
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
        auto& block = std::get<NDB::Block>(Blocks.back());
        auto& pathInd = std::get<size_t>(Blocks.back());
        block.swap(next->Get()->Block);
        pathInd = next->Get()->PathIndex;
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
    }

    void HandleReadFinished() {
        Y_VERIFY(Count);
        --Count;
    }

    const IHTTPGateway::TPtr Gateway;
    const THolderFactory& HolderFactory;
    TPlainContainerCache ContainerCache;

    const ui64 InputIndex;
    const NActors::TActorId ComputeActorId;


    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TPathList Paths;
    const bool AddPathIndex;
    const ui64 StartPathIndex;
    const TReadSpec::TPtr ReadSpec;
    std::deque<std::tuple<NDB::Block, size_t>> Blocks;
    ui32 Count;
};

using namespace NKikimr::NMiniKQL;

NDB::DataTypePtr MetaToClickHouse(const TType* type, NSerialization::TSerializationInterval::EUnit unit) {
    switch (type->GetKind()) {
        case TType::EKind::EmptyList:
            return std::make_shared<NDB::DataTypeArray>(std::make_shared<NDB::DataTypeNothing>());
        case TType::EKind::Optional:
            return makeNullable(MetaToClickHouse(static_cast<const TOptionalType*>(type)->GetItemType(), unit));
        case TType::EKind::List:
            return std::make_shared<NDB::DataTypeArray>(MetaToClickHouse(static_cast<const TListType*>(type)->GetItemType(), unit));
        case TType::EKind::Tuple: {
            const auto tupleType = static_cast<const TTupleType*>(type);
            NDB::DataTypes elems;
            elems.reserve(tupleType->GetElementsCount());
            for (auto i = 0U; i < tupleType->GetElementsCount(); ++i)
                elems.emplace_back(MetaToClickHouse(tupleType->GetElementType(i), unit));
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
            case NUdf::EDataSlot::Interval:
                return NSerialization::GetInterval(unit);
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
    const THolderFactory& holderFactory,
    IHTTPGateway::TPtr gateway,
    NS3::TSource&& params,
    ui64 inputIndex,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const NActors::TActorId& computeActorId,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
{
    const IFunctionRegistry& functionRegistry = *holderFactory.GetFunctionRegistry();

    TPathList paths;
    ui64 startPathIndex = 0;
    ReadPathsList(params, taskParams, paths, startPathIndex);

    const auto token = secureParams.Value(params.GetToken(), TString{});
    const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
    const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();

    const auto& settings = params.GetSettings();
    bool addPathIndex = false;
    if (auto it = settings.find("addPathIndex"); it != settings.cend()) {
        addPathIndex = FromString<bool>(it->second);
    }

    NYql::NSerialization::TSerializationInterval::EUnit intervalUnit = NYql::NSerialization::TSerializationInterval::EUnit::MICROSECONDS;
    if (auto it = settings.find("data.interval.unit"); it != settings.cend()) {
        intervalUnit = NYql::NSerialization::TSerializationInterval::ToUnit(it->second);
    }

    if (params.HasFormat() && params.HasRowType()) {
        const auto pb = std::make_unique<TProgramBuilder>(typeEnv, functionRegistry);
        const auto outputItemType = NCommon::ParseTypeFromYson(TStringBuf(params.GetRowType()), *pb,  Cerr);
        const auto structType = static_cast<TStructType*>(outputItemType);

        const auto readSpec = std::make_shared<TReadSpec>();
        readSpec->Columns.resize(structType->GetMembersCount());
        for (ui32 i = 0U; i < structType->GetMembersCount(); ++i) {
            auto& colsumn = readSpec->Columns[i];
            colsumn.type = MetaToClickHouse(structType->GetMemberType(i), intervalUnit);
            colsumn.name = structType->GetMemberName(i);
        }
        readSpec->Format = params.GetFormat();

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

        const auto actor = new TS3StreamReadActor(inputIndex, std::move(gateway), holderFactory, params.GetUrl(), authToken,
                                                  std::move(paths), addPathIndex, startPathIndex, readSpec, computeActorId);
        return {actor, actor};
    } else {
        const auto actor = new TS3ReadActor(inputIndex, std::move(gateway), holderFactory, params.GetUrl(), authToken,
                                            std::move(paths), addPathIndex, startPathIndex, computeActorId);
        return {actor, actor};
    }
}

} // namespace NYql::NDq
