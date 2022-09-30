#include <util/system/platform.h>
#if defined(_linux_) || defined(_darwin_)
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeArray.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeDate.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeDateTime64.h>
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
#include "yql_s3_actors_util.h"

#include <ydb/core/protos/services.pb.h>

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
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
#include <library/cpp/actors/core/log.h>

#include <util/generic/size_literals.h>
#include <util/stream/format.h>

#include <queue>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>


#define LOG_E(name, stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_W(name, stream) \
    LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_I(name, stream) \
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_D(name, stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_T(name, stream) \
    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)

#define LOG_CORO_E(name, stream) \
    LOG_ERROR_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, name << ": " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId << ". " << stream)
#define LOG_CORO_W(name, stream) \
    LOG_WARN_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, name << ": " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId << ". " << stream)
#define LOG_CORO_I(name, stream) \
    LOG_INFO_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, name << ": " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId << ". " << stream)
#define LOG_CORO_D(name, stream) \
    LOG_DEBUG_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, name << ": " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId << ". " << stream)
#define LOG_CORO_T(name, stream) \
    LOG_TRACE_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, name << ": " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId << ". " << stream)

namespace NYql::NDq {

using namespace ::NActors;
using namespace ::NYql::NS3Details;

namespace {

struct TS3ReadError : public yexception {
    using yexception::yexception;
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

        EvReadResult = EvBegin,
        EvDataPart,
        EvReadStarted,
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

    struct TEvDataPart : public TEventLocal<TEvDataPart, EvDataPart> {
        TEvDataPart(IHTTPGateway::TCountedContent&& data) : Result(std::move(data)) {}
        IHTTPGateway::TCountedContent Result;
    };

    struct TEvReadStarted : public TEventLocal<TEvReadStarted, EvReadStarted> {
        explicit TEvReadStarted(long httpResponseCode) : HttpResponseCode(httpResponseCode) {}
        const long HttpResponseCode;
    };

    struct TEvReadFinished : public TEventLocal<TEvReadFinished, EvReadFinished> {
        TEvReadFinished() = default;

        TEvReadFinished(TIssues&& issues)
            : Issues(std::move(issues))
        {
        }

        TIssues Issues;
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
        const TTxId& txId,
        IHTTPGateway::TPtr gateway,
        const THolderFactory& holderFactory,
        const TString& url,
        const TString& token,
        TPathList&& paths,
        bool addPathIndex,
        ui64 startPathIndex,
        const NActors::TActorId& computeActorId,
        ui64 sizeLimit,
        const IRetryPolicy<long>::TPtr& retryPolicy
    )   : Gateway(std::move(gateway))
        , HolderFactory(holderFactory)
        , InputIndex(inputIndex)
        , TxId(txId)
        , ComputeActorId(computeActorId)
        , RetryPolicy(retryPolicy)
        , ActorSystem(TActivationContext::ActorSystem())
        , Url(url)
        , Headers(MakeHeader(token))
        , Paths(std::move(paths))
        , AddPathIndex(addPathIndex)
        , StartPathIndex(startPathIndex)
        , SizeLimit(sizeLimit)
    {}

    void Bootstrap() {
        LOG_D("TS3ReadActor", "Bootstrap" << ", InputIndex: " << InputIndex);
        Become(&TS3ReadActor::StateFunc);
        for (size_t pathInd = 0; pathInd < Paths.size(); ++pathInd) {
            const TPath& path = Paths[pathInd];
            auto url = Url + std::get<TString>(path);
            auto id = pathInd + StartPathIndex;
            LOG_D("TS3ReadActor", "Download: " << url << ", ID: " << id);
            Gateway->Download(url, Headers, std::min(std::get<size_t>(path), SizeLimit),
                std::bind(&TS3ReadActor::OnDownloadFinished, ActorSystem, SelfId(), std::placeholders::_1, id), {}, RetryPolicy);
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

    i64 GetAsyncInputData(TUnboxedValueVector& buffer, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
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
        const auto id = result->Get()->PathIndex;
        const auto httpCode = result->Get()->Result.HttpResponseCode;
        LOG_D("TS3ReadActor", "ID: " << id << ", TEvReadResult size: " << result->Get()->Result.size() << ", HTTP response code: " << httpCode);
        if (200 == httpCode || 206 == httpCode) {
            Blocks.emplace(std::make_tuple(std::move(result->Get()->Result), id));
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
        } else {
            TString errorText = result->Get()->Result.Extract();
            TString errorCode;
            TString message;
            if (!ParseS3ErrorResponse(errorText, errorCode, message)) {
                message = errorText;
            }
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, BuildIssues(httpCode, errorCode, message), NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
        }
    }

    void Handle(TEvPrivate::TEvReadError::TPtr& result) {
        ++IsDoneCounter;
        auto id = result->Get()->PathIndex;
        LOG_W("TS3ReadActor", "ID: " << id << ", TEvReadError: " << result->Get()->Error.ToOneLineString());
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, result->Get()->Error, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        LOG_D("TS3ReadActor", "PassAway");
        ContainerCache.Clear();
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
    const TTxId TxId;
    const NActors::TActorId ComputeActorId;
    const IRetryPolicy<long>::TPtr RetryPolicy;

    TActorSystem* const ActorSystem;

    const TString Url;
    const IHTTPGateway::THeaders Headers;
    const TPathList Paths;
    const bool AddPathIndex;
    const ui64 StartPathIndex;
    const ui64 SizeLimit;

    std::queue<std::tuple<IHTTPGateway::TContent, ui64>> Blocks;
};

struct TReadSpec {
    using TPtr = std::shared_ptr<TReadSpec>;

    NDB::ColumnsWithTypeAndName Columns;
    NDB::FormatSettings Settings;
    TString Format, Compression;
    ui64 SizeLimit = 0;
};

struct TRetryStuff {
    using TPtr = std::shared_ptr<TRetryStuff>;

    TRetryStuff(
        IHTTPGateway::TPtr gateway,
        TString url,
        const IHTTPGateway::THeaders& headers,
        std::size_t sizeLimit,
        const TTxId& txId,
        const IRetryPolicy<long>::TPtr& retryPolicy
    ) : Gateway(std::move(gateway)), Url(std::move(url)), Headers(headers), Offset(0U), SizeLimit(sizeLimit), TxId(txId), RetryState(retryPolicy->CreateRetryState()), Cancelled(false)
    {}

    const IHTTPGateway::TPtr Gateway;
    const TString Url;
    const IHTTPGateway::THeaders Headers;
    std::size_t Offset, SizeLimit;
    const TTxId TxId;
    const IRetryPolicy<long>::IRetryState::TPtr RetryState;
    IHTTPGateway::TCancelHook CancelHook;
    TMaybe<TDuration> NextRetryDelay;
    std::atomic_bool Cancelled;

    void Cancel() {
        Cancelled.store(true);
        if (const auto cancelHook = std::move(CancelHook)) {
            CancelHook = {};
            cancelHook(TIssue("Request cancelled."));
        }
    }

    bool IsCancelled() {
        return Cancelled.load();
    }
};

void OnDownloadStart(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, long httpResponseCode) {
    actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvReadStarted(httpResponseCode)));
}

void OnNewData(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, IHTTPGateway::TCountedContent&& data) {
    actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvDataPart(std::move(data))));
}

void OnDownloadFinished(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, TIssues issues) {
    actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvReadFinished(std::move(issues))));
}

void DownloadStart(const TRetryStuff::TPtr& retryStuff, TActorSystem* actorSystem, const TActorId& self, const TActorId& parent) {
    retryStuff->CancelHook = retryStuff->Gateway->Download(retryStuff->Url,
        retryStuff->Headers, retryStuff->Offset, retryStuff->SizeLimit,
        std::bind(&OnDownloadStart, actorSystem, self, parent, std::placeholders::_1),
        std::bind(&OnNewData, actorSystem, self, parent, std::placeholders::_1),
        std::bind(&OnDownloadFinished, actorSystem, self, parent, std::placeholders::_1));
}

class TS3ReadCoroImpl : public TActorCoroImpl {
private:
    class TReadBufferFromStream : public NDB::ReadBuffer {
    public:
        TReadBufferFromStream(TS3ReadCoroImpl* coro)
            : NDB::ReadBuffer(nullptr, 0ULL), Coro(coro)
        {}
    private:
        bool nextImpl() final {
            while (Coro->Next(Value)) {
                if (!Value.empty()) {
                    working_buffer = NDB::BufferBase::Buffer(const_cast<char*>(Value.data()), const_cast<char*>(Value.data()) + Value.size());
                    return true;
                }
            }
            return false;
        }

        TS3ReadCoroImpl *const Coro;
        TString Value;
    };

    static constexpr std::string_view TruncatedSuffix = "... [truncated]"sv;
public:
    TS3ReadCoroImpl(ui64 inputIndex, const TTxId& txId, const NActors::TActorId& computeActorId, const TRetryStuff::TPtr& retryStuff, const TReadSpec::TPtr& readSpec, size_t pathIndex, const TString& path)
        : TActorCoroImpl(256_KB), InputIndex(inputIndex), TxId(txId), RetryStuff(retryStuff), ReadSpec(readSpec), ComputeActorId(computeActorId), PathIndex(pathIndex), Path(path)
    {}

    bool Next(TString& value) {
        if (InputFinished)
            return false;

        const auto ev = WaitForSpecificEvent<TEvPrivate::TEvReadStarted, TEvPrivate::TEvDataPart, TEvPrivate::TEvReadFinished>();
        switch (const auto etype = ev->GetTypeRewrite()) {
            case TEvPrivate::TEvReadStarted::EventType:
                ErrorText.clear();
                Issues.Clear();
                value.clear();
                RetryStuff->NextRetryDelay = RetryStuff->RetryState->GetNextRetryDelay(HttpResponseCode = ev->Get<TEvPrivate::TEvReadStarted>()->HttpResponseCode);
                LOG_CORO_D("TS3ReadCoroImpl", "TEvReadStarted, Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", Http code: " << HttpResponseCode << ", retry after: " << RetryStuff->NextRetryDelay);
                return true;
            case TEvPrivate::TEvReadFinished::EventType:
                Issues = std::move(ev->Get<TEvPrivate::TEvReadFinished>()->Issues);
                if (Issues) {
                    if (RetryStuff->NextRetryDelay = RetryStuff->RetryState->GetNextRetryDelay(0L); !RetryStuff->NextRetryDelay) {
                        InputFinished = true;
                        LOG_CORO_W("TS3ReadCoroImpl", "ReadError: " << Issues.ToOneLineString() << ", Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", LastOffset: " << LastOffset << ", LastData: " << GetLastDataAsText());
                        throw TS3ReadError(); // Don't pass control to data parsing, because it may validate eof and show wrong issues about incorrect data format
                    }
                }

                if (!RetryStuff->IsCancelled() && RetryStuff->NextRetryDelay && RetryStuff->SizeLimit > 0ULL) {
                    LOG_CORO_D("TS3ReadCoroImpl", "TS3ReadCoroActor" << ": " << SelfActorId << ", TxId: " << RetryStuff->TxId << ". Retry Download, Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset);
                    GetActorSystem()->Schedule(*RetryStuff->NextRetryDelay, new IEventHandle(ParentActorId, SelfActorId, new TEvPrivate::TEvRetryEventFunc(std::bind(&DownloadStart, RetryStuff, GetActorSystem(), SelfActorId, ParentActorId))));
                } else {
                    LOG_CORO_D("TS3ReadCoroImpl", "TEvReadFinished, Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", LastOffset: " << LastOffset << ", Error: " << ServerReturnedError << ", LastData: " << GetLastDataAsText());
                    InputFinished = true;
                    if (ServerReturnedError) {
                        throw TS3ReadError(); // Don't pass control to data parsing, because it may validate eof and show wrong issues about incorrect data format
                    }
                    return false; // end of data (real data, not an error)
                }
                return true;
            case TEvPrivate::TEvDataPart::EventType:
                if (200L == HttpResponseCode || 206L == HttpResponseCode) {
                    value = ev->Get<TEvPrivate::TEvDataPart>()->Result.Extract();
                    RetryStuff->Offset += value.size();
                    RetryStuff->SizeLimit -= value.size();
                    LastOffset = RetryStuff->Offset;
                    LastData = value;
                    LOG_CORO_T("TS3ReadCoroImpl", "TEvDataPart, size: " << value.size() << ", Url: " << RetryStuff->Url << ", Offset (updated): " << RetryStuff->Offset);
                    Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
                } else if (HttpResponseCode && !RetryStuff->IsCancelled() && !RetryStuff->NextRetryDelay) {
                    ServerReturnedError = true;
                    if (ErrorText.size() < 256_KB)
                        ErrorText.append(ev->Get<TEvPrivate::TEvDataPart>()->Result.Extract());
                    else if (!ErrorText.EndsWith(TruncatedSuffix))
                        ErrorText.append(TruncatedSuffix);
                    value.clear();
                    LOG_CORO_W("TS3ReadCoroImpl", "TEvDataPart, ERROR: " << ErrorText << ", Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", LastOffset: " << LastOffset << ", LastData: " << GetLastDataAsText());
                }
                return true;
            default:
                return false;
        }
    }
private:
    void WaitFinish() {
        LOG_CORO_D("TS3ReadCoroImpl", "WaitFinish: " << Path);
        if (InputFinished)
            return;

        while (true) {
            const auto ev = WaitForSpecificEvent<TEvPrivate::TEvReadStarted, TEvPrivate::TEvDataPart, TEvPrivate::TEvReadFinished>();
            const auto etype = ev->GetTypeRewrite();
            switch (etype) {
                case TEvPrivate::TEvReadFinished::EventType:
                    Issues = std::move(ev->Get<TEvPrivate::TEvReadFinished>()->Issues);
                    LOG_CORO_D("TS3ReadCoroImpl", "TEvReadFinished: " << Issues.ToOneLineString());
                    break;
                default:
                    continue;
            }
            InputFinished = true;
            return;
        }
    }

    void Run() final try {

        LOG_CORO_D("TS3ReadCoroImpl", "Run" << ", Path: " << Path);

        NYql::NDqProto::StatusIds::StatusCode fatalCode = NYql::NDqProto::StatusIds::EXTERNAL_ERROR;

        TIssue exceptIssue;
        try {
            TReadBufferFromStream buffer(this);
            const auto decompress(MakeDecompressor(buffer, ReadSpec->Compression));
            YQL_ENSURE(ReadSpec->Compression.empty() == !decompress, "Unsupported " << ReadSpec->Compression << " compression.");
            NDB::InputStreamFromInputFormat stream(NDB::FormatFactory::instance().getInputFormat(ReadSpec->Format, decompress ? *decompress : buffer, NDB::Block(ReadSpec->Columns), nullptr, 1_MB, ReadSpec->Settings));

            while (auto block = stream.read()) {
                Send(ParentActorId, new TEvPrivate::TEvNextBlock(block, PathIndex));
            }
        } catch (const TS3ReadError&) {
            // Finish reading. Add error from server to issues
        } catch (const std::exception& err) {
            exceptIssue.Message = TStringBuilder() << "Error while reading file " << Path << ", details: " << err.what();
            fatalCode = NYql::NDqProto::StatusIds::INTERNAL_ERROR;
            RetryStuff->Cancel();
        }

        WaitFinish();

        if (!ErrorText.empty()) {
            TString errorCode;
            TString message;
            if (!ParseS3ErrorResponse(ErrorText, errorCode, message)) {
                message = ErrorText;
            }
            Issues.AddIssues(BuildIssues(HttpResponseCode, errorCode, message));
        }

        if (exceptIssue.Message) {
            Issues.AddIssue(exceptIssue);
        }

        if (Issues)
            Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, std::move(Issues), fatalCode));
        else
            Send(ParentActorId, new TEvPrivate::TEvReadFinished);
    } catch (const TDtorException&) {
        return RetryStuff->Cancel();
    } catch (const std::exception& err) {
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, TIssues{TIssue(TStringBuilder() << "Error while reading file " << Path << ", details: " << err.what())}, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
        return;
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) final {
        TStringBuilder message;
        message << "S3 read. Unexpected message type " << Hex(ev->GetTypeRewrite());
        if (auto* eventBase = ev->GetBase()) {
            message << " (" << eventBase->ToStringHeader() << ")";
        }
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, TIssues{TIssue(message)}));
    }

    TString GetLastDataAsText() {

        if (LastData.empty()) {
            return "[]";
        }

        auto begin = const_cast<char*>(LastData.data());
        auto end = begin + LastData.size();

        TStringBuilder result;

        result << "[";

        if (LastData.size() > 32) {
            begin += LastData.size() - 32;
            result << "...";
        }

        while (begin < end) {
            char c = *begin++;
            if (c >= 32 && c <= 126) {
                result << c;
            } else {
                result << "\\" << Hex(static_cast<ui8>(c));
            }
        }

        result << "]";

        return result;
    }
private:
    const ui64 InputIndex;
    const TTxId TxId;
    const TRetryStuff::TPtr RetryStuff;
    const TReadSpec::TPtr ReadSpec;
    const TString Format, RowType, Compression;
    const NActors::TActorId ComputeActorId;
    const size_t PathIndex;
    const TString Path;

    bool InputFinished = false;
    long HttpResponseCode = 0L;
    bool ServerReturnedError = false;
    TString ErrorText;
    TIssues Issues;

    std::size_t LastOffset = 0;
    TString LastData;
};

class TS3ReadCoroActor : public TActorCoro {
public:
    TS3ReadCoroActor(THolder<TS3ReadCoroImpl> impl, TRetryStuff::TPtr retryStuff)
        : TActorCoro(THolder<TActorCoroImpl>(impl.Release()))
        , RetryStuff(std::move(retryStuff))
    {}
private:
    void Registered(TActorSystem* actorSystem, const TActorId& parent) override {
        TActorCoro::Registered(actorSystem, parent); // Calls TActorCoro::OnRegister and sends bootstrap event to ourself.
        LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_COMPUTE, "TS3ReadCoroActor" << ": " << SelfId() << ", TxId: " << RetryStuff->TxId << ". " << "Start Download, Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset);
        DownloadStart(RetryStuff, actorSystem, SelfId(), parent);
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
        const TTxId& txId,
        IHTTPGateway::TPtr gateway,
        const THolderFactory& holderFactory,
        const TString& url,
        const TString& token,
        TPathList&& paths,
        bool addPathIndex,
        ui64 startPathIndex,
        const TReadSpec::TPtr& readSpec,
        const NActors::TActorId& computeActorId,
        const IRetryPolicy<long>::TPtr& retryPolicy
    )   : Gateway(std::move(gateway))
        , HolderFactory(holderFactory)
        , InputIndex(inputIndex)
        , TxId(txId)
        , ComputeActorId(computeActorId)
        , RetryPolicy(retryPolicy)
        , Url(url)
        , Headers(MakeHeader(token))
        , Paths(std::move(paths))
        , AddPathIndex(addPathIndex)
        , StartPathIndex(startPathIndex)
        , ReadSpec(readSpec)
        , Count(Paths.size())
    {}

    void Bootstrap() {
        LOG_D("TS3StreamReadActor", "Bootstrap");
        Become(&TS3StreamReadActor::StateFunc);
        for (size_t pathInd = 0; pathInd < Paths.size(); ++pathInd) {
            const TPath& path = Paths[pathInd];
            auto stuff = std::make_shared<TRetryStuff>(Gateway, Url + std::get<TString>(path), Headers, std::get<std::size_t>(path), TxId, RetryPolicy);
            RetryStuffForFile.push_back(stuff);
            auto impl = MakeHolder<TS3ReadCoroImpl>(InputIndex, TxId, ComputeActorId, stuff, ReadSpec, pathInd + StartPathIndex, std::get<TString>(path));
            RegisterWithSameMailbox(std::make_unique<TS3ReadCoroActor>(std::move(impl), std::move(stuff)).release());
        }
    }

    static constexpr char ActorName[] = "S3_STREAM_READ_ACTOR";

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

    i64 GetAsyncInputData(TUnboxedValueVector& output, TMaybe<TInstant>&, bool& finished, i64 free) final {
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
        LOG_D("TS3StreamReadActor", "PassAway");
        for (auto stuff: RetryStuffForFile) {
            stuff->Cancel();
        }
        ContainerCache.Clear();
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
        /*
        If an empty range is being downloaded on the last file,
        then we need to pass the information to Compute Actor that
        the download of all data is finished in this place
        */
        if (Blocks.empty() && Count == 0) {
            Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
        }
    }

    const IHTTPGateway::TPtr Gateway;
    std::vector<TRetryStuff::TPtr> RetryStuffForFile;
    const THolderFactory& HolderFactory;
    TPlainContainerCache ContainerCache;

    const ui64 InputIndex;
    const TTxId TxId;
    const NActors::TActorId ComputeActorId;
    const IRetryPolicy<long>::TPtr RetryPolicy;

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
// the same func exists in clickhouse client udf :(
NDB::DataTypePtr PgMetaToClickHouse(const TPgType* type) {
    auto typeId = type->GetTypeId();
    TTypeInfoHelper typeInfoHelper;
    auto* pgDescription = typeInfoHelper.FindPgTypeDescription(typeId);
    Y_ENSURE(pgDescription);
    const auto typeName = pgDescription->Name;
    using NUdf::TStringRef;
    if (typeName == TStringRef("bool")) {
        return std::make_shared<NDB::DataTypeUInt8>();
    }

    if (typeName == TStringRef("int4")) {
        return std::make_shared<NDB::DataTypeInt32>();
    }

    if (typeName == TStringRef("int8")) {
        return std::make_shared<NDB::DataTypeInt64>();
    }

    if (typeName == TStringRef("float4")) {
        return std::make_shared<NDB::DataTypeFloat32>();
    }

    if (typeName == TStringRef("float8")) {
        return std::make_shared<NDB::DataTypeFloat64>();
    }
    return std::make_shared<NDB::DataTypeString>();
}

NDB::DataTypePtr PgMetaToNullableClickHouse(const TPgType* type) {
    return makeNullable(PgMetaToClickHouse(type));
}

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
        case TType::EKind::Pg:
            return PgMetaToNullableClickHouse(AS_TYPE(TPgType, type));
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
            case NUdf::EDataSlot::Timestamp:
            case NUdf::EDataSlot::TzTimestamp:
                return std::make_shared<NDB::DataTypeDateTime64>(6);
            case NUdf::EDataSlot::Uuid:
                return std::make_shared<NDB::DataTypeUUID>();
            case NUdf::EDataSlot::Interval:
                return NSerialization::GetInterval(unit);
            default:
                throw yexception() << "Unsupported data slot in MetaToClickHouse: " << slot;
            }
        }
        default:
            throw yexception() << "Unsupported type kind in MetaToClickHouse: " << type->GetKindAsStr();
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
    const TTxId& txId,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const NActors::TActorId& computeActorId,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const IRetryPolicy<long>::TPtr& retryPolicy)
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
            auto& column = readSpec->Columns[i];
            column.type = MetaToClickHouse(structType->GetMemberType(i), intervalUnit);
            column.name = structType->GetMemberName(i);
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

        const auto actor = new TS3StreamReadActor(inputIndex, txId, std::move(gateway), holderFactory, params.GetUrl(), authToken,
                                                  std::move(paths), addPathIndex, startPathIndex, readSpec, computeActorId, retryPolicy);
        return {actor, actor};
    } else {
        ui64 sizeLimit = std::numeric_limits<ui64>::max();
        if (const auto it = settings.find("sizeLimit"); settings.cend() != it)
            sizeLimit = FromString<ui64>(it->second);

        const auto actor = new TS3ReadActor(inputIndex, txId, std::move(gateway), holderFactory, params.GetUrl(), authToken,
                                            std::move(paths), addPathIndex, startPathIndex, computeActorId, sizeLimit, retryPolicy);
        return {actor, actor};
    }
}

} // namespace NYql::NDq
