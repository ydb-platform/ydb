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
#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBufferFromFile.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/Block.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/ColumnsWithTypeAndName.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatFactory.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Processors/Formats/InputStreamFromInputFormat.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Processors/Formats/Impl/ArrowBufferedStreams.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/cast.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#endif

#include "yql_s3_read_actor.h"
#include "yql_s3_source_factory.h"
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

#include <ydb/library/yql/providers/s3/common/util.h>
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

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw yexception() << _s.ToString(); \
    } while (false)

namespace NYql::NDq {

using namespace ::NActors;
using namespace ::NYql::NS3Details;

namespace {

constexpr TDuration MEMORY_USAGE_REPORT_PERIOD = TDuration::Seconds(10);

struct TS3ReadAbort : public yexception {
    using yexception::yexception;
};

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
        EvNextRecordBatch,
        EvBlockProcessed,
        EvFileFinished,
        EvPause,
        EvContinue,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvReadResult : public TEventLocal<TEvReadResult, EvReadResult> {
        TEvReadResult(IHTTPGateway::TContent&& result, const TString& requestId, size_t pathInd)
            : Result(std::move(result))
            , RequestId(requestId)
            , PathIndex(pathInd)
        {}

        IHTTPGateway::TContent Result;
        const TString RequestId;
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
        TEvReadFinished(size_t pathIndex, TIssues&& issues)
            : PathIndex(pathIndex), Issues(std::move(issues)) {
        }
        const size_t PathIndex;
        TIssues Issues;
    };

    struct TEvFileFinished : public TEventLocal<TEvFileFinished, EvFileFinished> {
        TEvFileFinished(size_t pathIndex, ui64 ingressBytes, ui64 expectedDataSize, ui64 actualDataSize)
            : PathIndex(pathIndex), IngressBytes(ingressBytes), 
              ExpectedDataSize(expectedDataSize), ActualDataSize(actualDataSize) {
        }
        const size_t PathIndex;
        ui64 IngressBytes;
        ui64 ExpectedDataSize;
        ui64 ActualDataSize;
    };

    struct TEvReadError : public TEventLocal<TEvReadError, EvReadError> {
        TEvReadError(TIssues&& error, const TString& requestId, size_t pathInd = std::numeric_limits<size_t>::max())
            : Error(std::move(error))
            , RequestId(requestId)
            , PathIndex(pathInd)
        {}

        const TIssues Error;
        const TString RequestId;
        const size_t PathIndex;
    };

    struct TEvRetryEventFunc : public NActors::TEventLocal<TEvRetryEventFunc, EvRetry> {
        explicit TEvRetryEventFunc(std::function<void()> functor) : Functor(std::move(functor)) {}
        const std::function<void()> Functor;
    };

    struct TEvNextBlock : public NActors::TEventLocal<TEvNextBlock, EvNextBlock> {
        TEvNextBlock(NDB::Block& block, size_t pathInd, std::function<void()> functor = [](){}, ui64 ingressBytes = 0) : PathIndex(pathInd), Functor(functor), IngressBytes(ingressBytes) { Block.swap(block); }
        NDB::Block Block;
        const size_t PathIndex;
        std::function<void()> Functor;
        ui64 IngressBytes;
    };

    struct TEvNextRecordBatch : public NActors::TEventLocal<TEvNextRecordBatch, EvNextRecordBatch> {
        TEvNextRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch, size_t pathInd, std::function<void()> functor = []() {}, ui64 ingressBytes = 0) : Batch(batch), PathIndex(pathInd), Functor(functor), IngressBytes(ingressBytes) { }
        std::shared_ptr<arrow::RecordBatch> Batch;
        const size_t PathIndex;
        std::function<void()> Functor;
        ui64 IngressBytes;
    };

    struct TEvBlockProcessed : public NActors::TEventLocal<TEvBlockProcessed, EvBlockProcessed> {
        TEvBlockProcessed() {}
    };

    struct TEvPause : public NActors::TEventLocal<TEvPause, EvPause> {
    };

    struct TEvContinue : public NActors::TEventLocal<TEvContinue, EvContinue> {
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
        , Token(token)
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
            const TString requestId = CreateGuidAsString();
            LOG_D("TS3ReadActor", "Download: " << url << ", ID: " << id << ", request id: [" << requestId << "]");
            Gateway->Download(url, MakeHeaders(Token, requestId), std::min(std::get<size_t>(path), SizeLimit),
                std::bind(&TS3ReadActor::OnDownloadFinished, ActorSystem, SelfId(), requestId, std::placeholders::_1, id), {}, RetryPolicy);
        };
    }

    static constexpr char ActorName[] = "S3_READ_ACTOR";

private:
    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {}
    void LoadState(const NDqProto::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}
    ui64 GetInputIndex() const final { return InputIndex; }

    ui64 GetIngressBytes() override {
        return IngressBytes;
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvReadResult, Handle);
        hFunc(TEvPrivate::TEvReadError, Handle);
    )

    static void OnDownloadFinished(TActorSystem* actorSystem, TActorId selfId, const TString& requestId, IHTTPGateway::TResult&& result, size_t pathInd) {
        switch (result.index()) {
        case 0U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadResult(std::get<IHTTPGateway::TContent>(std::move(result)), requestId, pathInd)));
            return;
        case 1U:
            actorSystem->Send(new IEventHandle(selfId, TActorId(), new TEvPrivate::TEvReadError(std::get<TIssues>(std::move(result)), requestId, pathInd)));
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
        const auto path = std::get<TString>(Paths[id - StartPathIndex]);
        const auto httpCode = result->Get()->Result.HttpResponseCode;
        const auto requestId = result->Get()->RequestId;
        IngressBytes += result->Get()->Result.size();
        LOG_D("TS3ReadActor", "ID: " << id << ", Path: " << path << ", read size: " << result->Get()->Result.size() << ", HTTP response code: " << httpCode << ", request id: [" << requestId << "]");
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
            message = TStringBuilder{} << "Error while reading file " << path << ", details: " << message << ", request id: [" << requestId << "]";
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, BuildIssues(httpCode, errorCode, message), NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
        }
    }

    void Handle(TEvPrivate::TEvReadError::TPtr& result) {
        ++IsDoneCounter;
        auto id = result->Get()->PathIndex;
        const auto requestId = result->Get()->RequestId;
        const auto path = std::get<TString>(Paths[id - StartPathIndex]);
        LOG_W("TS3ReadActor", "Error while reading file " << path << ", details: ID: " << id << ", TEvReadError: " << result->Get()->Error.ToOneLineString() << ", request id: [" << requestId << "]");
        auto issues = NS3Util::AddParentIssue(TStringBuilder{} << "Error while reading file " << path << " with request id [" << requestId << "]", TIssues{result->Get()->Error});
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        LOG_D("TS3ReadActor", "PassAway");
        ContainerCache.Clear();
        TActorBootstrapped<TS3ReadActor>::PassAway();
    }

    static IHTTPGateway::THeaders MakeHeaders(const TString& token, const TString& requestId) {
        IHTTPGateway::THeaders headers{TString{"X-Request-ID:"} += requestId};
        if (token) {
            headers.emplace_back(TString("X-YaCloud-SubjectToken:") += token);
        }
        return headers;
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
    const TString Token;
    const TPathList Paths;
    const bool AddPathIndex;
    const ui64 StartPathIndex;
    const ui64 SizeLimit;
    ui64 IngressBytes = 0;

    std::queue<std::tuple<IHTTPGateway::TContent, ui64>> Blocks;
};

struct TReadSpec {
    using TPtr = std::shared_ptr<TReadSpec>;

    bool Arrow = false;
    NDB::ColumnsWithTypeAndName CHColumns;
    std::shared_ptr<arrow::Schema> ArrowSchema;
    NDB::FormatSettings Settings;
    TString Format, Compression;
    ui64 SizeLimit = 0;
    ui32 BlockLengthPosition = 0;
    std::vector<ui32> ColumnReorder;
};

struct TRetryStuff {
    using TPtr = std::shared_ptr<TRetryStuff>;

    TRetryStuff(
        IHTTPGateway::TPtr gateway,
        TString url,
        const IHTTPGateway::THeaders& headers,
        std::size_t sizeLimit,
        const TTxId& txId,
        const TString& requestId,
        const IRetryPolicy<long>::TPtr& retryPolicy
    ) : Gateway(std::move(gateway))
      , Url(std::move(url))
      , Headers(headers)
      , Offset(0U)
      , SizeLimit(sizeLimit)
      , TxId(txId)
      , RequestId(requestId)
      , RetryState(retryPolicy->CreateRetryState())
      , Cancelled(false)
    {}

    const IHTTPGateway::TPtr Gateway;
    const TString Url;
    const IHTTPGateway::THeaders Headers;
    std::size_t Offset, SizeLimit;
    const TTxId TxId;
    const TString RequestId;
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

void OnDownloadFinished(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, size_t pathIndex, TIssues issues) {
    actorSystem->Send(new IEventHandle(self, parent, new TEvPrivate::TEvReadFinished(pathIndex, std::move(issues))));
}

void DownloadStart(const TRetryStuff::TPtr& retryStuff, TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, size_t pathIndex) {
    retryStuff->CancelHook = retryStuff->Gateway->Download(retryStuff->Url,
        retryStuff->Headers, retryStuff->Offset, retryStuff->SizeLimit,
        std::bind(&OnDownloadStart, actorSystem, self, parent, std::placeholders::_1),
        std::bind(&OnNewData, actorSystem, self, parent, std::placeholders::_1),
        std::bind(&OnDownloadFinished, actorSystem, self, parent, pathIndex, std::placeholders::_1));
}

template <typename T>
class IBatchReader {
public:
    virtual ~IBatchReader() = default;

    virtual bool Next(T& value) = 0;
};

class TBlockReader : public IBatchReader<NDB::Block> {
public:
    TBlockReader(std::unique_ptr<NDB::IBlockInputStream>&& stream)
        : Stream(std::move(stream))
    {}

    bool Next(NDB::Block& value) final {
        value = Stream->read();
        return !!value;
    }

private:
    std::unique_ptr<NDB::IBlockInputStream> Stream;
};

using TColumnConverter = std::function<std::shared_ptr<arrow::Array>(const std::shared_ptr<arrow::Array>&)>;

class TArrowParquetBatchReader : public IBatchReader<std::shared_ptr<arrow::RecordBatch>> {
public:
    TArrowParquetBatchReader(std::unique_ptr<parquet::arrow::FileReader>&& fileReader, std::vector<int>&& columnIndices, std::vector<TColumnConverter>&& columnConverters)
        : FileReader(std::move(fileReader))
        , ColumnIndices(std::move(columnIndices))
        , ColumnConverters(std::move(columnConverters))
        , TotalGroups(FileReader->num_row_groups())
        , CurrentGroup(0)
    {}

    bool Next(std::shared_ptr<arrow::RecordBatch>& value) final {
        for (;;) {
            if (CurrentGroup == TotalGroups) {
                return false;
            }

            if (!CurrentBatchReader) {
                THROW_ARROW_NOT_OK(FileReader->ReadRowGroup(CurrentGroup++, ColumnIndices, &CurrentTable));
                CurrentBatchReader = std::make_unique<arrow::TableBatchReader>(*CurrentTable);
            }

            THROW_ARROW_NOT_OK(CurrentBatchReader->ReadNext(&value));
            if (value) {
                auto columns = value->columns();
                for (size_t i = 0; i < ColumnConverters.size(); ++i) {
                    auto converter = ColumnConverters[i];
                    if (converter) {
                        columns[i] = converter(columns[i]);
                    }
                }

                value = arrow::RecordBatch::Make(value->schema(), value->num_rows(), columns);
                return true;
            }

            CurrentBatchReader = nullptr;
            CurrentTable = nullptr;
        }
    }

private:
    std::unique_ptr<parquet::arrow::FileReader> FileReader;
    const std::vector<int> ColumnIndices;
    std::vector<TColumnConverter> ColumnConverters;
    const int TotalGroups;
    int CurrentGroup;
    std::shared_ptr<arrow::Table> CurrentTable;
    std::unique_ptr<arrow::TableBatchReader> CurrentBatchReader;
};

ui64 GetSizeOfData(const arrow::ArrayData& data) {
    ui64 size = sizeof(data);
    size += data.buffers.size() * sizeof(void*);
    size += data.child_data.size() * sizeof(void*);
    for (const auto& b : data.buffers) {
        if (b) {
            size += b->size();
        }
    }

    for (const auto& c : data.child_data) {
        if (c) {
            size += GetSizeOfData(*c);
        }
    }

    return size;
}

ui64 GetSizeOfBatch(const arrow::RecordBatch& batch) {
    ui64 size = sizeof(batch);
    size += batch.num_columns() * sizeof(void*);
    for (int i = 0; i < batch.num_columns(); ++i) {
        size += GetSizeOfData(*batch.column_data(i));
    }

    return size;
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
    TS3ReadCoroImpl(ui64 inputIndex, const TTxId& txId, const NActors::TActorId& computeActorId, 
        const TRetryStuff::TPtr& retryStuff, const TReadSpec::TPtr& readSpec, size_t pathIndex, 
        const TString& path, const TString& url, const std::size_t maxBlocksInFly, 
        const TS3ReadActorFactoryConfig& readActorFactoryCfg, ui64 expectedDataSize)
        : TActorCoroImpl(256_KB), ReadActorFactoryCfg(readActorFactoryCfg), InputIndex(inputIndex), 
        TxId(txId), RetryStuff(retryStuff), ReadSpec(readSpec), ComputeActorId(computeActorId), 
        PathIndex(pathIndex), Path(path), Url(url), MaxBlocksInFly(maxBlocksInFly), ExpectedDataSize(expectedDataSize)
    {}

    bool Next(TString& value) {
        if (InputFinished)
            return false;

        if (Paused || DeferredEvents.empty()) {
            auto ev = WaitForSpecificEvent<TEvPrivate::TEvReadStarted
                                         , TEvPrivate::TEvDataPart
                                         , TEvPrivate::TEvReadFinished
                                         , TEvPrivate::TEvPause
                                         , TEvPrivate::TEvContinue
                                         , NActors::TEvents::TEvPoison>();

            switch (const auto etype = ev->GetTypeRewrite()) {
                case TEvPrivate::TEvPause::EventType:
                    Paused = true;
                    break;
                case TEvPrivate::TEvContinue::EventType:
                    Paused = false;
                    break;
                case NActors::TEvents::TEvPoison::EventType:
                    RetryStuff->Cancel();
                    throw TS3ReadAbort();
                default:
                    DeferredEvents.push(std::move(ev));
                    break;
            }
        }

        if (Paused || DeferredEvents.empty()) {
            value.clear();
            return true;
        }

        THolder<IEventHandle> ev;
        ev.Swap(DeferredEvents.front());
        DeferredEvents.pop();

        switch (const auto etype = ev->GetTypeRewrite()) {
            case TEvPrivate::TEvReadStarted::EventType:
                ErrorText.clear();
                Issues.Clear();
                value.clear();
                RetryStuff->NextRetryDelay = RetryStuff->RetryState->GetNextRetryDelay(HttpResponseCode = ev->Get<TEvPrivate::TEvReadStarted>()->HttpResponseCode);
                LOG_CORO_D("TS3ReadCoroImpl", "TEvReadStarted, Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", Http code: " << HttpResponseCode << ", retry after: " << RetryStuff->NextRetryDelay << ", request id: [" << RetryStuff->RequestId << "]");
                return true;
            case TEvPrivate::TEvReadFinished::EventType:
                Issues = std::move(ev->Get<TEvPrivate::TEvReadFinished>()->Issues);
                if (Issues) {
                    if (RetryStuff->NextRetryDelay = RetryStuff->RetryState->GetNextRetryDelay(0L); !RetryStuff->NextRetryDelay) {
                        InputFinished = true;
                        LOG_CORO_W("TS3ReadCoroImpl", "ReadError: " << Issues.ToOneLineString() << ", Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", LastOffset: " << LastOffset << ", LastData: " << GetLastDataAsText() << ", request id: [" << RetryStuff->RequestId << "]");
                        throw TS3ReadError(); // Don't pass control to data parsing, because it may validate eof and show wrong issues about incorrect data format
                    }
                }

                if (!RetryStuff->IsCancelled() && RetryStuff->NextRetryDelay && RetryStuff->SizeLimit > 0ULL) {
                    LOG_CORO_D("TS3ReadCoroImpl", "Retry Download in " << RetryStuff->NextRetryDelay << ", Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", request id: [" << RetryStuff->RequestId << "], Issues: " << Issues.ToOneLineString());
                    GetActorSystem()->Schedule(*RetryStuff->NextRetryDelay, new IEventHandle(ParentActorId, SelfActorId, new TEvPrivate::TEvRetryEventFunc(std::bind(&DownloadStart, RetryStuff, GetActorSystem(), SelfActorId, ParentActorId, PathIndex))));
                    value.clear();
                } else {
                    LOG_CORO_D("TS3ReadCoroImpl", "TEvReadFinished, Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", LastOffset: " << LastOffset << ", Error: " << ServerReturnedError << ", LastData: " << GetLastDataAsText() << ", request id: [" << RetryStuff->RequestId << "]");
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
                    IngressBytes += value.size();
                    RetryStuff->Offset += value.size();
                    RetryStuff->SizeLimit -= value.size();
                    LastOffset = RetryStuff->Offset;
                    LastData = value;
                    LOG_CORO_T("TS3ReadCoroImpl", "TEvDataPart, size: " << value.size() << ", Url: " << RetryStuff->Url << ", Offset (updated): " << RetryStuff->Offset << ", request id: [" << RetryStuff->RequestId << "]");
                    Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
                } else if (HttpResponseCode && !RetryStuff->IsCancelled() && !RetryStuff->NextRetryDelay) {
                    ServerReturnedError = true;
                    if (ErrorText.size() < 256_KB)
                        ErrorText.append(ev->Get<TEvPrivate::TEvDataPart>()->Result.Extract());
                    else if (!ErrorText.EndsWith(TruncatedSuffix))
                        ErrorText.append(TruncatedSuffix);
                    value.clear();
                    LOG_CORO_W("TS3ReadCoroImpl", "TEvDataPart, ERROR: " << ErrorText << ", Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", LastOffset: " << LastOffset << ", LastData: " << GetLastDataAsText() << ", request id: [" << RetryStuff->RequestId << "]");
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
            const auto ev = WaitForSpecificEvent<TEvPrivate::TEvReadStarted
                                               , TEvPrivate::TEvDataPart
                                               , TEvPrivate::TEvReadFinished
                                               , TEvPrivate::TEvPause
                                               , TEvPrivate::TEvContinue
                                               , NActors::TEvents::TEvPoison>();

            const auto etype = ev->GetTypeRewrite();
            switch (etype) {
                case TEvPrivate::TEvReadFinished::EventType:
                    Issues = std::move(ev->Get<TEvPrivate::TEvReadFinished>()->Issues);
                    LOG_CORO_D("TS3ReadCoroImpl", "TEvReadFinished: " << Path << " " << Issues.ToOneLineString());
                    break;
                case NActors::TEvents::TEvPoison::EventType:
                    RetryStuff->Cancel();
                    throw TS3ReadAbort();
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
        bool isLocal = Url.StartsWith("file://");
        try {
            std::unique_ptr<NDB::ReadBuffer> buffer;
            if (isLocal) {
                buffer = std::make_unique<NDB::ReadBufferFromFile>(Url.substr(7) + Path);
            } else {
                buffer = std::make_unique<TReadBufferFromStream>(this);
            }

            const auto decompress(MakeDecompressor(*buffer, ReadSpec->Compression));
            YQL_ENSURE(ReadSpec->Compression.empty() == !decompress, "Unsupported " << ReadSpec->Compression << " compression.");
            if (ReadSpec->Arrow) {
                YQL_ENSURE(ReadSpec->Format == "parquet");
                std::unique_ptr<parquet::arrow::FileReader> fileReader;
                auto arrowFile = NDB::asArrowFile(decompress ? *decompress : *buffer);

                THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(arrowFile, arrow::default_memory_pool(), &fileReader));
                std::shared_ptr<arrow::Schema> schema;
                THROW_ARROW_NOT_OK(fileReader->GetSchema(&schema));

                std::vector<int> columnIndices;
                std::vector<TColumnConverter> columnConverters;
                for (int i = 0; i < ReadSpec->ArrowSchema->num_fields(); ++i) {
                    const auto& targetField = ReadSpec->ArrowSchema->field(i);
                    auto srcFieldIndex = schema->GetFieldIndex(targetField->name());
                    YQL_ENSURE(srcFieldIndex != -1, "Missing field: " << targetField->name());
                    auto targetType = targetField->type();
                    auto originalType = schema->field(srcFieldIndex)->type();
                    YQL_ENSURE(!originalType->layout().has_dictionary, "Unsupported dictionary encoding is used for field: "
                               << targetField->name() << ", type: " << originalType->ToString());
                    if (targetType->Equals(originalType)) {
                        columnConverters.emplace_back();
                    } else {
                        YQL_ENSURE(arrow::compute::CanCast(*originalType, *targetType), "Mismatch type for field: " << targetField->name() << ", expected: "
                            << targetType->ToString() << ", got: " << originalType->ToString());
                        columnConverters.emplace_back([targetType](const std::shared_ptr<arrow::Array>& value) {
                            auto res = arrow::compute::Cast(*value, targetType);
                            THROW_ARROW_NOT_OK(res.status());
                            return std::move(res).ValueOrDie();
                        });
                    }

                    columnIndices.push_back(srcFieldIndex);
                }

                TArrowParquetBatchReader reader(std::move(fileReader), std::move(columnIndices), std::move(columnConverters));
                ActualDataSize += ProcessBatches<std::shared_ptr<arrow::RecordBatch>, TEvPrivate::TEvNextRecordBatch>(reader, isLocal);
            } else {
                auto stream = std::make_unique<NDB::InputStreamFromInputFormat>(NDB::FormatFactory::instance().getInputFormat(ReadSpec->Format, decompress ? *decompress : *buffer, NDB::Block(ReadSpec->CHColumns), nullptr, ReadActorFactoryCfg.RowsInBatch, ReadSpec->Settings));
                TBlockReader reader(std::move(stream));
                ActualDataSize += ProcessBatches<NDB::Block, TEvPrivate::TEvNextBlock>(reader, isLocal);
            }
        } catch (const TS3ReadError&) {
            // Finish reading. Add error from server to issues
            LOG_CORO_D("TS3ReadCoroImpl", "S3 read error. Path: " << Path);
        } catch (const TDtorException&) {
            throw;
        } catch (const std::exception& err) {
            exceptIssue.SetMessage(err.what());
            fatalCode = NYql::NDqProto::StatusIds::INTERNAL_ERROR;
            RetryStuff->Cancel();
        }

        if (!isLocal) {
            WaitFinish();
        }

        if (!ErrorText.empty()) {
            TString errorCode;
            TString message;
            if (!ParseS3ErrorResponse(ErrorText, errorCode, message)) {
                message = ErrorText;
            }
            Issues.AddIssues(BuildIssues(HttpResponseCode, errorCode, message));
        }

        if (exceptIssue.GetMessage()) {
            Issues.AddIssue(exceptIssue);
        }

        auto issues = NS3Util::AddParentIssue(TStringBuilder{} << "Error while reading file " << Path, std::move(Issues));
        if (issues)
            Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, std::move(issues), fatalCode));
        else
            Send(ParentActorId, new TEvPrivate::TEvFileFinished(PathIndex, IngressBytes, ExpectedDataSize, ActualDataSize));
    } catch (const TS3ReadAbort&) {
        LOG_CORO_D("TS3ReadCoroImpl", "S3 read abort. Path: " << Path);
    } catch (const TDtorException&) {
        return RetryStuff->Cancel();
    } catch (const std::exception& err) {
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, TIssues{TIssue(TStringBuilder() << "Error while reading file " << Path << ", details: " << err.what())}, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
        return;
    }

    template <typename T>
    ui64 SizeOfBatch(const T&) {
        return 0;
    }

    template <>
    ui64 SizeOfBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
        return GetSizeOfBatch(*batch);
    }
    
    template <>
    ui64 SizeOfBatch(const NDB::Block& batch) {
        return batch.bytes();
    }

    template <typename T, typename TEv>
    ui64 ProcessBatches(IBatchReader<T>& reader, bool isLocal) {
        auto actorSystem = GetActorSystem();
        auto selfActorId = SelfActorId;
        size_t cntBlocksInFly = 0;
        ui64 result = 0;
        if (isLocal) {
            for (;;) {
                T batch;
                if (!reader.Next(batch)) {
                    break;
                }

                result += SizeOfBatch<T>(batch);

                if (++cntBlocksInFly > MaxBlocksInFly) {
                    WaitForSpecificEvent<TEvPrivate::TEvBlockProcessed>();
                    --cntBlocksInFly;
                }
                Send(ParentActorId, new TEv(batch, PathIndex, [actorSystem, selfActorId]() {
                    actorSystem->Send(new IEventHandle(selfActorId, selfActorId, new TEvPrivate::TEvBlockProcessed()));
                }, IngressBytes));
            }
            while (cntBlocksInFly--) {
                WaitForSpecificEvent<TEvPrivate::TEvBlockProcessed>();
            }
        } else {
            for (;;) {
                T batch;
                if (!reader.Next(batch)) {
                    break;
                }
                result += SizeOfBatch<T>(batch);
                Send(ParentActorId, new TEv(batch, PathIndex));
            }
        }
        return result;
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) final {
        TStringBuilder message;
        message << "Error while reading file " << Path << ", details: "
                << "S3 read. Unexpected message type " << Hex(ev->GetTypeRewrite());
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
    const TS3ReadActorFactoryConfig ReadActorFactoryCfg;
    const ui64 InputIndex;
    const TTxId TxId;
    const TRetryStuff::TPtr RetryStuff;
    const TReadSpec::TPtr ReadSpec;
    const TString Format, RowType, Compression;
    const NActors::TActorId ComputeActorId;
    const size_t PathIndex;
    const TString Path;
    const TString Url;

    bool InputFinished = false;
    long HttpResponseCode = 0L;
    bool ServerReturnedError = false;
    TString ErrorText;
    TIssues Issues;

    std::size_t LastOffset = 0;
    TString LastData;
    std::size_t MaxBlocksInFly = 2;
    ui64 IngressBytes = 0;
    ui64 ExpectedDataSize;
    ui64 ActualDataSize = 0;
    bool Paused = false;
    std::queue<THolder<IEventHandle>> DeferredEvents;
};

class TS3ReadCoroActor : public TActorCoro {
public:
    TS3ReadCoroActor(THolder<TS3ReadCoroImpl> impl, TRetryStuff::TPtr retryStuff, size_t pathIndex)
        : TActorCoro(THolder<TActorCoroImpl>(impl.Release()))
        , RetryStuff(std::move(retryStuff))
        , PathIndex(pathIndex)
    {}
private:
    void Registered(TActorSystem* actorSystem, const TActorId& parent) override {
        TActorCoro::Registered(actorSystem, parent); // Calls TActorCoro::OnRegister and sends bootstrap event to ourself.
        if (RetryStuff->Url.substr(0, 6) != "file://") {
            LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_COMPUTE, "TS3ReadCoroActor" << ": " << SelfId() << ", TxId: " << RetryStuff->TxId << ". " << "Start Download, Url: " << RetryStuff->Url << ", Offset: " << RetryStuff->Offset << ", request id: [" << RetryStuff->RequestId << "]");
            DownloadStart(RetryStuff, actorSystem, SelfId(), parent, PathIndex);
        }
    }

    const TRetryStuff::TPtr RetryStuff;
    const size_t PathIndex;
};

double FormatRatio(const TString& formatName) {
    Y_UNUSED(formatName);
    return 1.0;
}

double CompressionRatio(const TString& compressionName) {
    if (compressionName == "gzip") {
        return 15.0;
    }
    if (compressionName == "lz4") {
        return 20.0;
    }
    if (compressionName) {
        // "brotli"
        // "zstd"
        // "bzip2"
        // "xz"
        return 10.0;
    }
    // no compression
    return 1.0;
}

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
        const IRetryPolicy<long>::TPtr& retryPolicy,
        const std::size_t maxBlocksInFly,
        const TS3ReadActorFactoryConfig& readActorFactoryCfg,
        ::NMonitoring::TDynamicCounterPtr counters,
        ::NMonitoring::TDynamicCounterPtr taskCounters
    )   : ReadActorFactoryCfg(readActorFactoryCfg)
        , Gateway(std::move(gateway))
        , HolderFactory(holderFactory)
        , InputIndex(inputIndex)
        , TxId(txId)
        , ComputeActorId(computeActorId)
        , RetryPolicy(retryPolicy)
        , Url(url)
        , Token(token)
        , Paths(std::move(paths))
        , AddPathIndex(addPathIndex)
        , StartPathIndex(startPathIndex)
        , ReadSpec(readSpec)
        , Count(Paths.size())
        , MaxBlocksInFly(maxBlocksInFly)
        , Counters(counters)
        , TaskCounters(taskCounters)
    {
        if (Counters) {
            QueueDataSize = Counters->GetCounter("QueueDataSize");
            QueueBlockCount = Counters->GetCounter("QueueBlockCount");
            BufferDataSize = Counters->GetCounter("BufferDataSize");
            DownloadPaused = Counters->GetCounter("DownloadPaused");
        }
        if (TaskCounters) {
            ExpectedDataSize = TaskCounters->GetCounter("ExpectedDataSize");
            ActualDataSize = TaskCounters->GetCounter("ActualDataSize");
        }
        Ratio = FormatRatio(ReadSpec->Compression) * CompressionRatio(ReadSpec->Format);
    }

    void Bootstrap() {
        LOG_D("TS3StreamReadActor", "Bootstrap");
        Become(&TS3StreamReadActor::StateFunc);
        while (TryRegisterCoro()) {

        }
    }

    bool TryRegisterCoro() {
        if (CurrentPathIndex >= Paths.size()) {
            // no path is pending
            return false;
        }
        /*
        if (BufferTotalDataSize > static_cast<i64>(ReadActorFactoryCfg.DataInflight)) {
            // too large data inflight
            return false;
        }
        */
        if (DownloadInflight >= ReadActorFactoryCfg.MaxInflight) {
            // too large download inflight
            return false;
        }
        RegisterCoro(CurrentPathIndex++);
        return true;
    }

    void RegisterCoro(size_t index) {
        DownloadInflight++;
        const TPath& path = Paths[index];
        const TString requestId = CreateGuidAsString();
        ui64 fileSize = std::get<std::size_t>(path);
        ui64 expectedDataSize = (ui64) fileSize * Ratio;
        auto stuff = std::make_shared<TRetryStuff>(Gateway, Url + std::get<TString>(path), MakeHeaders(Token, requestId), fileSize, TxId, requestId, RetryPolicy);
        auto pathIndex = index + StartPathIndex;
        RetryStuffForFile.emplace(pathIndex, stuff);
        BufferTotalDataSize += expectedDataSize;
        if (Counters) {
            BufferDataSize->Add(expectedDataSize);
        }
        auto impl = MakeHolder<TS3ReadCoroImpl>(InputIndex, TxId, ComputeActorId, stuff, ReadSpec, pathIndex, std::get<TString>(path), Url, MaxBlocksInFly, ReadActorFactoryCfg, expectedDataSize);
        CoroActors.insert(RegisterWithSameMailbox(std::make_unique<TS3ReadCoroActor>(std::move(impl), std::move(stuff), pathIndex).release()));
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

    class TReadyBlock {
    public:
        TReadyBlock(TEvPrivate::TEvNextBlock::TPtr& event) : PathInd(event->Get()->PathIndex), Functor (std::move(event->Get()->Functor)) { Block.swap(event->Get()->Block); }
        TReadyBlock(TEvPrivate::TEvNextRecordBatch::TPtr& event) : Batch(event->Get()->Batch), PathInd(event->Get()->PathIndex), Functor(std::move(event->Get()->Functor)) {}
        NDB::Block Block;
        std::shared_ptr<arrow::RecordBatch> Batch;
        size_t PathInd;
        std::function<void()> Functor;
    };

    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {}
    void LoadState(const NDqProto::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}
    ui64 GetInputIndex() const final { return InputIndex; }

    ui64 GetIngressBytes() override {
        return IngressBytes;
    }

    ui64 GetBlockSize(const TReadyBlock& block) const {
        return ReadSpec->Arrow ? GetSizeOfBatch(*block.Batch) : block.Block.bytes();
    }

    void ReportMemoryUsage() const {
        const TInstant now = TInstant::Now();
        if (now - LastMemoryReport < MEMORY_USAGE_REPORT_PERIOD) {
            return;
        }
        LastMemoryReport = now;
        size_t blocksTotalSize = 0;
        for (const auto& block : Blocks) {
            blocksTotalSize += GetBlockSize(block);
        }
        LOG_D("TS3StreamReadActor", "Memory usage. Ready blocks: " << Blocks.size() << ". Ready blocks total size: " << blocksTotalSize);
    }

    i64 GetAsyncInputData(TUnboxedValueVector& output, TMaybe<TInstant>&, bool& finished, i64 free) final {
        ReportMemoryUsage();

        i64 total = 0LL;
        if (!Blocks.empty()) do {
            const i64 s = GetBlockSize(Blocks.front());

            NUdf::TUnboxedValue value;
            if (ReadSpec->Arrow) {
                const auto& batch = *Blocks.front().Batch;

                NUdf::TUnboxedValue* structItems = nullptr;
                auto structObj = ArrowRowContainerCache.NewArray(HolderFactory, 1 + batch.num_columns(), structItems);
                for (int i = 0; i < batch.num_columns(); ++i) {
                    structItems[ReadSpec->ColumnReorder[i]] = HolderFactory.CreateArrowBlock(arrow::Datum(batch.column_data(i)));
                }

                structItems[ReadSpec->BlockLengthPosition] = HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(batch.num_rows())));
                value = structObj;
            } else {
                value = HolderFactory.Create<TBoxedBlock>(Blocks.front().Block);
            }

            Blocks.front().Functor();

            if (AddPathIndex) {
                NUdf::TUnboxedValue* tupleItems = nullptr;
                auto tuple = ContainerCache.NewArray(HolderFactory, 2, tupleItems);
                *tupleItems++ = value;
                *tupleItems++ = NUdf::TUnboxedValuePod(Blocks.front().PathInd);
                value = tuple;
            }

            free -= s;
            total += s;
            output.emplace_back(std::move(value));
            Blocks.pop_front();
            BufferTotalDataSize -= s;
            QueueTotalDataSize -= s;
            if (Counters) {
                BufferDataSize->Sub(s);
                QueueDataSize->Sub(s);
                QueueBlockCount->Dec();
            }
            TryRegisterCoro();
        } while (!Blocks.empty() && free > 0LL && GetBlockSize(Blocks.front()) <= size_t(free));

        MaybeContinue();

        finished = Blocks.empty() && !Count;
        if (finished) {
            ContainerCache.Clear();
            ArrowTupleContainerCache.Clear();
            ArrowRowContainerCache.Clear();
        }
        return total;
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        LOG_D("TS3StreamReadActor", "PassAway");
        if (Counters) {
            BufferDataSize->Sub(BufferTotalDataSize);
            QueueDataSize->Sub(QueueTotalDataSize);
            QueueBlockCount->Sub(Blocks.size());
            if (Paused) {
                DownloadPaused->Dec();
            }
        }
        BufferTotalDataSize = 0;
        QueueTotalDataSize = 0;

        for (const auto actorId : CoroActors) {
            Send(actorId, new NActors::TEvents::TEvPoison());
        }

        ContainerCache.Clear();
        TActorBootstrapped<TS3StreamReadActor>::PassAway();
    }

    static IHTTPGateway::THeaders MakeHeaders(const TString& token, const TString& requestId) {
        IHTTPGateway::THeaders headers{TString{"X-Request-ID:"} += requestId};
        if (token) {
            headers.emplace_back(TString("X-YaCloud-SubjectToken:") += token);
        }
        return headers;
    }

    void MaybePause() {
        if (!Paused && QueueTotalDataSize >= ReadActorFactoryCfg.DataInflight) {
            for (const auto actorId : CoroActors) {
                Send(actorId, new TEvPrivate::TEvPause());
            }
            Paused = true;
            if (Counters) {
                DownloadPaused->Inc();
            }
        }
    }

    void MaybeContinue() {
        if (Paused && QueueTotalDataSize < ReadActorFactoryCfg.DataInflight) {
            for (const auto actorId : CoroActors) {
                Send(actorId, new TEvPrivate::TEvContinue());
            }
            Paused = false;
            if (Counters) {
                DownloadPaused->Dec();
            }
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvRetryEventFunc, HandleRetry);
        hFunc(TEvPrivate::TEvNextBlock, HandleNextBlock);
        hFunc(TEvPrivate::TEvNextRecordBatch, HandleNextRecordBatch);
        hFunc(TEvPrivate::TEvFileFinished, HandleFileFinished);
    )

    void HandleRetry(TEvPrivate::TEvRetryEventFunc::TPtr& retry) {
        return retry->Get()->Functor();
    }

    void HandleNextBlock(TEvPrivate::TEvNextBlock::TPtr& next) {
        YQL_ENSURE(!ReadSpec->Arrow);
        IngressBytes = next->Get()->IngressBytes;
        auto size = next->Get()->Block.bytes();
        QueueTotalDataSize += size;
        if (Counters) {
            QueueBlockCount->Inc();
            QueueDataSize->Add(size);
        }
        MaybePause();
        Blocks.emplace_back(next);
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
        ReportMemoryUsage();
    }

    void HandleNextRecordBatch(TEvPrivate::TEvNextRecordBatch::TPtr& next) {
        YQL_ENSURE(ReadSpec->Arrow);
        IngressBytes = next->Get()->IngressBytes;
        auto size = GetSizeOfBatch(*next->Get()->Batch);
        QueueTotalDataSize += size;
        if (Counters) {
            QueueBlockCount->Inc();
            QueueDataSize->Add(size);
        }
        MaybePause();
        Blocks.emplace_back(next);
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
        ReportMemoryUsage();
    }

    void HandleFileFinished(TEvPrivate::TEvFileFinished::TPtr& ev) {
        CoroActors.erase(ev->Sender);
        IngressBytes = ev->Get()->IngressBytes;
        RetryStuffForFile.erase(ev->Get()->PathIndex);
        Y_VERIFY(Count);
        --Count;

        // final netto of expected vs actual
        BufferTotalDataSize += ev->Get()->ActualDataSize;
        BufferTotalDataSize -= ev->Get()->ExpectedDataSize;
        if (Counters) {
            BufferDataSize->Add(ev->Get()->ActualDataSize);
            BufferDataSize->Sub(ev->Get()->ExpectedDataSize);
        }
        if (TaskCounters) {
            ExpectedDataSize->Add(ev->Get()->ExpectedDataSize);
            ActualDataSize->Add(ev->Get()->ActualDataSize);
        }
        DownloadInflight--;
        if (CurrentPathIndex < Paths.size()) {
            TryRegisterCoro();
        } else {
            /*
            If an empty range is being downloaded on the last file,
            then we need to pass the information to Compute Actor that
            the download of all data is finished in this place
            */
            if (Blocks.empty() && Count == 0) {
                Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
            }
        }
    }

    const TS3ReadActorFactoryConfig ReadActorFactoryCfg;
    const IHTTPGateway::TPtr Gateway;
    THashMap<size_t, TRetryStuff::TPtr> RetryStuffForFile;
    const THolderFactory& HolderFactory;
    TPlainContainerCache ContainerCache;
    TPlainContainerCache ArrowTupleContainerCache;
    TPlainContainerCache ArrowRowContainerCache;

    const ui64 InputIndex;
    const TTxId TxId;
    const NActors::TActorId ComputeActorId;
    const IRetryPolicy<long>::TPtr RetryPolicy;

    const TString Url;
    const TString Token;
    const TPathList Paths;
    const bool AddPathIndex;
    const ui64 StartPathIndex;
    const TReadSpec::TPtr ReadSpec;
    std::deque<TReadyBlock> Blocks;
    ui32 Count;
    const std::size_t MaxBlocksInFly;
    ui64 IngressBytes = 0;
    size_t CurrentPathIndex = 0;
    mutable TInstant LastMemoryReport = TInstant::Now();
    ui64 QueueTotalDataSize = 0;
    i64 BufferTotalDataSize = 0;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueDataSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueBlockCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr ExpectedDataSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr ActualDataSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr BufferDataSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr DownloadPaused;
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr TaskCounters;
    double Ratio = 0.0;
    ui64 DownloadInflight = 0;
    std::set<NActors::TActorId> CoroActors;
    bool Paused = false;
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
                return std::make_shared<NDB::DataTypeDateTime>("UTC");
            case NUdf::EDataSlot::Timestamp:
            case NUdf::EDataSlot::TzTimestamp:
                return std::make_shared<NDB::DataTypeDateTime64>(6, "UTC");
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

NDB::FormatSettings::DateTimeFormat ToDateTimeFormat(const TString& formatName) {
    static TMap<TString, NDB::FormatSettings::DateTimeFormat> formats{
        {"POSIX", NDB::FormatSettings::DateTimeFormat::POSIX},
        {"ISO", NDB::FormatSettings::DateTimeFormat::ISO}
    };
    if (auto it = formats.find(formatName); it != formats.end()) {
        return it->second;
    }
    return NDB::FormatSettings::DateTimeFormat::Unspecified;
}

NDB::FormatSettings::TimestampFormat ToTimestampFormat(const TString& formatName) {
    static TMap<TString, NDB::FormatSettings::TimestampFormat> formats{
        {"POSIX", NDB::FormatSettings::TimestampFormat::POSIX},
        {"ISO", NDB::FormatSettings::TimestampFormat::ISO},
        {"UNIX_TIME_MILLISECONDS", NDB::FormatSettings::TimestampFormat::UnixTimeMilliseconds},
        {"UNIX_TIME_SECONDS", NDB::FormatSettings::TimestampFormat::UnixTimeSeconds},
        {"UNIX_TIME_MICROSECONDS", NDB::FormatSettings::TimestampFormat::UnixTimeMicroSeconds}
    };
    if (auto it = formats.find(formatName); it != formats.end()) {
        return it->second;
    }
    return NDB::FormatSettings::TimestampFormat::Unspecified;
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
    const IRetryPolicy<long>::TPtr& retryPolicy,
    const TS3ReadActorFactoryConfig& cfg,
    ::NMonitoring::TDynamicCounterPtr counters,
    ::NMonitoring::TDynamicCounterPtr taskCounters)
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
        readSpec->Arrow = params.GetArrow();
        if (readSpec->Arrow) {
            arrow::SchemaBuilder builder;
            auto extraStructType = static_cast<TStructType*>(pb->NewStructType(structType, "_yql_block_length",
                pb->NewBlockType(pb->NewDataType(NUdf::EDataSlot::Uint64), TBlockType::EShape::Scalar)));

            for (ui32 i = 0U; i < extraStructType->GetMembersCount(); ++i) {
                if (extraStructType->GetMemberName(i) == "_yql_block_length") {
                    readSpec->BlockLengthPosition = i;
                    continue;
                }

                auto memberType = extraStructType->GetMemberType(i);
                std::shared_ptr<arrow::DataType> dataType;

                YQL_ENSURE(ConvertArrowType(memberType, dataType), "Unsupported arrow type");
                THROW_ARROW_NOT_OK(builder.AddField(std::make_shared<arrow::Field>(std::string(extraStructType->GetMemberName(i)), dataType, memberType->IsOptional())));
                readSpec->ColumnReorder.push_back(i);
            }

            auto res = builder.Finish();
            THROW_ARROW_NOT_OK(res.status());
            readSpec->ArrowSchema = std::move(res).ValueOrDie();
        } else {
            readSpec->CHColumns.resize(structType->GetMembersCount());
            for (ui32 i = 0U; i < structType->GetMembersCount(); ++i) {
                auto& column = readSpec->CHColumns[i];
                column.type = MetaToClickHouse(structType->GetMemberType(i), intervalUnit);
                column.name = structType->GetMemberName(i);
            }
        }

        readSpec->Format = params.GetFormat();

        if (const auto it = settings.find("compression"); settings.cend() != it)
            readSpec->Compression = it->second;

        if (const auto it = settings.find("csvdelimiter"); settings.cend() != it && !it->second.empty())
            readSpec->Settings.csv.delimiter = it->second[0];

        if (const auto it = settings.find("data.datetime.formatname"); settings.cend() != it) {
            readSpec->Settings.date_time_format_name = ToDateTimeFormat(it->second);
        }

        if (const auto it = settings.find("data.datetime.format"); settings.cend() != it) {
            readSpec->Settings.date_time_format = it->second;
        }

        if (const auto it = settings.find("data.timestamp.formatname"); settings.cend() != it) {
            readSpec->Settings.timestamp_format_name = ToTimestampFormat(it->second);
        }

        if (const auto it = settings.find("data.timestamp.format"); settings.cend() != it) {
            readSpec->Settings.timestamp_format = it->second;
        }

        if (readSpec->Settings.date_time_format_name == NDB::FormatSettings::DateTimeFormat::Unspecified && readSpec->Settings.date_time_format.empty()) {
            readSpec->Settings.date_time_format_name = NDB::FormatSettings::DateTimeFormat::POSIX;
        }

        if (readSpec->Settings.timestamp_format_name == NDB::FormatSettings::TimestampFormat::Unspecified && readSpec->Settings.timestamp_format.empty()) {
            readSpec->Settings.timestamp_format_name = NDB::FormatSettings::TimestampFormat::POSIX;
        }

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
        std::size_t maxBlocksInFly = 2;
        if (const auto it = settings.find("fileReadBlocksInFly"); settings.cend() != it)
            maxBlocksInFly = FromString<ui64>(it->second);
        const auto actor = new TS3StreamReadActor(inputIndex, txId, std::move(gateway), holderFactory, params.GetUrl(), authToken,
                                                  std::move(paths), addPathIndex, startPathIndex, readSpec, computeActorId, retryPolicy,
                                                  maxBlocksInFly, cfg, counters, taskCounters);

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
