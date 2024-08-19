#include <util/system/platform.h>
#if defined(_linux_) || defined(_darwin_)
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeArray.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeDate.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeDateTime64.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypesDecimal.h>
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
#include <arrow/util/future.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <google/protobuf/text_format.h>

#endif

#include "yql_arrow_column_converters.h"
#include "yql_s3_decompressor_actor.h"
#include "yql_s3_actors_util.h"
#include "yql_s3_raw_read_actor.h"
#include "yql_s3_read_actor.h"
#include "yql_s3_source_factory.h"
#include "yql_s3_source_queue.h"

#include <ydb/core/base/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>
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
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/public/udf/arrow/util.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>

#include <ydb/library/yql/providers/s3/common/util.h>
#include <ydb/library/yql/providers/s3/common/source_context.h>
#include <ydb/library/yql/providers/s3/compressors/factory.h>
#include <ydb/library/yql/providers/s3/credentials/credentials.h>
#include <ydb/library/yql/providers/s3/events/events.h>
#include <ydb/library/yql/providers/s3/object_listers/yql_s3_list.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>
#include <ydb/library/yql/providers/s3/proto/file_queue.pb.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/providers/s3/serializations/serialization_interval.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor_coroutine.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <util/generic/size_literals.h>
#include <util/stream/format.h>
#include <util/system/fstat.h>

#include <algorithm>
#include <queue>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/xml/document/xml-document.h>

#define LOG_E(name, stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_W(name, stream) \
    LOG_WARN_S (*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_I(name, stream) \
    LOG_INFO_S (*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_D(name, stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_T(name, stream) \
    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)

#define LOG_CORO_E(stream) \
    LOG_ERROR_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)
#define LOG_CORO_W(stream) \
    LOG_WARN_S (GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)
#define LOG_CORO_I(stream) \
    LOG_INFO_S (GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)
#define LOG_CORO_D(stream) \
    LOG_DEBUG_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)
#define LOG_CORO_T(stream) \
    LOG_TRACE_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw yexception() << _s.ToString(); \
    } while (false)

namespace NYql::NDq {

using namespace ::NActors;
using namespace ::NYql::NS3Details;

using ::NYql::NS3Lister::ES3PatternVariant;
using ::NYql::NS3Lister::ES3PatternType;

namespace {

struct TS3ReadAbort : public yexception {
    using yexception::yexception;
};

struct TS3ReadError : public yexception {
    using yexception::yexception;
};

ui64 SubtractSaturating(ui64 lhs, ui64 rhs) {
    return (lhs > rhs) ? lhs - rhs : 0;
}

struct TReadSpec {
    using TPtr = std::shared_ptr<TReadSpec>;

    bool Arrow = false;
    ui64 ParallelRowGroupCount = 0;
    bool RowGroupReordering = true;
    ui64 ParallelDownloadCount = 0;
    std::unordered_map<TStringBuf, NKikimr::NMiniKQL::TType*, THash<TStringBuf>> RowSpec;
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
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy
    ) : Gateway(std::move(gateway))
      , Url(NS3Util::UrlEscapeRet(url))
      , Headers(headers)
      , Offset(0U)
      , SizeLimit(sizeLimit)
      , TxId(txId)
      , RequestId(requestId)
      , RetryPolicy(retryPolicy)
      , Cancelled(false)
    {}

    const IHTTPGateway::TPtr Gateway;
    const TString Url;
    const IHTTPGateway::THeaders Headers;
    std::size_t Offset, SizeLimit;
    const TTxId TxId;
    const TString RequestId;
    IHTTPGateway::TRetryPolicy::IRetryState::TPtr RetryState;
    IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;
    IHTTPGateway::TCancelHook CancelHook;
    TMaybe<TDuration> NextRetryDelay;
    std::atomic_bool Cancelled;

    const IHTTPGateway::TRetryPolicy::IRetryState::TPtr& GetRetryState() {
        if (!RetryState) {
            RetryState = RetryPolicy->CreateRetryState();
        }
        return RetryState;
    }

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

void OnDownloadStart(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, CURLcode curlResponseCode, long httpResponseCode) {
    actorSystem->Send(new IEventHandle(self, parent, new TEvS3Provider::TEvDownloadStart(curlResponseCode, httpResponseCode)));
}

void OnNewData(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, IHTTPGateway::TCountedContent&& data) {
    actorSystem->Send(new IEventHandle(self, parent, new TEvS3Provider::TEvDownloadData(std::move(data))));
}

void OnDownloadFinished(TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, size_t pathIndex, CURLcode curlResponseCode, TIssues issues) {
    actorSystem->Send(new IEventHandle(self, parent, new TEvS3Provider::TEvDownloadFinish(pathIndex, curlResponseCode, std::move(issues))));
}

void DownloadStart(const TRetryStuff::TPtr& retryStuff, TActorSystem* actorSystem, const TActorId& self, const TActorId& parent, size_t pathIndex, const ::NMonitoring::TDynamicCounters::TCounterPtr& inflightCounter) {
    retryStuff->CancelHook = retryStuff->Gateway->Download(
        retryStuff->Url,
        retryStuff->Headers,
        retryStuff->Offset,
        retryStuff->SizeLimit,
        std::bind(&OnDownloadStart, actorSystem, self, parent, std::placeholders::_1, std::placeholders::_2),
        std::bind(&OnNewData, actorSystem, self, parent, std::placeholders::_1),
        std::bind(&OnDownloadFinished, actorSystem, self, parent, pathIndex, std::placeholders::_1, std::placeholders::_2),
        inflightCounter);
}

struct TParquetFileInfo {
    ui64 RowCount = 0;
    ui64 CompressedSize = 0;
    ui64 UncompressedSize = 0;
};

class TS3ReadCoroImpl : public TActorCoroImpl {
    friend class TS3StreamReadActor;

public:

    class THttpRandomAccessFile : public arrow::io::RandomAccessFile {
    public:
        THttpRandomAccessFile(TS3ReadCoroImpl* coro, size_t fileSize) : Coro(coro), FileSize(fileSize) {
        }

        // has no meaning and use
        arrow::Result<int64_t> GetSize() override { return FileSize; }
        arrow::Result<int64_t> Tell() const override { return InnerPos; }
        arrow::Status Seek(int64_t position) override { InnerPos = position; return {}; }
        arrow::Status Close() override { return {}; }
        bool closed() const override { return false; }
        // must not be used currently
        arrow::Result<int64_t> Read(int64_t, void*) override {
            Y_ABORT_UNLESS(0);
            return arrow::Result<int64_t>();
        }
        arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t) override {
            Y_ABORT_UNLESS(0);
            return arrow::Result<std::shared_ptr<arrow::Buffer>>();
        }
        // useful ones
        arrow::Future<std::shared_ptr<arrow::Buffer>> ReadAsync(const arrow::io::IOContext&, int64_t position, int64_t nbytes) override {
            return arrow::Future<std::shared_ptr<arrow::Buffer>>::MakeFinished(ReadAt(position, nbytes));
        }
        arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
            return Coro->ReadAt(position, nbytes);
        }
        arrow::Status WillNeed(const std::vector<arrow::io::ReadRange>& readRanges) override {
            return Coro->WillNeed(readRanges);
        }

    private:
        TS3ReadCoroImpl *const Coro;
        const size_t FileSize;
        int64_t InnerPos = 0;
    };

    class TRandomAccessFileTrafficCounter : public arrow::io::RandomAccessFile {
    public:
        TRandomAccessFileTrafficCounter(TS3ReadCoroImpl* coro, std::shared_ptr<arrow::io::RandomAccessFile> impl)
            : Coro(coro), Impl(impl) {
        }
        arrow::Result<int64_t> GetSize() override { return Impl->GetSize(); }
        virtual arrow::Result<int64_t> Tell() const override { return Impl->Tell(); }
        virtual arrow::Status Seek(int64_t position) override { return Impl->Seek(position); }
        virtual arrow::Status Close() override { return Impl->Close(); }
        virtual bool closed() const override { return Impl->closed(); }
        arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) override {
            auto result = Impl->Read(nbytes, buffer);
            Coro->IngressBytes += nbytes;
            return result;
        }
        arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
            auto result = Impl->Read(nbytes);
            Coro->IngressBytes += nbytes;
            return result;
        }
        arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
            auto result = Impl->ReadAt(position, nbytes);
            Coro->IngressBytes += nbytes;
            return result;
        }
        arrow::Future<std::shared_ptr<arrow::Buffer>> ReadAsync(const arrow::io::IOContext& ctx, int64_t position, int64_t nbytes) override {
            auto result = Impl->ReadAsync(ctx, position, nbytes);
            Coro->IngressBytes += nbytes;
            return result;
        }
        arrow::Status WillNeed(const std::vector<arrow::io::ReadRange>& ranges) override {
            return Impl->WillNeed(ranges);
        }

    private:
        TS3ReadCoroImpl *const Coro;
        std::shared_ptr<arrow::io::RandomAccessFile> Impl;
    };

    class TCoroReadBuffer : public NDB::ReadBuffer {
    public:
        TCoroReadBuffer(TS3ReadCoroImpl* coro)
            : NDB::ReadBuffer(nullptr, 0ULL)
            , Coro(coro)
        { }
    private:
        bool nextImpl() final {
            while (!Coro->InputFinished || !Coro->DeferredDataParts.empty()) {
                Coro->CpuTime += Coro->GetCpuTimeDelta();
                Coro->ProcessOneEvent();
                Coro->StartCycleCount = GetCycleCountFast();
                if (Coro->InputBuffer) {
                    RawDataBuffer.swap(Coro->InputBuffer);
                    Coro->InputBuffer.clear();
                    auto rawData = const_cast<char*>(RawDataBuffer.data());
                    working_buffer = NDB::BufferBase::Buffer(rawData, rawData + RawDataBuffer.size());
                    return true;
                }
            }
            return false;
        }
        TS3ReadCoroImpl *const Coro;
        TString RawDataBuffer;
    };

    class TCoroDecompressorBuffer : public NDB::ReadBuffer {
    public:
        TCoroDecompressorBuffer(TS3ReadCoroImpl* coro)
            : NDB::ReadBuffer(nullptr, 0ULL)
            , Coro(coro)
        { }
    private:
        bool nextImpl() final {
            while (!Coro->DecompressedInputFinished || !Coro->DeferredDecompressedDataParts.empty()) {
                Coro->CpuTime += Coro->GetCpuTimeDelta();
                Coro->ProcessOneEvent();
                Coro->StartCycleCount = GetCycleCountFast();
                auto decompressed = Coro->ExtractDecompressedDataPart();
                if (decompressed) {
                    RawDataBuffer.swap(decompressed);
                    auto rawData = const_cast<char*>(RawDataBuffer.data());
                    working_buffer = NDB::BufferBase::Buffer(rawData, rawData + RawDataBuffer.size());
                    return true;
                } else if (Coro->InputBuffer) {
                    Coro->Send(Coro->DecompressorActorId, new TEvS3Provider::TEvDecompressDataRequest(std::move(Coro->InputBuffer)));
                }
            }
            return false;
        }
        TS3ReadCoroImpl *const Coro;
        TString RawDataBuffer;
    };

    void RunClickHouseParserOverHttp() {

        LOG_CORO_D("RunClickHouseParserOverHttp");

        std::unique_ptr<NDB::ReadBuffer> coroBuffer = AsyncDecompressing ? std::unique_ptr<NDB::ReadBuffer>(std::make_unique<TCoroDecompressorBuffer>(this)) : std::unique_ptr<NDB::ReadBuffer>(std::make_unique<TCoroReadBuffer>(this));
        std::unique_ptr<NDB::ReadBuffer> decompressorBuffer;
        NDB::ReadBuffer* buffer = coroBuffer.get();

        // lz4 decompressor reads signature in ctor, w/o actual data it will be deadlocked
        DownloadStart(RetryStuff, GetActorSystem(), SelfActorId, ParentActorId, PathIndex, HttpInflightSize);

        if (ReadSpec->Compression && !AsyncDecompressing) {
            decompressorBuffer = MakeDecompressor(*buffer, ReadSpec->Compression);
            YQL_ENSURE(decompressorBuffer, "Unsupported " << ReadSpec->Compression << " compression.");
            buffer = decompressorBuffer.get();
        }

        auto stream = std::make_unique<NDB::InputStreamFromInputFormat>(
            NDB::FormatFactory::instance().getInputFormat(
                ReadSpec->Format, *buffer, NDB::Block(ReadSpec->CHColumns), nullptr, ReadActorFactoryCfg.RowsInBatch, ReadSpec->Settings
            )
        );

        while (NDB::Block batch = stream->read()) {
            Paused = SourceContext->Add(batch.bytes(), SelfActorId);
            const bool isCancelled = StopIfConsumedEnough(batch.rows());
            Send(ParentActorId, new TEvS3Provider::TEvNextBlock(batch, PathIndex, TakeIngressDelta(), TakeCpuTimeDelta(), ReadSpec->Compression ? TakeIngressDecompressedDelta(buffer->count()) : 0ULL));
            if (Paused) {
                CpuTime += GetCpuTimeDelta();
                auto ev = WaitForSpecificEvent<TEvS3Provider::TEvContinue>(&TS3ReadCoroImpl::ProcessUnexpectedEvent);
                HandleEvent(*ev);
                StartCycleCount = GetCycleCountFast();
            }
            if (isCancelled) {
                LOG_CORO_D("RunClickHouseParserOverHttp - STOPPED ON SATURATION");
                break;
            }
        }

        LOG_CORO_D("RunClickHouseParserOverHttp - FINISHED");
    }

    void RunClickHouseParserOverFile() {

        LOG_CORO_D("RunClickHouseParserOverFile");

        TString fileName = Url.substr(7) + Path;

        std::unique_ptr<NDB::ReadBuffer> coroBuffer = AsyncDecompressing ? std::unique_ptr<NDB::ReadBuffer>(std::make_unique<TCoroDecompressorBuffer>(this)) : std::unique_ptr<NDB::ReadBuffer>(std::make_unique<NDB::ReadBufferFromFile>(fileName));
        std::unique_ptr<NDB::ReadBuffer> decompressorBuffer;
        NDB::ReadBuffer* buffer = coroBuffer.get();

        if (ReadSpec->Compression && !AsyncDecompressing) {
            decompressorBuffer = MakeDecompressor(*buffer, ReadSpec->Compression);
            YQL_ENSURE(decompressorBuffer, "Unsupported " << ReadSpec->Compression << " compression.");
            buffer = decompressorBuffer.get();
        }

        auto stream = std::make_unique<NDB::InputStreamFromInputFormat>(
            NDB::FormatFactory::instance().getInputFormat(
                ReadSpec->Format, *buffer, NDB::Block(ReadSpec->CHColumns), nullptr, ReadActorFactoryCfg.RowsInBatch, ReadSpec->Settings
            )
        );

        while (NDB::Block batch = stream->read()) {
            Paused = SourceContext->Add(batch.bytes(), SelfActorId);
            const bool isCancelled = StopIfConsumedEnough(batch.rows());
            Send(ParentActorId, new TEvS3Provider::TEvNextBlock(batch, PathIndex, TakeIngressDelta(), TakeCpuTimeDelta(), ReadSpec->Compression ? TakeIngressDecompressedDelta(buffer->count()) : 0ULL));
            if (Paused) {
                CpuTime += GetCpuTimeDelta();
                auto ev = WaitForSpecificEvent<TEvS3Provider::TEvContinue>(&TS3ReadCoroImpl::ProcessUnexpectedEvent);
                HandleEvent(*ev);
                StartCycleCount = GetCycleCountFast();
            }
            if (isCancelled) {
                LOG_CORO_D("RunClickHouseParserOverFile STOPPED ON SATURATION");
                break;
            }
        }
        IngressBytes += GetFileLength(fileName);

        LOG_CORO_D("RunClickHouseParserOverFile FINISHED");
    }

    struct TReadCache {
        ui64 Cookie = 0;
        TString Data;
        std::optional<ui64> RowGroupIndex;
        bool Ready = false;
    };

    struct TReadRangeCompare
    {
        bool operator() (const TEvS3Provider::TReadRange& lhs, const TEvS3Provider::TReadRange& rhs) const
        {
            return (lhs.Offset < rhs.Offset) || (lhs.Offset == rhs.Offset && lhs.Length < rhs.Length);
        }
    };

    ui64 RangeCookie = 0;
    std::map<TEvS3Provider::TReadRange, TReadCache, TReadRangeCompare> RangeCache;
    std::map<ui64, ui64> ReadInflightSize;
    std::optional<ui64> CurrentRowGroupIndex;
    std::map<ui64, ui64> RowGroupRangeInflight;
    std::priority_queue<ui64, std::vector<ui64>, std::greater<ui64>> ReadyRowGroups;
    std::map<ui64, ui64> RowGroupReaderIndex;

    static void OnResult(TActorSystem* actorSystem, TActorId selfId, TEvS3Provider::TReadRange range, ui64 cookie, IHTTPGateway::TResult&& result) {
        if (!result.Issues) {
            actorSystem->Send(new IEventHandle(selfId, TActorId{}, new TEvS3Provider::TEvReadResult2(range, std::move(result.Content)), 0, cookie));
        } else {
            actorSystem->Send(new IEventHandle(selfId, TActorId{}, new TEvS3Provider::TEvReadResult2(range, std::move(result.Issues)), 0, cookie));
        }
    }

    ui64 DecreaseRowGroupInflight(ui64 rowGroupIndex) {
        auto inflight = RowGroupRangeInflight[rowGroupIndex];
        if (inflight > 1) {
            RowGroupRangeInflight[rowGroupIndex] = --inflight;
        } else {
            inflight = 0;
            RowGroupRangeInflight.erase(rowGroupIndex);
        }
        return inflight;
    }


    TReadCache& GetOrCreate(TEvS3Provider::TReadRange range) {
        auto it = RangeCache.find(range);
        if (it != RangeCache.end()) {
            return it->second;
        }
        RetryStuff->Gateway->Download(RetryStuff->Url, RetryStuff->Headers,
                            range.Offset,
                            range.Length,
                            std::bind(&OnResult, GetActorSystem(), SelfActorId, range, ++RangeCookie, std::placeholders::_1),
                            {},
                            RetryStuff->RetryPolicy);
        LOG_CORO_D("Download STARTED [" << range.Offset << "-" << range.Length << "], cookie: " << RangeCookie);
        auto& result = RangeCache[range];
        if (result.Cookie) {
            // may overwrite old range in case of desync?
            if (result.RowGroupIndex) {
                LOG_CORO_W("RangeInfo DISCARDED [" << range.Offset << "-" << range.Length << "], cookie: " << RangeCookie << ", rowGroup " << *result.RowGroupIndex);
                DecreaseRowGroupInflight(*result.RowGroupIndex);
            }
        }
        result.RowGroupIndex = CurrentRowGroupIndex;
        result.Cookie = RangeCookie;
        if (CurrentRowGroupIndex) {
            RowGroupRangeInflight[*CurrentRowGroupIndex]++;
        }
        return result;
    }

    arrow::Status WillNeed(const std::vector<arrow::io::ReadRange>& readRanges) {
        if (readRanges.empty()) { // select count(*) case
            if (CurrentRowGroupIndex) {
                ReadyRowGroups.push(*CurrentRowGroupIndex);
            }
        } else {
            for (auto& range : readRanges) {
                GetOrCreate(TEvS3Provider::TReadRange{ .Offset = range.offset, .Length = range.length });
            }
        }
        return {};
    }

    void HandleEvent(TEvS3Provider::TEvReadResult2::THandle& event) {

        if (event.Get()->Failure) {
            throw yexception() << event.Get()->Issues.ToOneLineString();
        }
        auto readyRange = event.Get()->ReadRange;
        LOG_CORO_D("Download FINISHED [" << readyRange.Offset << "-" << readyRange.Length << "], cookie: " << event.Cookie);
        IngressBytes += readyRange.Length;

        auto it = RangeCache.find(readyRange);

        if (it == RangeCache.end()) {
            LOG_CORO_W("Download completed for unknown/discarded range [" << readyRange.Offset << "-" << readyRange.Length << "]");
            return;
        }

        if (it->second.Cookie != event.Cookie) {
            LOG_CORO_W("Mistmatched cookie for range [" << readyRange.Offset << "-" << readyRange.Length << "], received " << event.Cookie << ", expected " << it->second.Cookie);
            return;
        }

        it->second.Data = event.Get()->Result.Extract();
        ui64 size = it->second.Data.size();
        it->second.Ready = true;
        if (it->second.RowGroupIndex) {
            if (!DecreaseRowGroupInflight(*it->second.RowGroupIndex)) {
                LOG_CORO_D("RowGroup #" << *it->second.RowGroupIndex << " is READY");
                ReadyRowGroups.push(*it->second.RowGroupIndex);
            }
            ReadInflightSize[*it->second.RowGroupIndex] += size;
            if (RawInflightSize) {
                RawInflightSize->Add(size);
            }
        }
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) {

        LOG_CORO_D("ReadAt STARTED [" << position << "-" << nbytes << "]");
        TEvS3Provider::TReadRange range { .Offset = position, .Length = nbytes };
        auto& cache = GetOrCreate(range);

        CpuTime += GetCpuTimeDelta();

        while (!cache.Ready) {
            auto ev = WaitForSpecificEvent<TEvS3Provider::TEvReadResult2>(&TS3ReadCoroImpl::ProcessUnexpectedEvent);
            HandleEvent(*ev);
        }

        StartCycleCount = GetCycleCountFast();

        TString data = cache.Data;
        RangeCache.erase(range);

        LOG_CORO_D("ReadAt FINISHED [" << position << "-" << nbytes << "] #" << data.size());

        return arrow::Buffer::FromString(data);
    }

    void RunCoroBlockArrowParserOverHttp() {

        LOG_CORO_D("RunCoroBlockArrowParserOverHttp");

        ui64 readerCount = 1;

        std::vector<std::unique_ptr<parquet::arrow::FileReader>> readers;

        parquet::arrow::FileReaderBuilder builder;
        builder.memory_pool(arrow::default_memory_pool());
        parquet::ArrowReaderProperties properties;
        properties.set_cache_options(arrow::io::CacheOptions::LazyDefaults());
        properties.set_pre_buffer(true);
        builder.properties(properties);

        // init the 1st reader, get meta/rg count
        readers.resize(1);
        THROW_ARROW_NOT_OK(builder.Open(std::make_shared<THttpRandomAccessFile>(this, RetryStuff->SizeLimit)));
        THROW_ARROW_NOT_OK(builder.Build(&readers[0]));
        auto fileMetadata = readers[0]->parquet_reader()->metadata();
        ui64 numGroups = readers[0]->num_row_groups();

        if (numGroups) {

            std::shared_ptr<arrow::Schema> schema;
            THROW_ARROW_NOT_OK(readers[0]->GetSchema(&schema));
            std::vector<int> columnIndices;
            std::vector<TColumnConverter> columnConverters;

            BuildColumnConverters(ReadSpec->ArrowSchema, schema, columnIndices, columnConverters, ReadSpec->RowSpec, ReadSpec->Settings);

            // select count(*) case - single reader is enough
            if (!columnIndices.empty()) {
                if (ReadSpec->ParallelRowGroupCount) {
                    readerCount = ReadSpec->ParallelRowGroupCount;
                } else {
                    // we want to read in parallel as much as 1/2 of fair share bytes
                    // (it's compressed size, after decoding it will grow)
                    ui64 compressedSize = 0;
                    for (int i = 0; i < fileMetadata->num_row_groups(); i++) {
                        auto rowGroup = fileMetadata->RowGroup(i);
                        for (const auto columIndex : columnIndices) {
                            compressedSize += rowGroup->ColumnChunk(columIndex)->total_compressed_size();
                        }
                    }
                    // count = (fair_share / 2) / (compressed_size / num_group)
                    auto desiredReaderCount = (SourceContext->FairShare() * numGroups) / (compressedSize * 2);
                    // min is 1
                    // max is 5 (should be also tuned probably)
                    if (desiredReaderCount) {
                        readerCount = std::min(desiredReaderCount, 5ul);
                    }
                }
                if (readerCount > numGroups) {
                    readerCount = numGroups;
                }
            }

            if (readerCount > 1) {
                // init other readers if any
                readers.resize(readerCount);
                for (ui64 i = 1; i < readerCount; i++) {
                    THROW_ARROW_NOT_OK(builder.Open(std::make_shared<THttpRandomAccessFile>(this, RetryStuff->SizeLimit),
                                    parquet::default_reader_properties(),
                                    fileMetadata));
                    THROW_ARROW_NOT_OK(builder.Build(&readers[i]));
                }
            }

            for (ui64 i = 0; i < readerCount; i++) {
                if (!columnIndices.empty()) {
                    CurrentRowGroupIndex = i;
                    THROW_ARROW_NOT_OK(readers[i]->WillNeedRowGroups({ static_cast<int>(i) }, columnIndices));
                    SourceContext->IncChunkCount();
                }
                RowGroupReaderIndex[i] = i;
            }

            ui64 nextGroup = readerCount;
            ui64 readyGroupCount = 0;

            while (readyGroupCount < numGroups) {
                if (Paused) {
                    CpuTime += GetCpuTimeDelta();
                    auto ev = WaitForSpecificEvent<TEvS3Provider::TEvContinue>(&TS3ReadCoroImpl::ProcessUnexpectedEvent);
                    HandleEvent(*ev);
                    StartCycleCount = GetCycleCountFast();
                }

                ui64 readyGroupIndex;
                if (!columnIndices.empty()) {
                    CpuTime += GetCpuTimeDelta();

                    // if reordering is not allowed wait for row groups sequentially
                    while (ReadyRowGroups.empty()
                            || (!ReadSpec->RowGroupReordering && ReadyRowGroups.top() > readyGroupCount) ) {
                        ProcessOneEvent();
                    }

                    StartCycleCount = GetCycleCountFast();

                    readyGroupIndex = ReadyRowGroups.top();
                    ReadyRowGroups.pop();
                } else {
                    // select count(*) case - no columns, no download, just fetch meta info instantly
                    readyGroupIndex = readyGroupCount;
                }
                SourceContext->DecChunkCount();
                auto readyReaderIndex = RowGroupReaderIndex[readyGroupIndex];
                RowGroupReaderIndex.erase(readyGroupIndex);

                std::shared_ptr<arrow::Table> table;

                LOG_CORO_D("Decode RowGroup " << readyGroupIndex << " of " << numGroups << " from reader " << readyReaderIndex);
                THROW_ARROW_NOT_OK(readers[readyReaderIndex]->DecodeRowGroups({ static_cast<int>(readyGroupIndex) }, columnIndices, &table));
                readyGroupCount++;

                auto downloadedBytes = ReadInflightSize[readyGroupIndex];
                ui64 decodedBytes = 0;
                ReadInflightSize.erase(readyGroupIndex);

                auto reader = std::make_unique<arrow::TableBatchReader>(*table);

                std::shared_ptr<arrow::RecordBatch> batch;
                arrow::Status status;
                bool isCancelled = false;
                ui64 numRows = 0;
                while (status = reader->ReadNext(&batch), status.ok() && batch) {
                    auto convertedBatch = ConvertArrowColumns(batch, columnConverters);
                    auto size = NUdf::GetSizeOfArrowBatchInBytes(*convertedBatch);
                    decodedBytes += size;
                    Paused = SourceContext->Add(size, SelfActorId, Paused);
                    Send(ParentActorId, new TEvS3Provider::TEvNextRecordBatch(
                        convertedBatch, PathIndex, TakeIngressDelta(), TakeCpuTimeDelta()
                    ));
                    numRows += convertedBatch->num_rows();
                }
                if (StopIfConsumedEnough(numRows)) {
                    isCancelled = true;
                }
                if (!status.ok()) {
                    throw yexception() << status.ToString();
                }
                SourceContext->UpdateProgress(downloadedBytes, decodedBytes, table->num_rows());
                if (RawInflightSize) {
                    RawInflightSize->Sub(downloadedBytes);
                }
                if (nextGroup < numGroups) {
                    if (!columnIndices.empty()) {
                        CurrentRowGroupIndex = nextGroup;
                        THROW_ARROW_NOT_OK(readers[readyReaderIndex]->WillNeedRowGroups({ static_cast<int>(nextGroup) }, columnIndices));
                        SourceContext->IncChunkCount();
                    }
                    RowGroupReaderIndex[nextGroup] = readyReaderIndex;
                    nextGroup++;
                } else {
                    readers[readyReaderIndex].reset();
                }
                if (isCancelled) {
                    LOG_CORO_D("RunCoroBlockArrowParserOverHttp - STOPPED ON SATURATION, downloaded " <<
                               SourceContext->GetDownloadedBytes() << " bytes");
                    break;        
                }
            }
        }

        LOG_CORO_D("RunCoroBlockArrowParserOverHttp - FINISHED");
    }

    void RunCoroBlockArrowParserOverFile() {

        LOG_CORO_D("RunCoroBlockArrowParserOverFile");

        std::shared_ptr<arrow::io::RandomAccessFile> arrowFile =
            std::make_shared<TRandomAccessFileTrafficCounter>(this,
                arrow::io::ReadableFile::Open((Url + Path).substr(7), arrow::default_memory_pool()).ValueOrDie()
            );
        std::unique_ptr<parquet::arrow::FileReader> fileReader;
        parquet::arrow::FileReaderBuilder builder;
        builder.memory_pool(arrow::default_memory_pool());
        parquet::ArrowReaderProperties properties;
        properties.set_cache_options(arrow::io::CacheOptions::LazyDefaults());
        properties.set_pre_buffer(true);
        builder.properties(properties);
        THROW_ARROW_NOT_OK(builder.Open(arrowFile));
        THROW_ARROW_NOT_OK(builder.Build(&fileReader));

        std::shared_ptr<arrow::Schema> schema;
        THROW_ARROW_NOT_OK(fileReader->GetSchema(&schema));
        std::vector<int> columnIndices;
        std::vector<TColumnConverter> columnConverters;

        BuildColumnConverters(ReadSpec->ArrowSchema, schema, columnIndices, columnConverters, ReadSpec->RowSpec, ReadSpec->Settings);

        for (int group = 0; group < fileReader->num_row_groups(); group++) {

            if (Paused) {
                CpuTime += GetCpuTimeDelta();
                LOG_CORO_D("RunCoroBlockArrowParserOverFile - PAUSED " << SourceContext->GetValue());
                auto ev = WaitForSpecificEvent<TEvS3Provider::TEvContinue>(&TS3ReadCoroImpl::ProcessUnexpectedEvent);
                HandleEvent(*ev);
                LOG_CORO_D("RunCoroBlockArrowParserOverFile - CONTINUE " << SourceContext->GetValue());
                StartCycleCount = GetCycleCountFast();
            }

            std::shared_ptr<arrow::Table> table;
            ui64 ingressBytes = IngressBytes;
            THROW_ARROW_NOT_OK(fileReader->ReadRowGroup(group, columnIndices, &table));
            ui64 downloadedBytes = IngressBytes - ingressBytes;
            auto reader = std::make_unique<arrow::TableBatchReader>(*table);

            ui64 decodedBytes = 0;
            std::shared_ptr<arrow::RecordBatch> batch;
            ::arrow::Status status;
            bool isCancelled = false;
            ui64 numRows = 0;
            while (status = reader->ReadNext(&batch), status.ok() && batch) {
                auto convertedBatch = ConvertArrowColumns(batch, columnConverters);
                auto size = NUdf::GetSizeOfArrowBatchInBytes(*convertedBatch);
                decodedBytes += size;
                Paused = SourceContext->Add(size, SelfActorId, Paused);
                Send(ParentActorId, new TEvS3Provider::TEvNextRecordBatch(
                    convertedBatch, PathIndex, TakeIngressDelta(), TakeCpuTimeDelta()
                ));
                numRows += batch->num_rows();
            }
            if (StopIfConsumedEnough(numRows)) {
                isCancelled = true;
            }
            if (!status.ok()) {
                throw yexception() << status.ToString();
            }
            SourceContext->UpdateProgress(downloadedBytes, decodedBytes, table->num_rows());
            if (isCancelled) {
                LOG_CORO_D("RunCoroBlockArrowParserOverFile - STOPPED ON SATURATION");
                break;
            }
        }

        LOG_CORO_D("RunCoroBlockArrowParserOverFile - FINISHED");
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvS3Provider::TEvDownloadStart, Handle);
        hFunc(TEvS3Provider::TEvDownloadData, Handle);
        hFunc(TEvS3Provider::TEvDownloadFinish, Handle);
        hFunc(TEvS3Provider::TEvDecompressDataResult, Handle);
        hFunc(TEvS3Provider::TEvDecompressDataFinish, Handle);
        hFunc(TEvS3Provider::TEvContinue, Handle);
        hFunc(TEvS3Provider::TEvReadResult2, Handle);
        hFunc(NActors::TEvents::TEvPoison, Handle);
    )

    void ProcessOneEvent() {
        if (!Paused && !DeferredDataParts.empty()) {
            ExtractDataPart(*DeferredDataParts.front(), true);
            DeferredDataParts.pop();
            if (DeferredQueueSize) {
                DeferredQueueSize->Dec();
            }
            return;
        }
        TAutoPtr<::NActors::IEventHandle> ev(WaitForEvent().Release());
        StateFunc(ev);
    }

    void ExtractDataPart(TEvS3Provider::TEvDownloadData& event, bool deferred = false) {
        InputBuffer = event.Result.Extract();
        IngressBytes += InputBuffer.size();
        RetryStuff->Offset += InputBuffer.size();
        RetryStuff->SizeLimit -= InputBuffer.size();
        LastOffset = RetryStuff->Offset;
        LastData = InputBuffer;
        LOG_CORO_T("TEvDownloadData (" << (deferred ? "deferred" : "instant") << "), size: " << InputBuffer.size());
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
    }

    TString ExtractDecompressedDataPart() {
        if (!DeferredDecompressedDataParts.empty()) {
            auto result = std::move(DeferredDecompressedDataParts.front());
            DeferredDecompressedDataParts.pop();
            if (result->Exception) {
                throw result->Exception;
            }
            return result->Data;
        }
        return {};
    }

    void Handle(TEvS3Provider::TEvDownloadStart::TPtr& ev) {
        HttpResponseCode = ev->Get()->HttpResponseCode;
        CurlResponseCode = ev->Get()->CurlResponseCode;
        LOG_CORO_D("TEvDownloadStart, Http code: " << HttpResponseCode);
    }

    void Handle(TEvS3Provider::TEvDownloadData::TPtr& ev) {
        if (HttpDataRps) {
            HttpDataRps->Inc();
        }
        if (200L == HttpResponseCode || 206L == HttpResponseCode) {
            if (Paused || !DeferredDataParts.empty()) {
                DeferredDataParts.push(std::move(ev->Release()));
                if (DeferredQueueSize) {
                    DeferredQueueSize->Inc();
                }
            } else {
                ExtractDataPart(*ev->Get(), false);
            }
        } else if (HttpResponseCode && !RetryStuff->IsCancelled() && !RetryStuff->NextRetryDelay) {
            ServerReturnedError = true;
            if (ErrorText.size() < 256_KB)
                ErrorText.append(ev->Get()->Result.Extract());
            else if (!ErrorText.EndsWith(TruncatedSuffix))
                ErrorText.append(TruncatedSuffix);
            LOG_CORO_W("TEvDownloadData, ERROR: " << ErrorText << ", LastOffset: " << LastOffset << ", LastData: " << GetLastDataAsText());
        }
    }

    void Handle(TEvS3Provider::TEvDecompressDataResult::TPtr& ev) {
        CpuTime += ev->Get()->CpuTime;
        DeferredDecompressedDataParts.push(std::move(ev->Release()));
        
    }

    void Handle(TEvS3Provider::TEvDecompressDataFinish::TPtr& ev) {
        CpuTime += ev->Get()->CpuTime;
        DecompressedInputFinished = true;
    }

    void Handle(TEvS3Provider::TEvDownloadFinish::TPtr& ev) {

        if (CurlResponseCode == CURLE_OK) {
            CurlResponseCode = ev->Get()->CurlResponseCode;
        }

        Issues.Clear();
        if (!ErrorText.empty()) {
            TString errorCode;
            TString message;
            if (!ParseS3ErrorResponse(ErrorText, errorCode, message)) {
                message = ErrorText;
            }
            Issues.AddIssues(BuildIssues(HttpResponseCode, errorCode, message));
        }

        if (ev->Get()->Issues) {
            Issues.AddIssues(ev->Get()->Issues);
        }

        if (HttpResponseCode >= 300) {
            ServerReturnedError = true;
            Issues.AddIssue(TIssue{TStringBuilder() << "HTTP error code: " << HttpResponseCode});
        }

        if (Issues) {
            RetryStuff->NextRetryDelay = RetryStuff->GetRetryState()->GetNextRetryDelay(CurlResponseCode, HttpResponseCode);
            LOG_CORO_D("TEvDownloadFinish with Issues (try to retry): " << Issues.ToOneLineString());
            if (RetryStuff->NextRetryDelay) {
                // inplace retry: report problem to TransientIssues and repeat
                Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, Issues, NYql::NDqProto::StatusIds::UNSPECIFIED));
            } else {
                // can't retry here: fail download
                RetryStuff->RetryState = nullptr;
                InputFinished = true;
                FinishDecompressor();
                LOG_CORO_W("ReadError: " << Issues.ToOneLineString() << ", LastOffset: " << LastOffset << ", LastData: " << GetLastDataAsText());
                throw TS3ReadError(); // Don't pass control to data parsing, because it may validate eof and show wrong issues about incorrect data format
            }
        }

        if (!RetryStuff->IsCancelled() && RetryStuff->NextRetryDelay && RetryStuff->SizeLimit > 0ULL) {
            GetActorSystem()->Schedule(*RetryStuff->NextRetryDelay, new IEventHandle(ParentActorId, SelfActorId, new TEvS3Provider::TEvRetryEventFunc(std::bind(&DownloadStart, RetryStuff, GetActorSystem(), SelfActorId, ParentActorId, PathIndex, HttpInflightSize))));
            InputBuffer.clear();
            if (DeferredDataParts.size()) {
                if (DeferredQueueSize) {
                    DeferredQueueSize->Sub(DeferredDataParts.size());
                }
                std::queue<THolder<TEvS3Provider::TEvDownloadData>> tmp;
                DeferredDataParts.swap(tmp);
            }
        } else {
            LOG_CORO_D("TEvDownloadFinish, LastOffset: " << LastOffset << ", Error: " << ServerReturnedError);
            InputFinished = true;
            FinishDecompressor();
            if (ServerReturnedError) {
                throw TS3ReadError(); // Don't pass control to data parsing, because it may validate eof and show wrong issues about incorrect data format
            }
        }
    }

    void HandleEvent(TEvS3Provider::TEvContinue::THandle&) {
        LOG_CORO_D("TEvContinue");
        Paused = false;
    }

    void Handle(TEvS3Provider::TEvContinue::TPtr& ev) {
        HandleEvent(*ev);
    }

    void Handle(TEvS3Provider::TEvReadResult2::TPtr& ev) {
        HandleEvent(*ev);
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr&) {
        LOG_CORO_D("TEvPoison");
        RetryStuff->Cancel();
        FinishDecompressor(true);
        throw TS3ReadAbort();
    }

    void FinishDecompressor(bool force = false) {
        if (AsyncDecompressing) {
            Send(DecompressorActorId, new NActors::TEvents::TEvPoison(), 0, force);
        }
    }

private:
    static constexpr std::string_view TruncatedSuffix = "... [truncated]"sv;
public:
    TS3ReadCoroImpl(ui64 inputIndex, const TTxId& txId, const NActors::TActorId& computeActorId,
        const TRetryStuff::TPtr& retryStuff, const TReadSpec::TPtr& readSpec, size_t pathIndex,
        const TString& path, const TString& url, std::optional<ui64> maxRows,
        const TS3ReadActorFactoryConfig& readActorFactoryCfg,
        TSourceContext::TPtr queueBufferCounter,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& deferredQueueSize,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& httpInflightSize,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& httpDataRps,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& rawInflightSize,
        bool asyncDecompressing)
        : TActorCoroImpl(256_KB), ReadActorFactoryCfg(readActorFactoryCfg), InputIndex(inputIndex),
        TxId(txId), RetryStuff(retryStuff), ReadSpec(readSpec), ComputeActorId(computeActorId),
        PathIndex(pathIndex), Path(path), Url(url), RowsRemained(maxRows),
        SourceContext(queueBufferCounter),
        DeferredQueueSize(deferredQueueSize), HttpInflightSize(httpInflightSize),
        HttpDataRps(httpDataRps), RawInflightSize(rawInflightSize), AsyncDecompressing(asyncDecompressing) {
    }

    ~TS3ReadCoroImpl() override {
        if (DeferredDataParts.size() && DeferredQueueSize) {
            DeferredQueueSize->Sub(DeferredDataParts.size());
        }
        auto rawInflightSize = 0;
        for (auto it : ReadInflightSize) {
            rawInflightSize += it.second;
        }
        if (rawInflightSize && RawInflightSize) {
            RawInflightSize->Sub(rawInflightSize);
        }
        ReadInflightSize.clear();
    }

private:
    ui64 TakeIngressDelta() {
        auto currentIngressBytes = IngressBytes;
        IngressBytes = 0;
        return currentIngressBytes;
    }

    ui64 TakeIngressDecompressedDelta(ui64 current) {
        ui64 delta = current - TotalIngressDecompressedBytes;
        TotalIngressDecompressedBytes = current;
        return delta;
    }

    TDuration TakeCpuTimeDelta() {
        auto currentCpuTime = CpuTime;
        CpuTime = TDuration::Zero();
        return currentCpuTime;
    }

    TDuration GetCpuTimeDelta() {
        return TDuration::Seconds(NHPTimer::GetSeconds(GetCycleCountFast() - StartCycleCount));
    }

    bool StopIfConsumedEnough(ui64 consumedRows) {
        if (!RowsRemained) {
            return false;
        }

        *RowsRemained = SubtractSaturating(*RowsRemained, consumedRows);
        if (*RowsRemained > 0) {
            return false;
        }

        RetryStuff->Cancel();
        return true;
    }

    void Run() final {
        if (AsyncDecompressing) {
            DecompressorActorId = Register(CreateS3DecompressorActor(SelfActorId, ReadSpec->Compression));
        }

        NYql::NDqProto::StatusIds::StatusCode fatalCode = NYql::NDqProto::StatusIds::EXTERNAL_ERROR;

        StartCycleCount = GetCycleCountFast();

        try {
            if (ReadSpec->Arrow) {
                if (ReadSpec->Compression) {
                    Issues.AddIssue(TIssue("Blocks optimisations are incompatible with external compression"));
                    fatalCode = NYql::NDqProto::StatusIds::BAD_REQUEST;
                } else {
                    try {
                        if (Url.StartsWith("file://")) {
                            RunCoroBlockArrowParserOverFile();
                        } else {
                            RunCoroBlockArrowParserOverHttp();
                        }
                    } catch (const parquet::ParquetException& ex) {
                        Issues.AddIssue(TIssue(ex.what()));
                        fatalCode = NYql::NDqProto::StatusIds::BAD_REQUEST;
                        RetryStuff->Cancel();
                    }
                }
            } else {
                try {
                    if (Url.StartsWith("file://")) {
                        RunClickHouseParserOverFile();
                    } else {
                        RunClickHouseParserOverHttp();
                    }
                } catch (const TS3ReadError&) {
                    // Just to avoid parser error after transport failure
                    LOG_CORO_D("S3 read ERROR");
                } catch (const NDB::Exception& ex) {
                    Issues.AddIssue(TIssue(ex.message()));
                    fatalCode = NYql::NDqProto::StatusIds::BAD_REQUEST;
                    RetryStuff->Cancel();
                }
            }
        } catch (const TS3ReadAbort&) {
            // Poison handler actually
            LOG_CORO_D("S3 read ABORT");
        } catch (const TDtorException&) {
            // Stop any activity instantly
            RetryStuff->Cancel();
            return;
        } catch (const std::exception& err) {
            Issues.AddIssue(TIssue(err.what()));
            fatalCode = NYql::NDqProto::StatusIds::INTERNAL_ERROR;
            RetryStuff->Cancel();
        }

        CpuTime += GetCpuTimeDelta();

        auto issues = NS3Util::AddParentIssue(TStringBuilder{} << "Error while reading file " << Path, std::move(Issues));
        if (issues)
            Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, std::move(issues), fatalCode));
        else
            Send(ParentActorId, new TEvS3Provider::TEvFileFinished(PathIndex, TakeIngressDelta(), TakeCpuTimeDelta(), RetryStuff->SizeLimit));
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
        return StateFunc(ev);
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
    bool DecompressedInputFinished = false;
    long HttpResponseCode = 0L;
    CURLcode CurlResponseCode = CURLE_OK;
    bool ServerReturnedError = false;
    TString ErrorText;
    TIssues Issues;

    NActors::TActorId DecompressorActorId;
    std::size_t LastOffset = 0;
    TString LastData;
    ui64 IngressBytes = 0;
    ui64 TotalIngressDecompressedBytes = 0;
    TDuration CpuTime;
    ui64 StartCycleCount = 0;
    TString InputBuffer;
    std::optional<ui64> RowsRemained;
    bool Paused = false;
    std::queue<THolder<TEvS3Provider::TEvDownloadData>> DeferredDataParts;
    std::queue<THolder<TEvS3Provider::TEvDecompressDataResult>> DeferredDecompressedDataParts;
    TSourceContext::TPtr SourceContext;
    const ::NMonitoring::TDynamicCounters::TCounterPtr DeferredQueueSize;
    const ::NMonitoring::TDynamicCounters::TCounterPtr HttpInflightSize;
    const ::NMonitoring::TDynamicCounters::TCounterPtr HttpDataRps;
    const ::NMonitoring::TDynamicCounters::TCounterPtr RawInflightSize;
    const bool AsyncDecompressing;
};

class TS3ReadCoroActor : public TActorCoro {
public:
    TS3ReadCoroActor(THolder<TS3ReadCoroImpl> impl)
        : TActorCoro(THolder<TActorCoroImpl>(impl.Release()))
    {}
private:
    void Registered(TActorSystem* actorSystem, const TActorId& parent) override {
        TActorCoro::Registered(actorSystem, parent); // Calls TActorCoro::OnRegister and sends bootstrap event to ourself.
    }
};

class TS3StreamReadActor : public TActorBootstrapped<TS3StreamReadActor>, public IDqComputeActorAsyncInput {
public:
    TS3StreamReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        IHTTPGateway::TPtr gateway,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const TString& url,
        const TS3Credentials& credentials,
        const TString& pattern,
        ES3PatternVariant patternVariant,
        TPathList&& paths,
        bool addPathIndex,
        const TReadSpec::TPtr& readSpec,
        const NActors::TActorId& computeActorId,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        const TS3ReadActorFactoryConfig& readActorFactoryCfg,
        ::NMonitoring::TDynamicCounterPtr counters,
        ::NMonitoring::TDynamicCounterPtr taskCounters,
        ui64 fileSizeLimit,
        ui64 readLimit,
        std::optional<ui64> rowsLimitHint,
        IMemoryQuotaManager::TPtr memoryQuotaManager,
        bool useRuntimeListing,
        TActorId fileQueueActor,
        ui64 fileQueueBatchSizeLimit,
        ui64 fileQueueBatchObjectCountLimit,
        ui64 fileQueueConsumersCountDelta,
        bool asyncDecoding,
        bool asyncDecompressing
    )   : ReadActorFactoryCfg(readActorFactoryCfg)
        , Gateway(std::move(gateway))
        , HolderFactory(holderFactory)
        , InputIndex(inputIndex)
        , TxId(txId)
        , ComputeActorId(computeActorId)
        , RetryPolicy(retryPolicy)
        , Url(url)
        , Credentials(credentials)
        , Pattern(pattern)
        , PatternVariant(patternVariant)
        , Paths(std::move(paths))
        , AddPathIndex(addPathIndex)
        , RowsRemained(rowsLimitHint)
        , ReadSpec(readSpec)
        , Counters(std::move(counters))
        , TaskCounters(std::move(taskCounters))
        , FileQueueActor(fileQueueActor)
        , FileSizeLimit(fileSizeLimit)
        , ReadLimit(readLimit)
        , MemoryQuotaManager(memoryQuotaManager)
        , UseRuntimeListing(useRuntimeListing)
        , FileQueueBatchSizeLimit(fileQueueBatchSizeLimit)
        , FileQueueBatchObjectCountLimit(fileQueueBatchObjectCountLimit)
        , FileQueueConsumersCountDelta(fileQueueConsumersCountDelta)
        , AsyncDecoding(asyncDecoding)
        , AsyncDecompressing(asyncDecompressing) {
        if (Counters) {
            QueueDataSize = Counters->GetCounter("QueueDataSize");
            QueueDataLimit = Counters->GetCounter("QueueDataLimit");
            QueueBlockCount = Counters->GetCounter("QueueBlockCount");
            DownloadCount = Counters->GetCounter("DownloadCount");
            DownloadPaused = Counters->GetCounter("DownloadPaused");
            QueueDataLimit->Add(ReadActorFactoryCfg.DataInflight);
            DecodedChunkSizeHist = Counters->GetHistogram("ChunkSizeBytes", NMonitoring::ExplicitHistogram({100,1000,10'000,30'000,100'000,300'000,1'000'000,3'000'000,10'000'000,30'000'000,100'000'000}));
        }
        if (TaskCounters) {
            TaskQueueDataSize = TaskCounters->GetCounter("QueueDataSize");
            TaskQueueDataLimit = TaskCounters->GetCounter("QueueDataLimit");
            TaskDownloadCount = TaskCounters->GetCounter("DownloadCount");
            TaskChunkDownloadCount = TaskCounters->GetCounter("ChunkDownloadCount");
            TaskDownloadPaused = TaskCounters->GetCounter("DownloadPaused");
            DeferredQueueSize = TaskCounters->GetCounter("DeferredQueueSize");
            HttpInflightSize = TaskCounters->GetCounter("HttpInflightSize");
            HttpInflightLimit = TaskCounters->GetCounter("HttpInflightLimit");
            HttpDataRps = TaskCounters->GetCounter("HttpDataRps", true);
            TaskQueueDataLimit->Add(ReadActorFactoryCfg.DataInflight);
            RawInflightSize = TaskCounters->GetCounter("RawInflightSize");
        }
        IngressStats.Level = statsLevel;
    }

    void Bootstrap() {
        LOG_D("TS3StreamReadActor", "Bootstrap");

        // Arrow blocks are currently not limited by mem quoter, so we use rough buffer quotation
        // After exact mem control implementation, this allocation should be deleted
        if (!MemoryQuotaManager->AllocateQuota(ReadActorFactoryCfg.DataInflight)) {
            TIssues issues;
            issues.AddIssue(TIssue{TStringBuilder() << "OutOfMemory - can't allocate read buffer"});
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), NYql::NDqProto::StatusIds::OVERLOADED));
            return;
        }

        SourceContext = std::make_shared<TSourceContext>(
            SelfId(),
            ReadActorFactoryCfg.DataInflight,
            TActivationContext::ActorSystem(),
            QueueDataSize,
            TaskQueueDataSize,
            DownloadCount,
            DownloadPaused,
            TaskDownloadCount,
            TaskDownloadPaused,
            TaskChunkDownloadCount,
            DecodedChunkSizeHist,
            HttpInflightSize,
            HttpDataRps,
            DeferredQueueSize,
            ReadSpec->Format,
            ReadSpec->Compression,
            ReadSpec->ArrowSchema,
            ReadSpec->RowSpec,
            ReadSpec->Settings
        );

        if (!UseRuntimeListing) {
            FileQueueActor = RegisterWithSameMailbox(CreateS3FileQueueActor(
                TxId,
                std::move(Paths),
                ReadActorFactoryCfg.MaxInflight * 2,
                FileSizeLimit,
                ReadLimit,
                false,
                1,
                FileQueueBatchSizeLimit,
                FileQueueBatchObjectCountLimit,
                Gateway,
                RetryPolicy,
                Url,
                Credentials,
                Pattern,
                PatternVariant,
                ES3PatternType::Wildcard));
        }
        FileQueueEvents.Init(TxId, SelfId(), SelfId());
        FileQueueEvents.OnNewRecipientId(FileQueueActor);
        if (UseRuntimeListing && FileQueueConsumersCountDelta > 0) {
            FileQueueEvents.Send(new TEvS3Provider::TEvUpdateConsumersCount(FileQueueConsumersCountDelta));
        }
        SendPathBatchRequest();

        Become(&TS3StreamReadActor::StateFunc);
        Bootstrapped = true;
    }

    bool TryRegisterCoro() {
        TrySendPathBatchRequest();
        if (PathBatchQueue.empty()) {
            // no path is pending
            return false;
        }
        if (IsCurrentBatchEmpty) {
            // waiting for batch to finish
            return false;
        }
        if (SourceContext->IsFull()) {
            // too large data inflight
            return false;
        }

        auto splitCount = SourceContext->GetSplitCount();

        if (splitCount >= ReadActorFactoryCfg.MaxInflight) {
            // hard limit
            return false;
        }
        if (ReadSpec->ParallelDownloadCount && splitCount >= ReadSpec->ParallelDownloadCount) {
            // explicit limit
            return false;
        } 
        if (splitCount && DownloadSize * SourceContext->Ratio() > ReadActorFactoryCfg.DataInflight * 2) {
            // dynamic limit
            return false;
        }
        RegisterCoro();
        return true;
    }

    void RegisterCoro() {
        SourceContext->IncSplitCount();
        const auto& object = ReadPathFromCache();
        DownloadSize += object.GetSize();
        const TString requestId = CreateGuidAsString();
        auto pathIndex = object.GetPathIndex();
        if (TaskCounters) {
            HttpInflightLimit->Add(Gateway->GetBuffersSizePerStream());
        }
        LOG_D(
            "TS3StreamReadActor",
            "RegisterCoro with path " << object.GetPath() << " with pathIndex "
                                      << pathIndex);

        TActorId actorId;
        const auto& authInfo = Credentials.GetAuthInfo();
        auto stuff = std::make_shared<TRetryStuff>(
            Gateway,
            Url + object.GetPath(),
            IHTTPGateway::MakeYcHeaders(requestId, authInfo.GetToken(), {}, authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
            object.GetSize(),
            TxId,
            requestId,
            RetryPolicy);
        auto impl = MakeHolder<TS3ReadCoroImpl>(
            InputIndex,
            TxId,
            ComputeActorId,
            std::move(stuff),
            ReadSpec,
            pathIndex,
            object.GetPath(),
            Url,
            RowsRemained,
            ReadActorFactoryCfg,
            SourceContext,
            DeferredQueueSize,
            HttpInflightSize,
            HttpDataRps,
            RawInflightSize,
            AsyncDecompressing
        );
        if (AsyncDecoding) {
            actorId = Register(new TS3ReadCoroActor(std::move(impl)));
        } else {
            actorId = RegisterWithSameMailbox(new TS3ReadCoroActor(std::move(impl)));
        }

        CoroActors.insert(actorId);
    }

    NS3::FileQueue::TObjectPath ReadPathFromCache() {
        Y_ENSURE(!PathBatchQueue.empty());
        auto& currentBatch = PathBatchQueue.front();
        Y_ENSURE(!currentBatch.empty());
        auto object = currentBatch.back();
        currentBatch.pop_back();
        if (currentBatch.empty()) {
            PathBatchQueue.pop_front();
            IsCurrentBatchEmpty = true;
        }
        TrySendPathBatchRequest();
        return object;
    }
    void TrySendPathBatchRequest() {
        if (PathBatchQueue.size() < 2 && !IsFileQueueEmpty && !IsWaitingFileQueueResponse) {
            SendPathBatchRequest();
        }
    }
    void SendPathBatchRequest() {
        FileQueueEvents.Send(new TEvS3Provider::TEvGetNextBatch());
        IsWaitingFileQueueResponse = true;
    }

    static constexpr char ActorName[] = "S3_STREAM_READ_ACTOR";

private:
    class TBoxedBlock : public NKikimr::NMiniKQL::TComputationValue<TBoxedBlock> {
    public:
        TBoxedBlock(NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo, NDB::Block& block)
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
        TReadyBlock(TEvS3Provider::TEvNextBlock::TPtr& event) : PathInd(event->Get()->PathIndex) { Block.swap(event->Get()->Block); }
        TReadyBlock(TEvS3Provider::TEvNextRecordBatch::TPtr& event) : Batch(event->Get()->Batch), PathInd(event->Get()->PathIndex) {}
        NDB::Block Block;
        std::shared_ptr<arrow::RecordBatch> Batch;
        size_t PathInd;
    };

    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {}
    void LoadState(const NDqProto::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}

    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const final {
        return IngressStats;
    }

    TDuration GetCpuTime() override {
        return CpuTime;
    }

    ui64 GetBlockSize(const TReadyBlock& block) const {
        return ReadSpec->Arrow ? NUdf::GetSizeOfArrowBatchInBytes(*block.Batch) : block.Block.bytes();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& output, TMaybe<TInstant>&, bool& finished, i64 free) final {
        i64 total = 0LL;
        if (!Blocks.empty()) do {
            const i64 s = GetBlockSize(Blocks.front());

            NUdf::TUnboxedValue value;
            if (ReadSpec->Arrow) {
                const auto& batch = *Blocks.front().Batch;
// Cerr << "ASYNC batch with COLS=" << batch.num_columns() << " and ROWS=" << batch.num_rows() << Endl;
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
            SourceContext->Sub(s);
            if (Counters) {
                QueueBlockCount->Dec();
            }
            TryRegisterCoro();
        } while (!Blocks.empty() && free > 0LL && GetBlockSize(Blocks.front()) <= size_t(free));

        finished = (ConsumedEnoughRows() || LastFileWasProcessed()) && !FileQueueEvents.RemoveConfirmedEvents();
        if (finished) {
            ContainerCache.Clear();
            ArrowTupleContainerCache.Clear();
            ArrowRowContainerCache.Clear();
        } else if(!total) {
            IngressStats.TryPause();
        }
        return total;
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor

        if (Bootstrapped) {
            LOG_D("TS3StreamReadActor", "PassAway");
            if (Counters) {
                QueueBlockCount->Sub(Blocks.size());
                QueueDataLimit->Sub(ReadActorFactoryCfg.DataInflight);
            }
            if (TaskCounters) {
                TaskQueueDataLimit->Sub(ReadActorFactoryCfg.DataInflight);
                HttpInflightLimit->Sub(Gateway->GetBuffersSizePerStream() * CoroActors.size());
            }
            SourceContext.reset();

            for (const auto actorId : CoroActors) {
                Send(actorId, new NActors::TEvents::TEvPoison());
            }
            LOG_T("TS3StreamReadActor", "PassAway FileQueue RemoveConfirmedEvents=" << FileQueueEvents.RemoveConfirmedEvents());
            FileQueueEvents.Unsubscribe();

            ContainerCache.Clear();
            ArrowTupleContainerCache.Clear();
            ArrowRowContainerCache.Clear();

            MemoryQuotaManager->FreeQuota(ReadActorFactoryCfg.DataInflight);
        } else {
            LOG_W("TS3StreamReadActor", "PassAway w/o Bootstrap");
        }

        MemoryQuotaManager.reset();
        TActorBootstrapped<TS3StreamReadActor>::PassAway();
    }

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(TEvS3Provider::TEvRetryEventFunc, HandleRetry);
        hFunc(TEvS3Provider::TEvNextBlock, HandleNextBlock);
        hFunc(TEvS3Provider::TEvNextRecordBatch, HandleNextRecordBatch);
        hFunc(TEvS3Provider::TEvFileFinished, HandleFileFinished);
        hFunc(TEvS3Provider::TEvAck, Handle);
        hFunc(TEvS3Provider::TEvObjectPathBatch, HandleObjectPathBatch);
        hFunc(TEvS3Provider::TEvObjectPathReadError, HandleObjectPathReadError);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(IDqComputeActorAsyncInput::TEvAsyncInputError, Handle);
        , catch (const std::exception& e) {
            TIssues issues{TIssue{TStringBuilder() << "An unknown exception has occurred: '" << e.what() << "'"}};
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
        }
    )

    void HandleObjectPathBatch(TEvS3Provider::TEvObjectPathBatch::TPtr& objectPathBatch) {
        if (!FileQueueEvents.OnEventReceived(objectPathBatch)) {
            return;
        }

        Y_ENSURE(IsWaitingFileQueueResponse);
        IsWaitingFileQueueResponse = false;
        auto& objectBatch = objectPathBatch->Get()->Record;
        ListedFiles += objectBatch.GetObjectPaths().size();
        IsFileQueueEmpty = objectBatch.GetNoMoreFiles();
        if (IsFileQueueEmpty && !IsConfirmedFileQueueFinish) {
            LOG_T("TS3StreamReadActor", "Sending finish confirmation to FileQueue");
            SendPathBatchRequest();
            IsConfirmedFileQueueFinish = true;
        }
        if (!objectBatch.GetObjectPaths().empty()) {
            PathBatchQueue.emplace_back(
                std::make_move_iterator(objectBatch.MutableObjectPaths()->begin()),
                std::make_move_iterator(objectBatch.MutableObjectPaths()->end()));
        }
        LOG_D(
            "TS3StreamReadActor",
            "HandleObjectPathBatch of size " << objectBatch.GetObjectPaths().size());
        while (TryRegisterCoro()) {}

        if (LastFileWasProcessed()) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
        }
    }

    void HandleObjectPathReadError(TEvS3Provider::TEvObjectPathReadError::TPtr& result) {
        if (!FileQueueEvents.OnEventReceived(result)) {
            return;
        }

        IsFileQueueEmpty = true;
        if (!IsConfirmedFileQueueFinish) {
            LOG_T("TS3StreamReadActor", "Sending finish confirmation to FileQueue");
            SendPathBatchRequest();
            IsConfirmedFileQueueFinish = true;
        }
        TIssues issues;
        IssuesFromMessage(result->Get()->Record.GetIssues(), issues);
        LOG_W("TS3StreamReadActor", "Error while object listing, details: TEvObjectPathReadError: " << issues.ToOneLineString());
        issues = NS3Util::AddParentIssue(TStringBuilder{} << "Error while object listing", std::move(issues));
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
    }

    void HandleRetry(TEvS3Provider::TEvRetryEventFunc::TPtr& retry) {
        return retry->Get()->Functor();
    }

    void HandleNextBlock(TEvS3Provider::TEvNextBlock::TPtr& next) {
        YQL_ENSURE(!ReadSpec->Arrow);
        auto rows = next->Get()->Block.rows();
        IngressStats.Bytes += next->Get()->IngressDelta;
        IngressStats.DecompressedBytes += next->Get()->IngressDecompressedDelta;
        IngressStats.Rows += rows;
        IngressStats.Chunks++;
        IngressStats.Resume();
        CpuTime += next->Get()->CpuTimeDelta;
        if (Counters) {
            QueueBlockCount->Inc();
        }
        StopLoadsIfEnough(rows);
        Blocks.emplace_back(next);
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
    }

    void HandleNextRecordBatch(TEvS3Provider::TEvNextRecordBatch::TPtr& next) {
        YQL_ENSURE(ReadSpec->Arrow);
        auto rows = next->Get()->Batch->num_rows();
        IngressStats.Bytes += next->Get()->IngressDelta;
        IngressStats.Rows += rows;
        IngressStats.Chunks++;
        IngressStats.Resume();
        CpuTime += next->Get()->CpuTimeDelta;
        if (Counters) {
            QueueBlockCount->Inc();
        }
        StopLoadsIfEnough(rows);
        Blocks.emplace_back(next);
        Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
    }

    void HandleFileFinished(TEvS3Provider::TEvFileFinished::TPtr& ev) {
        CoroActors.erase(ev->Sender);
        if (IsCurrentBatchEmpty && CoroActors.size() == 0) {
            IsCurrentBatchEmpty = false;
        }
        if (ev->Get()->IngressDelta) {
            IngressStats.Bytes += ev->Get()->IngressDelta;
            IngressStats.Chunks++;
            IngressStats.Resume();
        }
        CpuTime += ev->Get()->CpuTimeDelta;
        auto size = ev->Get()->SplitSize;
        if (DownloadSize < size) {
            DownloadSize = 0;
        } else {
            DownloadSize -= size;
        }

        if (TaskCounters) {
            HttpInflightLimit->Sub(Gateway->GetBuffersSizePerStream());
        }
        SourceContext->DecSplitCount();
        CompletedFiles++;
        IngressStats.Splits++;
        if (!PathBatchQueue.empty()) {
            TryRegisterCoro();
        } else {
            /*
            If an empty range is being downloaded on the last file,
            then we need to pass the information to Compute Actor that
            the download of all data is finished in this place
            */
            if (LastFileWasProcessed()) {
                Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
            }
        }
    }
    
    void Handle(TEvS3Provider::TEvAck::TPtr& ev) {
        FileQueueEvents.OnEventReceived(ev);
    }

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&) {
        FileQueueEvents.Retry();
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        LOG_T("TS3StreamReadActor", "Handle disconnected FileQueue " << ev->Get()->NodeId);
        FileQueueEvents.HandleNodeDisconnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        LOG_T("TS3StreamReadActor", "Handle connected FileQueue " << ev->Get()->NodeId);
        FileQueueEvents.HandleNodeConnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        LOG_T("TS3StreamReadActor", "Handle undelivered FileQueue ");
        if (!FileQueueEvents.HandleUndelivered(ev)) {
            TIssues issues{TIssue{TStringBuilder() << "FileQueue was lost"}};
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::UNAVAILABLE));
        }
    }

    void Handle(IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr& ev) {
        Send(ev->Forward(ComputeActorId));
    }

    bool LastFileWasProcessed() const {
        return Blocks.empty() && (ListedFiles == CompletedFiles) && IsFileQueueEmpty;
    }

    void StopLoadsIfEnough(ui64 consumedRows) {
        if (!RowsRemained) {
            return;
        }

        *RowsRemained = SubtractSaturating(*RowsRemained, consumedRows);
        if (*RowsRemained == 0) {
            LOG_T("TS3StreamReadActor", "StopLoadsIfEnough(consumedRows = " << consumedRows << ") sends poison");
            for (const auto actorId : CoroActors) {
                Send(actorId, new NActors::TEvents::TEvPoison());
            }
        }
    }

    bool ConsumedEnoughRows() const noexcept {
        return RowsRemained && *RowsRemained == 0;
    }

    const TS3ReadActorFactoryConfig ReadActorFactoryCfg;
    const IHTTPGateway::TPtr Gateway;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    NKikimr::NMiniKQL::TPlainContainerCache ContainerCache;
    NKikimr::NMiniKQL::TPlainContainerCache ArrowTupleContainerCache;
    NKikimr::NMiniKQL::TPlainContainerCache ArrowRowContainerCache;

    const ui64 InputIndex;
    TDqAsyncStats IngressStats;
    const TTxId TxId;
    const NActors::TActorId ComputeActorId;
    const IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;

    const TString Url;
    const TS3Credentials Credentials;
    const TString Pattern;
    const ES3PatternVariant PatternVariant;
    TPathList Paths;
    const bool AddPathIndex;
    size_t ListedFiles = 0;
    size_t CompletedFiles = 0;
    std::optional<ui64> RowsRemained;
    const TReadSpec::TPtr ReadSpec;
    std::deque<TReadyBlock> Blocks;
    TDuration CpuTime;
    mutable TInstant LastMemoryReport = TInstant::Now();
    TSourceContext::TPtr SourceContext;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueDataSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueDataLimit;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueBlockCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr DownloadCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr DownloadPaused;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeferredQueueSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskDownloadCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskDownloadPaused;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskChunkDownloadCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskQueueDataSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskQueueDataLimit;
    ::NMonitoring::TDynamicCounters::TCounterPtr HttpInflightSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr HttpInflightLimit;
    ::NMonitoring::TDynamicCounters::TCounterPtr HttpDataRps;
    ::NMonitoring::TDynamicCounters::TCounterPtr RawInflightSize;
    ::NMonitoring::THistogramPtr DecodedChunkSizeHist;
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr TaskCounters;
    ui64 DownloadSize = 0;
    std::set<NActors::TActorId> CoroActors;
    NActors::TActorId FileQueueActor;
    const ui64 FileSizeLimit;
    const ui64 ReadLimit;
    bool Bootstrapped = false;
    IMemoryQuotaManager::TPtr MemoryQuotaManager;
    bool UseRuntimeListing;
    ui64 FileQueueBatchSizeLimit;
    ui64 FileQueueBatchObjectCountLimit;
    ui64 FileQueueConsumersCountDelta;
    const bool AsyncDecoding;
    const bool AsyncDecompressing;
    bool IsCurrentBatchEmpty = false;
    bool IsFileQueueEmpty = false;
    bool IsWaitingFileQueueResponse = false;
    bool IsConfirmedFileQueueFinish = false;
    TRetryEventsQueue FileQueueEvents;
    TDeque<TVector<NS3::FileQueue::TObjectPath>> PathBatchQueue;
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
            case NUdf::EDataSlot::Decimal: {
                const auto decimalType = static_cast<const TDataDecimalType*>(type);
                auto [precision, scale] = decimalType->GetParams();
                return std::make_shared<const NDB::DataTypeDecimal<NDB::Decimal128>>(precision, scale);
            }
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

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateS3ReadActor(
    const TTypeEnvironment& typeEnv,
    const THolderFactory& holderFactory,
    IHTTPGateway::TPtr gateway,
    NS3::TSource&& params,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const TVector<TString>& readRanges,
    const NActors::TActorId& computeActorId,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
    const TS3ReadActorFactoryConfig& cfg,
    ::NMonitoring::TDynamicCounterPtr counters,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    IMemoryQuotaManager::TPtr memoryQuotaManager)
{
    const IFunctionRegistry& functionRegistry = *holderFactory.GetFunctionRegistry();

    TPathList paths;
    ReadPathsList(taskParams, readRanges, paths);

    const auto token = secureParams.Value(params.GetToken(), TString{});
    const TS3Credentials credentials(credentialsFactory, token);

    const auto& settings = params.GetSettings();
    TString pathPattern = "*";
    ES3PatternVariant pathPatternVariant = ES3PatternVariant::FilePattern;
    auto hasDirectories = std::find_if(paths.begin(), paths.end(), [](const TPath& a) {
                              return a.IsDirectory;
                          }) != paths.end();
    if (hasDirectories) {
        auto pathPatternValue = settings.find("pathpattern");
        if (pathPatternValue == settings.cend()) {
            ythrow yexception() << "'pathpattern' must be configured for directory listing";
        }
        pathPattern = pathPatternValue->second;

        auto pathPatternVariantValue = settings.find("pathpatternvariant");
        if (pathPatternVariantValue == settings.cend()) {
            ythrow yexception()
                << "'pathpatternvariant' must be configured for directory listing";
        }
        if (!TryFromString(pathPatternVariantValue->second, pathPatternVariant)) {
            ythrow yexception()
                << "Unknown 'pathpatternvariant': " << pathPatternVariantValue->second;
        }
    }
    ui64 fileSizeLimit = cfg.FileSizeLimit;
    if (params.HasFormat()) {
        if (auto it = cfg.FormatSizeLimits.find(params.GetFormat()); it != cfg.FormatSizeLimits.end()) {
            fileSizeLimit = it->second;
        }
    }

    bool addPathIndex = false;
    if (auto it = settings.find("addPathIndex"); it != settings.cend()) {
        addPathIndex = FromString<bool>(it->second);
    }

    NYql::NSerialization::TSerializationInterval::EUnit intervalUnit = NYql::NSerialization::TSerializationInterval::EUnit::MICROSECONDS;
    if (auto it = settings.find("data.interval.unit"); it != settings.cend()) {
        intervalUnit = NYql::NSerialization::TSerializationInterval::ToUnit(it->second);
    }

    std::optional<ui64> rowsLimitHint;
    if (params.GetRowsLimitHint() != 0) {
        rowsLimitHint = params.GetRowsLimitHint();
    }

    TActorId fileQueueActor;
    if (auto it = settings.find("fileQueueActor"); it != settings.cend()) {
        NActorsProto::TActorId protoId;
        TMemoryInput inputStream(it->second);
        ParseFromTextFormat(inputStream, protoId);
        fileQueueActor = ActorIdFromProto(protoId);
    }

    ui64 fileQueueBatchSizeLimit = 0;
    if (auto it = settings.find("fileQueueBatchSizeLimit"); it != settings.cend()) {
        fileQueueBatchSizeLimit = FromString<ui64>(it->second);
    }

    ui64 fileQueueBatchObjectCountLimit = 0;
    if (auto it = settings.find("fileQueueBatchObjectCountLimit"); it != settings.cend()) {
        fileQueueBatchObjectCountLimit = FromString<ui64>(it->second);
    }

    ui64 fileQueueConsumersCountDelta = 0;
    if (readRanges.size() > 1) {
        fileQueueConsumersCountDelta = readRanges.size() - 1;
    }

    if (params.HasFormat() && params.HasRowType()) {
        const auto pb = std::make_unique<TProgramBuilder>(typeEnv, functionRegistry);
        const auto outputItemType = NCommon::ParseTypeFromYson(TStringBuf(params.GetRowType()), *pb, Cerr);
        YQL_ENSURE(outputItemType->IsStruct(), "Row type is not struct");
        const auto structType = static_cast<TStructType*>(outputItemType);

        const auto readSpec = std::make_shared<TReadSpec>();
        readSpec->Arrow = params.GetFormat() == "parquet";
        readSpec->ParallelRowGroupCount = params.GetParallelRowGroupCount();
        readSpec->RowGroupReordering = params.GetRowGroupReordering();
        readSpec->ParallelDownloadCount = params.GetParallelDownloadCount();

        if (rowsLimitHint && *rowsLimitHint <= 1000) {
            readSpec->ParallelRowGroupCount = 1;
            readSpec->ParallelDownloadCount = 1;
        }
        if (readSpec->Arrow) {
            fileSizeLimit = cfg.BlockFileSizeLimit;
            arrow::SchemaBuilder builder;
            auto extraStructType = static_cast<TStructType*>(pb->NewStructType(structType, BlockLengthColumnName,
                pb->NewBlockType(pb->NewDataType(NUdf::EDataSlot::Uint64), TBlockType::EShape::Scalar)));

            for (ui32 i = 0U; i < extraStructType->GetMembersCount(); ++i) {
                TStringBuf memberName = extraStructType->GetMemberName(i);
                if (memberName == BlockLengthColumnName) {
                    readSpec->BlockLengthPosition = i;
                    continue;
                }

                auto memberType = extraStructType->GetMemberType(i);
                std::shared_ptr<arrow::DataType> dataType;

                YQL_ENSURE(ConvertArrowType(memberType, dataType), "Unsupported arrow type");
                THROW_ARROW_NOT_OK(builder.AddField(std::make_shared<arrow::Field>(std::string(memberName), dataType, memberType->IsOptional())));
                readSpec->ColumnReorder.push_back(i);
                readSpec->RowSpec.emplace(memberName, memberType);
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

        if (readSpec->Format == "csv_with_names") {
            readSpec->Settings.csv.empty_as_default = true;
        }

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
        ui64 sizeLimit = std::numeric_limits<ui64>::max();
        if (const auto it = settings.find("sizeLimit"); settings.cend() != it) {
            sizeLimit = FromString<ui64>(it->second);
        }

        const auto actor = new TS3StreamReadActor(inputIndex, statsLevel, txId, std::move(gateway), holderFactory, params.GetUrl(), credentials, pathPattern, pathPatternVariant,
                                                  std::move(paths), addPathIndex, readSpec, computeActorId, retryPolicy,
                                                  cfg, counters, taskCounters, fileSizeLimit, sizeLimit, rowsLimitHint, memoryQuotaManager,
                                                  params.GetUseRuntimeListing(), fileQueueActor, fileQueueBatchSizeLimit, fileQueueBatchObjectCountLimit, fileQueueConsumersCountDelta,
                                                  params.GetAsyncDecoding(), params.GetAsyncDecompressing());

        return {actor, actor};
    } else {
        ui64 sizeLimit = std::numeric_limits<ui64>::max();
        if (const auto it = settings.find("sizeLimit"); settings.cend() != it)
            sizeLimit = FromString<ui64>(it->second);

        return CreateRawReadActor(inputIndex, statsLevel, txId, std::move(gateway), holderFactory, params.GetUrl(), credentials, pathPattern, pathPatternVariant,
                                            std::move(paths), addPathIndex, computeActorId, sizeLimit, retryPolicy,
                                            cfg, counters, taskCounters, fileSizeLimit, rowsLimitHint,
                                            params.GetUseRuntimeListing(), fileQueueActor, fileQueueBatchSizeLimit, fileQueueBatchObjectCountLimit, fileQueueConsumersCountDelta);
    }
}

} // namespace NYql::NDq
