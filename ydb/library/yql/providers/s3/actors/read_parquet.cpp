#include "parquet_cache.h"
#include "read_parquet.h"
#include "yql_arrow_column_converters.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_util.h>

#include <arrow/api.h>
#include <arrow/util/future.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadParquetActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)
#define LOG_W(stream) \
    LOG_WARN_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadParquetActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)
#define LOG_I(stream) \
    LOG_INFO_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadParquetActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadParquetActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)
#define LOG_T(stream) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadParquetActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)

namespace NYql::NDq {

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw yexception() << _s.ToString(); \
    } while (false)

struct TS3ReadForMetadata : public yexception {
    using yexception::yexception;
};

struct TS3ReadInternalError : public yexception {
    using yexception::yexception;
};

struct TReadCache {
    ui64 Cookie = 0;
    TString Data;
    std::optional<ui64> RowGroupIndex;
    bool Ready = false;
};

struct TReadRangeCompare {
    bool operator() (const TEvS3Provider::TReadRange& lhs, const TEvS3Provider::TReadRange& rhs) const
    {
        return (lhs.Offset < rhs.Offset) || (lhs.Offset == rhs.Offset && lhs.Length < rhs.Length);
    }
};

struct TReaderSlot {
    std::unique_ptr<parquet::arrow::FileReader> Reader;
    bool Pending = false;
    ui64 RowGroup = 0;
};

class TS3ReadParquetActor : public NActors::TActorBootstrapped<TS3ReadParquetActor> {

public:

   class THttpRandomAccessFile : public arrow::io::RandomAccessFile {
    public:
        THttpRandomAccessFile(TS3ReadParquetActor* reader, size_t fileSize) : Reader(reader), FileSize(fileSize) {
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
            return Reader->ReadAt(position, nbytes);
        }
        arrow::Status WillNeed(const std::vector<arrow::io::ReadRange>& readRanges) override {
            return Reader->WillNeed(readRanges);
        }

    private:
        TS3ReadParquetActor *const Reader;
        const size_t FileSize;
        int64_t InnerPos = 0;
    };

    TS3ReadParquetActor(
        TSplitReadContext::TPtr context
        , ui64 parallelRowGroupCount
        , bool rowGroupReordering
        , bool useParquetCache
    )
        : Context(context) 
        , ParallelRowGroupCount(parallelRowGroupCount)
        , RowGroupReordering(rowGroupReordering)
        , UseParquetCache(useParquetCache) {

    }

    void Bootstrap() {
        if (Context->SplitOffset != 0 || Context->SplitSize != Context->FileSize) {
            TIssues issues{TIssue{TStringBuilder() << "Partial file splits are not supported"}};
            Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
            return;
        }

        LOG_D("Bootstrap");
        Become(&TS3ReadParquetActor::StateFunc);

        FooterSize = std::min<std::size_t>(Context->FileSize, 65536u);
        FooterOffset = Context->FileSize - FooterSize;
        StartDownload(FooterOffset, FooterSize);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvS3Provider::TEvReadResult2, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
        hFunc(TEvS3Provider::TEvCacheCheckResult, Handle);
    )

    void Handle(TEvS3Provider::TEvReadResult2::TPtr& ev) {

        // gateway error is always EXTERNAL_ERROR

        if (ev->Get()->Failure) {
            Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, ev->Get()->Issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }

        auto data = ev->Get()->Result.Extract();
        auto httpCode = ev->Get()->Result.HttpResponseCode;

        // not 20X code may include error response from S3

        if (httpCode != 200 && httpCode != 206) {
            TString errorCode;
            TString message;
            NDqProto::StatusIds::StatusCode statusCode = NDqProto::StatusIds::EXTERNAL_ERROR;
            if (ParseS3ErrorResponse(data, errorCode, message)) {
                auto code = StatusFromS3ErrorCode(errorCode);
                if (code != NDqProto::StatusIds::UNSPECIFIED) {
                    statusCode = code;
                }
            } else {
                message = data;
            }
            message = TStringBuilder{} << "Error while reading file " << Context->Url << ", details: " << message /* TODO << ", request id: [" << requestId << "]" */;
            Send(Context->SourceContext->SourceId, new IDqComputeActorAsyncInput::TEvAsyncInputError(0, BuildIssues(httpCode, errorCode, message), statusCode));
            return;
        }

        // all validations are passed, may use result as data

        if (MetadataParsed) {
            ProcessReadRange(ev);
            return;
        } else if (WaitForFooter) {
            WaitForFooter = false;
            RawFooter = data;
        } else if (WaitForMetadata) {
            WaitForMetadata = false;
            RawMetadata = data;
        } else {
            TIssues issues{TIssue{TStringBuilder() << "Unknown read result"}};
            Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
            return;
        }

        // try to parse metadata

        parquet::arrow::FileReaderBuilder builder;
        builder.memory_pool(arrow::default_memory_pool());
        parquet::ArrowReaderProperties properties;
        properties.set_cache_options(arrow::io::CacheOptions::LazyDefaults());
        properties.set_pre_buffer(true);
        builder.properties(properties);

        try {
            THROW_ARROW_NOT_OK(builder.Open(std::make_shared<THttpRandomAccessFile>(this, Context->FileSize)));
        } catch(const TS3ReadForMetadata& ex) {
            if (WaitForMetadata) {
                return;
            }
            TIssues issues;
            issues.AddIssue(TIssue(ex.what()));
            Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
            return;
        } catch(const TS3ReadInternalError& ex) {
            TIssues issues;
            issues.AddIssue(TIssue(ex.what()));
            Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
            return;
        }

        MetadataParsed = true;
        ui64 readerCount = 1;
        ReaderSlots.resize(1);

        // download row groups

        THROW_ARROW_NOT_OK(builder.Build(&ReaderSlots[0].Reader));
        auto fileMetadata = ReaderSlots[0].Reader->parquet_reader()->metadata();
        NumGroups = ReaderSlots[0].Reader->num_row_groups();

        if (NumGroups == 0) {
            Send(Context->SourceContext->SourceId, new TEvS3Provider::TEvFileFinished(Context->PathIndex, TakeIngressDelta(), TakeCpuTimeDelta(), Context->SplitSize));
            return;
        }

        std::shared_ptr<arrow::Schema> schema;
        THROW_ARROW_NOT_OK(ReaderSlots[0].Reader->GetSchema(&schema));

        BuildColumnConverters(Context->SourceContext->Schema, schema, ColumnIndices,
          ColumnConverters, Context->SourceContext->RowTypes, Context->SourceContext->Settings);

        // select count(*) case - single reader is enough
        if (!ColumnIndices.empty()) {
            if (ParallelRowGroupCount) {
                readerCount = ParallelRowGroupCount;
            } else {
                // we want to read in parallel as much as 1/2 of fair share bytes
                // (it's compressed size, after decoding it will grow)
                ui64 compressedSize = 0;
                for (int i = 0; i < fileMetadata->num_row_groups(); i++) {
                    auto rowGroup = fileMetadata->RowGroup(i);
                    for (const auto columIndex : ColumnIndices) {
                        compressedSize += rowGroup->ColumnChunk(columIndex)->total_compressed_size();
                    }
                }
                // count = (fair_share / 2) / (compressed_size / num_group)
                auto desiredReaderCount = (Context->SourceContext->FairShare() * NumGroups) / (compressedSize * 2);
                // min is 1
                // max is 5 (should be also tuned probably)
                if (desiredReaderCount) {
                    readerCount = std::min(desiredReaderCount, 5ul);
                }
            }
            if (readerCount > NumGroups) {
                readerCount = NumGroups;
            }
        }

        if (readerCount > 1) {
            // init other readers if any
            ReaderSlots.resize(readerCount);
            for (ui64 i = 1; i < readerCount; i++) {
                THROW_ARROW_NOT_OK(builder.Open(std::make_shared<THttpRandomAccessFile>(this, Context->FileSize),
                                parquet::default_reader_properties(),
                                fileMetadata));
                THROW_ARROW_NOT_OK(builder.Build(&ReaderSlots[i].Reader));
            }
        }

        if (ColumnIndices.empty()) {
            for (ui64 i = 0; i < NumGroups; i++) {
                ReadRowGroup(0, i);
                Send(Context->SourceContext->SourceId, new TEvS3Provider::TEvFileFinished(Context->PathIndex, TakeIngressDelta(), TakeCpuTimeDelta(), Context->SplitSize));
                return;
            }
        } else {
            for (ui64 i = 0; i < readerCount; i++) {
                AssignReader(i);
            }
        }
    }

    void HandlePoison() {
        LOG_D("TEvPoison");
        PassAway();
    }

    void AssignReader(ui64 readerIndex) {
        auto rowGroup = NextGroup++;
        RowGroupReaderIndex[rowGroup] = readerIndex;
        ReaderSlots[readerIndex].RowGroup = rowGroup;
        if (UseParquetCache) {
            Send(ParquetCacheActorId(), new TEvS3Provider::TEvCacheCheckRequest(
                Context->SourceContext->SourceId, Context->Url, rowGroup
            ), 0, readerIndex);
        } else {
            CurrentRowGroupIndex = rowGroup;
            THROW_ARROW_NOT_OK(ReaderSlots[readerIndex].Reader->WillNeedRowGroups({ static_cast<int>(rowGroup) }, ColumnIndices));
        }
        Context->SourceContext->IncChunkCount();
    }

    void Handle(TEvS3Provider::TEvCacheCheckResult::TPtr& ev) {
        auto readerIndex = ev->Cookie;
        if (ev->Get()->Hit) {
            // take data
            AssignReader(readerIndex);
        } else {
            CurrentRowGroupIndex = ReaderSlots[readerIndex].RowGroup;
            THROW_ARROW_NOT_OK(ReaderSlots[readerIndex].Reader->WillNeedRowGroups({ static_cast<int>(*CurrentRowGroupIndex) }, ColumnIndices));
        }
    }

    static void OnDownloadResult(NActors::TActorSystem* actorSystem, NActors::TActorId selfId, TEvS3Provider::TReadRange range, ui64 cookie, IHTTPGateway::TResult&& result) {
        if (!result.Issues) {
            actorSystem->Send(new NActors::IEventHandle(selfId, NActors::TActorId{}, new TEvS3Provider::TEvReadResult2(range, std::move(result.Content)), 0, cookie));
        } else {
            actorSystem->Send(new NActors::IEventHandle(selfId, NActors::TActorId{}, new TEvS3Provider::TEvReadResult2(range, std::move(result.Issues)), 0, cookie));
        }
    }

    void StartDownload(std::size_t offset, std::size_t size) {
        WaitForFooter = true;
        Context->Gateway->Download(
            Context->Url,
            Context->Headers,
            offset,
            size,
            std::bind(&OnDownloadResult, Context->SourceContext->ActorSystem, SelfId(), TEvS3Provider::TReadRange{static_cast<int64_t>(offset), static_cast<int64_t>(size)}, 0, std::placeholders::_1),
            {},
            Context->RetryPolicy);
    }

    void ProcessReadRange(TEvS3Provider::TEvReadResult2::TPtr& ev) {
        auto readyRange = ev->Get()->ReadRange;
        LOG_D("Download FINISHED [" << readyRange.Offset << "-" << readyRange.Length << "], cookie: " << ev->Cookie);
        IngressBytes += readyRange.Length;

        auto it = RangeCache.find(readyRange);
        if (it == RangeCache.end()) {
            LOG_W("Download completed for unknown/discarded range [" << readyRange.Offset << "-" << readyRange.Length << "]");
            return;
        }

        if (it->second.Cookie != ev->Cookie) {
            LOG_W("Mistmatched cookie for range [" << readyRange.Offset << "-" << readyRange.Length << "], received " << ev->Cookie << ", expected " << it->second.Cookie);
            return;
        }

        it->second.Data = ev->Get()->Result.Extract();
        ui64 size = it->second.Data.size();
        it->second.Ready = true;
        if (it->second.RowGroupIndex) {
            if (!DecreaseRowGroupInflight(*it->second.RowGroupIndex)) {
                LOG_D("RowGroup #" << *it->second.RowGroupIndex << " is READY");
                ReadyRowGroups.push(*it->second.RowGroupIndex);
            }
            ReadInflightSize[*it->second.RowGroupIndex] += size;
            // if (RawInflightSize) {
            //     RawInflightSize->Add(size);
            // }
        }
    }

    void ReadRowGroup(std::size_t, std::size_t) {

    }

    ui64 TakeIngressDelta() {
        auto currentIngressBytes = IngressBytes;
        IngressBytes = 0;
        return currentIngressBytes;
    }

    TDuration TakeCpuTimeDelta() {
        return TDuration::Zero();
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
        Context->Gateway->Download(
            Context->Url,
            Context->Headers,
            range.Offset,
            range.Length,
            std::bind(&OnDownloadResult, Context->SourceContext->ActorSystem, SelfId(), range, ++RangeCookie, std::placeholders::_1),
            {},
            Context->RetryPolicy);
    
        LOG_D("Download STARTED [" << range.Offset << "-" << range.Length << "], cookie: " << RangeCookie);
        auto& result = RangeCache[range];
        if (result.Cookie) {
            // may overwrite old range in case of desync?
            if (result.RowGroupIndex) {
                LOG_W("RangeInfo DISCARDED [" << range.Offset << "-" << range.Length << "], cookie: " << RangeCookie << ", rowGroup " << *result.RowGroupIndex);
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

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) {
        if (!MetadataParsed) {
            if (WaitForFooter) {
                throw TS3ReadInternalError() << "Footer is not loaded";
            }
            if (static_cast<std::size_t>(position) == FooterOffset && static_cast<std::size_t>(nbytes) == FooterSize) {
                return arrow::Buffer::FromString(RawFooter);
            }
            if (WaitForMetadata) {
                throw TS3ReadInternalError() << "Metadata is not loaded";
            }
            if (MetadataSize == 0) {
                MetadataOffset = position;
                MetadataSize = nbytes;
                WaitForMetadata = true;
                StartDownload(MetadataOffset, MetadataSize);
                throw TS3ReadForMetadata();
            }
            if (static_cast<std::size_t>(position) == MetadataOffset && static_cast<std::size_t>(nbytes) == MetadataSize) {
                return arrow::Buffer::FromString(RawMetadata);
            }
        }

        TEvS3Provider::TReadRange range { .Offset = position, .Length = nbytes };
        auto& cache = GetOrCreate(range);

        if (!cache.Ready) {
            throw TS3ReadInternalError() << "Cache for range of position = " << position << ", nbytes = " << nbytes << " is not ready";
        }

        TString data = cache.Data;
        RangeCache.erase(range);

        return arrow::Buffer::FromString(data);
    }

    TSplitReadContext::TPtr Context;
    const ui64 ParallelRowGroupCount;
    const bool RowGroupReordering;
    const bool UseParquetCache;

    bool WaitForFooter = false;
    std::size_t FooterOffset = 0;
    std::size_t FooterSize = 0;
    TString RawFooter;
    bool WaitForMetadata = false;
    std::size_t MetadataOffset = 0;
    std::size_t MetadataSize = 0;
    TString RawMetadata;
    bool MetadataParsed = false;
    std::vector<TReaderSlot> ReaderSlots;
    std::vector<int> ColumnIndices;
    std::vector<TColumnConverter> ColumnConverters;

    ui64 RangeCookie = 0;
    std::map<TEvS3Provider::TReadRange, TReadCache, TReadRangeCompare> RangeCache;
    std::map<ui64, ui64> ReadInflightSize;
    std::optional<ui64> CurrentRowGroupIndex;
    std::map<ui64, ui64> RowGroupRangeInflight;
    std::priority_queue<ui64, std::vector<ui64>, std::greater<ui64>> ReadyRowGroups;
    std::map<ui64, ui64> RowGroupReaderIndex;

    ui64 IngressBytes = 0;
    ui64 NumGroups = 0;
    ui64 NextGroup = 0;
};

NActors::IActor* CreateS3ReadParquetActor(
    TSplitReadContext::TPtr context,
    ui64 parallelRowGroupCount,
    bool rowGroupReordering,
    bool useParquetCache
) {
    return new TS3ReadParquetActor(context, parallelRowGroupCount, rowGroupReordering, useParquetCache);
}

} // namespace NYql::NDq
