#include "pipe.h"

#include "row_batch.h"
#include "row_buffer.h"
#include "unversioned_reader.h"
#include "unversioned_writer.h"

#include <yt/yt/core/misc/ring_queue.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NTableClient {

using namespace NThreading;
using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSchemafulPipeBufferTag
{ };

struct TSchemafulPipeData;
using TSchemafulPipeDataPtr = TIntrusivePtr<TSchemafulPipeData>;

DECLARE_REFCOUNTED_CLASS(TSchemafulPipeReader);
DECLARE_REFCOUNTED_CLASS(TSchemafulPipeWriter);

////////////////////////////////////////////////////////////////////////////////

struct TSchemafulPipeData final
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock);

    const IMemoryChunkProviderPtr ChunkProvider;
    const std::optional<int> MaxFlushBatchCount;

    TRingQueue<TSharedRange<TUnversionedRow>> FlushedBatches;

    TPromise<void> ReaderReadyEvent;
    TPromise<void> WriterReadyEvent = NewPromise<void>();

    int RowsWritten = 0;
    int RowsRead = 0;
    bool WriterBlocked = false;
    bool WriterClosed = false;
    bool Failed = false;

    explicit TSchemafulPipeData(IMemoryChunkProviderPtr chunkProvider, std::optional<int> maxFlushBatchCount)
        : ChunkProvider(std::move(chunkProvider))
        , MaxFlushBatchCount(maxFlushBatchCount)
    {
        ResetReaderReadyEvent();
    }

    void ResetReaderReadyEvent()
    {
        ReaderReadyEvent = NewPromise<void>();
        ReaderReadyEvent.OnCanceled(BIND(&TSchemafulPipeData::HandleCancel, MakeWeak(this)));
    }

    TPromise<void> ResetWriterReadyEvent()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock);
        auto oldEvent = std::move(WriterReadyEvent);
        WriterReadyEvent = NewPromise<void>();
        WriterBlocked = true;
        return oldEvent;
    }

    void HandleCancel(const TError& error)
    {
        Fail(TError(NYT::EErrorCode::Canceled, "Pipe reader canceled")
            << error);
    }

    void Fail(const TError& error)
    {
        YT_VERIFY(!error.IsOK());

        TPromise<void> readerReadyEvent;
        TPromise<void> writerReadyEvent;

        {
            auto guard = Guard(SpinLock);
            if (WriterClosed || Failed) {
                return;
            }

            Failed = true;
            readerReadyEvent = ReaderReadyEvent;
            writerReadyEvent = WriterReadyEvent;
            WriterBlocked = false;
        }

        readerReadyEvent.TrySet(error);
        writerReadyEvent.TrySet(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipeReader
    : public ISchemafulUnversionedReader
{
public:
    explicit TSchemafulPipeReader(TSchemafulPipeDataPtr data)
        : Data_(std::move(data))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        TSharedRange<TUnversionedRow> readRange;
        TPromise<void> writerReadyEvent;

        {
            auto guard = Guard(Data_->SpinLock);

            if (Data_->Failed) {
                ReadyEvent_ = Data_->ReaderReadyEvent.ToFuture();
            } else if (Data_->WriterClosed && Data_->RowsWritten == Data_->RowsRead) {
                return nullptr;
            } else {
                if (CurrentBatch_.Empty() && !Data_->FlushedBatches.empty()) {
                    CurrentBatch_ = std::move(Data_->FlushedBatches.front());
                    Data_->FlushedBatches.pop();

                    if (Data_->WriterBlocked &&
                        Data_->MaxFlushBatchCount &&
                        std::ssize(Data_->FlushedBatches) < *Data_->MaxFlushBatchCount)
                    {
                        Data_->WriterBlocked = false;
                        writerReadyEvent = std::move(Data_->WriterReadyEvent);
                        Data_->WriterReadyEvent = NewPromise<void>();
                    }
                }

                if (!CurrentBatch_.Empty()) {
                    i64 dataWeight = 0;
                    auto it = CurrentBatch_.begin();
                    while (it != CurrentBatch_.end() &&
                        std::distance(CurrentBatch_.begin(), it) < options.MaxRowsPerRead &&
                        dataWeight < options.MaxDataWeightPerRead)
                    {
                        dataWeight += GetDataWeight(*it);
                        ++it;
                    }

                    auto rowCount = std::distance(CurrentBatch_.begin(), it);
                    Data_->RowsRead += rowCount;

                    if (it == CurrentBatch_.end()) {
                        readRange = std::move(CurrentBatch_);
                    } else {
                        readRange = CurrentBatch_.Slice(CurrentBatch_.begin(), it);
                        CurrentBatch_ = CurrentBatch_.Slice(it, CurrentBatch_.end());
                    }
                }
            }

            if (!Data_->Failed && readRange.Empty()) {
                ReadyEvent_ = Data_->ReaderReadyEvent.ToFuture();
            }
        }

        if (writerReadyEvent) {
            writerReadyEvent.TrySet(TError());
        }

        return CreateBatchFromUnversionedRows(std::move(readRange));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return ReadyEvent_;
    }

    TDataStatistics GetDataStatistics() const override
    {
        return DataStatistics_.Load();
    }

    void SetDataStatistics(TDataStatistics dataStatistics)
    {
        DataStatistics_.Store(std::move(dataStatistics));
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return NChunkClient::TCodecStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

private:
    const TSchemafulPipeDataPtr Data_;

    TAtomicObject<TDataStatistics> DataStatistics_;

    TFuture<void> ReadyEvent_ = OKFuture;

    TSharedRange<TUnversionedRow> CurrentBatch_;
};

DEFINE_REFCOUNTED_TYPE(TSchemafulPipeReader);

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipeWriter
    : public IUnversionedRowsetWriter
{
public:
    explicit TSchemafulPipeWriter(TSchemafulPipeDataPtr data)
        : Data_(std::move(data))
        , RowBuffer_(New<TRowBuffer>(TSchemafulPipeBufferTag(), Data_->ChunkProvider))
    { }

    TFuture<void> Close() override
    {
        TPromise<void> readerReadyEvent;
        TPromise<void> writerReadyEvent;

        bool doClose = false;

        {
            auto guard = Guard(Data_->SpinLock);

            YT_VERIFY(!Data_->WriterClosed);
            Data_->WriterClosed = true;

            if (!Data_->Failed) {
                doClose = true;
                if (!Rows_.empty()) {
                    Data_->FlushedBatches.push(MakeSharedRange(
                        std::move(Rows_),
                        std::move(RowBuffer_)));
                }
            }

            readerReadyEvent = Data_->ReaderReadyEvent;
            writerReadyEvent = Data_->WriterReadyEvent;
        }

        readerReadyEvent.TrySet(TError());
        if (doClose) {
            writerReadyEvent.TrySet(TError());
        }

        return writerReadyEvent;
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        if (rows.empty()) {
            return true;
        }

        for (auto row : rows) {
            Rows_.push_back(RowBuffer_->CaptureRow(row));
        }

        const bool needsFlush = RowBuffer_->GetSize() >= FlushDataWeight;

        // Preallocate the next buffer outside the spinlock.
        TRowBufferPtr nextRowBuffer;
        if (needsFlush) {
            nextRowBuffer = New<TRowBuffer>(TSchemafulPipeBufferTag(), Data_->ChunkProvider);
        }

        TPromise<void> readerReadyEvent;

        {
            auto guard = Guard(Data_->SpinLock);

            YT_VERIFY(!Data_->WriterClosed);

            if (Data_->Failed) {
                return false;
            }

            Data_->RowsWritten += std::ssize(rows);

            if (needsFlush) {
                Data_->FlushedBatches.push(MakeSharedRange(
                    std::move(Rows_),
                    std::move(RowBuffer_)));
                RowBuffer_ = std::move(nextRowBuffer);

                readerReadyEvent = std::move(Data_->ReaderReadyEvent);
                Data_->ResetReaderReadyEvent();

                if (Data_->MaxFlushBatchCount &&
                    std::ssize(Data_->FlushedBatches) > *Data_->MaxFlushBatchCount)
                {
                    if (!Data_->WriterBlocked) {
                        Data_->ResetWriterReadyEvent();
                    }
                    readerReadyEvent.TrySet(TError());
                    return false;
                }
            }
        }

        // Signal readers only when a new batch was flushed.
        if (readerReadyEvent) {
            readerReadyEvent.TrySet(TError());
        }

        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        auto guard = Guard(Data_->SpinLock);
        if (Data_->Failed) {
            return Data_->WriterReadyEvent.ToFuture();
        }
        if (Data_->MaxFlushBatchCount &&
            std::ssize(Data_->FlushedBatches) > *Data_->MaxFlushBatchCount)
        {
            if (!Data_->WriterBlocked) {
                Data_->ResetWriterReadyEvent();
            }
            return Data_->WriterReadyEvent.ToFuture();
        }
        return OKFuture;
    }

    std::optional<TRowsDigest> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    const TSchemafulPipeDataPtr Data_;

    TRowBufferPtr RowBuffer_;
    std::vector<TUnversionedRow> Rows_;
};

DEFINE_REFCOUNTED_TYPE(TSchemafulPipeWriter);

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipe
    : public ISchemafulPipe
{
public:
    TSchemafulPipe(IMemoryChunkProviderPtr chunkProvider, std::optional<int> maxFlushBatchCount)
        : Data_(New<TSchemafulPipeData>(std::move(chunkProvider), maxFlushBatchCount))
        , Reader_(New<TSchemafulPipeReader>(Data_))
        , Writer_(New<TSchemafulPipeWriter>(Data_))
    { }

    ISchemafulUnversionedReaderPtr GetReader() const final
    {
        return Reader_;
    }

    IUnversionedRowsetWriterPtr GetWriter() const final
    {
        return Writer_;
    }

    void Fail(const TError& error) final
    {
        Data_->Fail(error);
    }

    void SetReaderDataStatistics(TDataStatistics dataStatistics) final
    {
        Reader_->SetDataStatistics(std::move(dataStatistics));
    }

private:
    const TSchemafulPipeDataPtr Data_;
    const TSchemafulPipeReaderPtr Reader_;
    const TSchemafulPipeWriterPtr Writer_;
};

DEFINE_REFCOUNTED_TYPE(TSchemafulPipe);

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchemafulPipePtr CreateSchemafulPipe(
    IMemoryChunkProviderPtr chunkProvider,
    std::optional<int> maxFlushBatchCount)
{
    return New<TSchemafulPipe>(std::move(chunkProvider), maxFlushBatchCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
