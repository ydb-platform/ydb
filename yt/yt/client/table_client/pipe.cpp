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

    const TRowBufferPtr RowBuffer;
    TRingQueue<TUnversionedRow> RowQueue;

    TPromise<void> ReaderReadyEvent;
    TPromise<void> WriterReadyEvent = NewPromise<void>();

    int RowsWritten = 0;
    int RowsRead = 0;
    bool WriterClosed = false;
    bool Failed = false;

    explicit TSchemafulPipeData(IMemoryChunkProviderPtr chunkProvider)
        : RowBuffer(New<TRowBuffer>(TSchemafulPipeBufferTag(), std::move(chunkProvider)))
    {
        ResetReaderReadyEvent();
    }

    void ResetReaderReadyEvent()
    {
        ReaderReadyEvent = NewPromise<void>();
        ReaderReadyEvent.OnCanceled(BIND(&TSchemafulPipeData::HandleCancel, MakeWeak(this)));
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
            if (WriterClosed || Failed)
                return;

            Failed = true;
            readerReadyEvent = ReaderReadyEvent;
            writerReadyEvent = WriterReadyEvent;
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
        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);
        i64 dataWeight = 0;

        {
            auto guard = Guard(Data_->SpinLock);

            if (Data_->WriterClosed && Data_->RowsWritten == Data_->RowsRead) {
                return nullptr;
            }

            if (!Data_->Failed) {
                auto& rowQueue = Data_->RowQueue;
                while (!rowQueue.empty() &&
                    std::ssize(rows) < options.MaxRowsPerRead &&
                    dataWeight < options.MaxDataWeightPerRead)
                {
                    auto row = rowQueue.front();
                    rowQueue.pop();
                    dataWeight += GetDataWeight(row);
                    rows.push_back(row);
                    ++Data_->RowsRead;
                }
            }

            if (rows.empty()) {
                ReadyEvent_ = Data_->ReaderReadyEvent.ToFuture();
            }
        }

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
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
};

DEFINE_REFCOUNTED_TYPE(TSchemafulPipeReader);

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipeWriter
    : public IUnversionedRowsetWriter
{
public:
    explicit TSchemafulPipeWriter(TSchemafulPipeDataPtr data)
        : Data_(std::move(data))
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
        // Copy data (no lock).
        auto capturedRows = Data_->RowBuffer->CaptureRows(rows);

        // Enqueue rows (with lock).
        TPromise<void> readerReadyEvent;

        {
            auto guard = Guard(Data_->SpinLock);

            YT_VERIFY(!Data_->WriterClosed);

            if (Data_->Failed) {
                return false;
            }

            for (auto row : capturedRows) {
                Data_->RowQueue.push(row);
                ++Data_->RowsWritten;
            }

            readerReadyEvent = std::move(Data_->ReaderReadyEvent);
            Data_->ResetReaderReadyEvent();
        }

        // Signal readers.
        readerReadyEvent.TrySet(TError());

        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        // TODO(babenko): implement backpressure from reader
        auto guard = Guard(Data_->SpinLock);
        YT_VERIFY(Data_->Failed);
        return Data_->WriterReadyEvent;
    }

    std::optional<TRowsDigest> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    const TSchemafulPipeDataPtr Data_;
};

DEFINE_REFCOUNTED_TYPE(TSchemafulPipeWriter);

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipe
    : public ISchemafulPipe
{
public:
    explicit TSchemafulPipe(IMemoryChunkProviderPtr chunkProvider)
        : Data_(New<TSchemafulPipeData>(std::move(chunkProvider)))
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

ISchemafulPipePtr CreateSchemafulPipe(IMemoryChunkProviderPtr chunkProvider)
{
    return New<TSchemafulPipe>(std::move(chunkProvider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
