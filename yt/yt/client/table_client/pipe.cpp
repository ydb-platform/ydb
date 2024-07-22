#include "pipe.h"

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NTableClient {

using NChunkClient::NProto::TDataStatistics;
using NCrypto::TMD5Hash;

////////////////////////////////////////////////////////////////////////////////

struct TSchemafulPipeBufferTag
{ };

struct TSchemafulPipe::TData
    : public TRefCounted
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

    explicit TData(IMemoryChunkProviderPtr chunkProvider)
        : RowBuffer(New<TRowBuffer>(TSchemafulPipeBufferTag(), std::move(chunkProvider)))
    {
        ResetReaderReadyEvent();
    }

    void ResetReaderReadyEvent()
    {
        ReaderReadyEvent = NewPromise<void>();
        ReaderReadyEvent.OnCanceled(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            Fail(TError(NYT::EErrorCode::Canceled, "Pipe reader canceled")
                << error);
        }));
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

class TSchemafulPipe::TReader
    : public ISchemafulUnversionedReader
{
public:
    explicit TReader(TDataPtr data)
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
        return TDataStatistics();
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
    const TDataPtr Data_;

    TFuture<void> ReadyEvent_ = VoidFuture;

};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipe::TWriter
    : public IUnversionedRowsetWriter
{
public:
    explicit TWriter(TDataPtr data)
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

    std::optional<TMD5Hash> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    const TDataPtr Data_;

};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulPipe::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(IMemoryChunkProviderPtr chunkProvider)
        : Data_(New<TData>(std::move(chunkProvider)))
        , Reader_(New<TReader>(Data_))
        , Writer_(New<TWriter>(Data_))
    { }

    ISchemafulUnversionedReaderPtr GetReader() const
    {
        return Reader_;
    }

    IUnversionedRowsetWriterPtr  GetWriter() const
    {
        return Writer_;
    }

    void Fail(const TError& error)
    {
        Data_->Fail(error);
    }

private:
    TDataPtr Data_;
    TReaderPtr Reader_;
    TWriterPtr Writer_;

};

////////////////////////////////////////////////////////////////////////////////

TSchemafulPipe::TSchemafulPipe(IMemoryChunkProviderPtr chunkProvider)
    : Impl_(New<TImpl>(std::move(chunkProvider)))
{ }

TSchemafulPipe::~TSchemafulPipe() = default;

ISchemafulUnversionedReaderPtr TSchemafulPipe::GetReader() const
{
    return Impl_->GetReader();
}

IUnversionedRowsetWriterPtr TSchemafulPipe::GetWriter() const
{
    return Impl_->GetWriter();
}

void TSchemafulPipe::Fail(const TError& error)
{
    Impl_->Fail(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
