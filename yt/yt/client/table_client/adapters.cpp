#include "adapters.h"

#include "private.h"
#include "schema.h"
#include "row_batch.h"

#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/periodic_yielder.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NFormats;

using NProfiling::TWallTimer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<ITableWriter> TInterface, std::derived_from<IUnversionedWriter> TUnderlyingInterface>
class TApiFromSchemalessWriterAdapter
    : public TInterface
{
public:
    explicit TApiFromSchemalessWriterAdapter(TIntrusivePtr<TUnderlyingInterface> underlyingWriter)
        : UnderlyingWriter_(std::move(underlyingWriter))
    { }

    bool Write(TRange<TUnversionedRow> rows) /*override*/
    {
        return UnderlyingWriter_->Write(rows);
    }

    TFuture<void> GetReadyEvent() /*override*/
    {
        return UnderlyingWriter_->GetReadyEvent();
    }

    TFuture<void> Close() /*override*/
    {
        return UnderlyingWriter_->Close();
    }

    const TNameTablePtr& GetNameTable() const /*override*/
    {
        return UnderlyingWriter_->GetNameTable();
    }

    const TTableSchemaPtr& GetSchema() const /*override*/
    {
        return UnderlyingWriter_->GetSchema();
    }

protected:
    const TIntrusivePtr<TUnderlyingInterface> UnderlyingWriter_;
};

ITableWriterPtr CreateApiFromSchemalessWriterAdapter(
    IUnversionedWriterPtr underlyingWriter)
{
    return New<TApiFromSchemalessWriterAdapter<ITableWriter, IUnversionedWriter>>(std::move(underlyingWriter));
}

////////////////////////////////////////////////////////////////////////////////

class TApiFromSchemalessTableFragmentWriterAdapter
    : public TApiFromSchemalessWriterAdapter<ITableFragmentWriter, IUnversionedTableFragmentWriter>
{
public:
    using TBase = TApiFromSchemalessWriterAdapter<ITableFragmentWriter, IUnversionedTableFragmentWriter>;
    using TBase::TBase;

    TSignedWriteFragmentResultPtr GetWriteFragmentResult() const /*override*/
    {
        return TBase::UnderlyingWriter_->GetWriteFragmentResult();
    }
};

ITableFragmentWriterPtr CreateApiFromSchemalessWriterAdapter(
    IUnversionedTableFragmentWriterPtr underlyingWriter)
{
    return New<TApiFromSchemalessTableFragmentWriterAdapter>(std::move(underlyingWriter));
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessApiFromWriterAdapter
    : public IUnversionedWriter
{
public:
    TSchemalessApiFromWriterAdapter(
        IRowBatchWriterPtr underlyingWriter,
        TTableSchemaPtr schema)
        : UnderlyingWriter_(std::move(underlyingWriter))
        , Schema_(std::move(schema))
    { }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        return UnderlyingWriter_->Write(rows);
    }

    TFuture<void> GetReadyEvent() override
    {
        return UnderlyingWriter_->GetReadyEvent();
    }

    TFuture<void> Close() override
    {
        return UnderlyingWriter_->Close();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return UnderlyingWriter_->GetNameTable();
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    std::optional<TMD5Hash> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    const IRowBatchWriterPtr UnderlyingWriter_;
    const TTableSchemaPtr Schema_;
};

IUnversionedWriterPtr CreateSchemalessFromApiWriterAdapter(
    IRowBatchWriterPtr underlyingWriter)
{
    return New<TSchemalessApiFromWriterAdapter>(std::move(underlyingWriter), New<TTableSchema>());
}

IUnversionedWriterPtr CreateSchemalessFromApiWriterAdapter(
    ITableWriterPtr underlyingWriter)
{
    return New<TSchemalessApiFromWriterAdapter>(underlyingWriter, underlyingWriter->GetSchema());
}

////////////////////////////////////////////////////////////////////////////////

void PipeReaderToWriter(
    const IRowBatchReaderPtr& reader,
    const IUnversionedRowsetWriterPtr& writer,
    const TPipeReaderToWriterOptions& options)
{
    auto yielder = CreatePeriodicYielder(TDuration::Seconds(1));

    TRowBatchReadOptions readOptions{
        .MaxRowsPerRead = options.BufferRowCount,
        .MaxDataWeightPerRead = options.BufferDataWeight
    };
    while (auto batch = reader->Read(readOptions)) {
        yielder.TryYield();

        TSharedRange<TUnversionedRow> rows;

        try {
            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            rows = batch->MaterializeRows();

            if (options.ValidateValues) {
                for (auto row : rows) {
                    for (const auto& value : row) {
                        ValidateStaticValue(value);
                    }
                }
            }

            if (options.Throttler) {
                i64 dataWeight = 0;
                for (auto row : rows) {
                    dataWeight += GetDataWeight(row);
                }
                WaitFor(options.Throttler->Throttle(dataWeight))
                    .ThrowOnError();
            }

            if (!rows.empty() && options.PipeDelay) {
                TDelayedExecutor::WaitForDuration(options.PipeDelay);
            }
        } catch (const std::exception& ex) {
            if (options.ReaderErrorWrapper) {
                THROW_ERROR options.ReaderErrorWrapper(ex);
            } else {
                throw;
            }
        }

        if (!writer->Write(rows)) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    WaitFor(writer->Close())
        .ThrowOnError();
}

void PipeReaderToWriterByBatches(
    const IRowBatchReaderPtr& reader,
    const ISchemalessFormatWriterPtr& writer,
    TRowBatchReadOptions options,
    TCallback<void(TRowBatchReadOptions* mutableOptions, TDuration timeForBatch)> optionsUpdater,
    TDuration pipeDelay)
{
    try {
        auto yielder = CreatePeriodicYielder(TDuration::Seconds(1));

        for (bool isFirstBatch = true; auto batch = reader->Read(options); isFirstBatch = false) {
            yielder.TryYield();

            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            auto rowsRead = batch->GetRowCount();

            if (pipeDelay != TDuration::Zero()) {
                TDelayedExecutor::WaitForDuration(pipeDelay);
            }

            TWallTimer timer(/*start*/ false);

            if (optionsUpdater) {
                timer.Start();
            }

            if (!writer->WriteBatch(batch)) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }

            if (optionsUpdater && !isFirstBatch) {
                options.MaxRowsPerRead = rowsRead;
                optionsUpdater(&options, timer.GetElapsedTime());
            }
        }

        WaitFor(writer->Close())
            .ThrowOnError();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to transfer batches from reader to writer");

        throw;
    }
}

void PipeInputToOutput(
    IInputStream* input,
    IOutputStream* output,
    i64 bufferBlockSize)
{
    struct TWriteBufferTag { };
    TBlob buffer(GetRefCountedTypeCookie<TWriteBufferTag>(), bufferBlockSize, /*initializeStorage*/ false);

    auto yielder = CreatePeriodicYielder(TDuration::Seconds(1));

    while (true) {
        yielder.TryYield();

        size_t length = input->Read(buffer.Begin(), buffer.Size());
        if (length == 0) {
            break;
        }

        output->Write(buffer.Begin(), length);
    }

    output->Finish();
}

void PipeInputToOutput(
    const IAsyncInputStreamPtr& input,
    IOutputStream* output,
    i64 bufferBlockSize)
{
    struct TWriteBufferTag { };
    auto buffer = TSharedMutableRef::Allocate<TWriteBufferTag>(bufferBlockSize, {.InitializeStorage = false});

    while (true) {
        auto length = WaitFor(input->Read(buffer))
            .ValueOrThrow();

        if (length == 0) {
            break;
        }

        output->Write(buffer.Begin(), length);
    }

    output->Finish();
}

void PipeInputToOutput(
    const IAsyncZeroCopyInputStreamPtr& input,
    IOutputStream* output)
{
    while (true) {
        auto data = WaitFor(input->Read())
            .ValueOrThrow();

        if (!data) {
            break;
        }

        output->Write(data.Begin(), data.Size());
    }

    output->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
