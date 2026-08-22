#pragma once

#include "public.h"

#include <yt/yt/client/api/table_reader.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemalessFromApiWriterAdapter(
    NApi::IRowBatchWriterPtr underlyingWriter);

IUnversionedWriterPtr CreateSchemalessFromApiWriterAdapter(
    NApi::ITableWriterPtr underlyingWriter);

NApi::ITableWriterPtr CreateApiFromSchemalessWriterAdapter(
    IUnversionedWriterPtr underlyingWriter);

NApi::ITableFragmentWriterPtr CreateApiFromSchemalessWriterAdapter(
    IUnversionedTableFragmentWriterPtr underlyingWriter);

////////////////////////////////////////////////////////////////////////////////

struct TPipeReaderToWriterOptions
{
    i64 BufferRowCount = 10000;
    i64 BufferDataWeight = 16_MB;
    bool ValidateValues = false;
    NConcurrency::IThroughputThrottlerPtr Throttler;
    std::function<TError(TError readerError)> ReaderErrorWrapper;
    // Used only for testing.
    TDuration PipeDelay;
};

void PipeReaderToWriter(
    const NApi::IRowBatchReaderPtr& reader,
    const IUnversionedRowsetWriterPtr& writer,
    const TPipeReaderToWriterOptions& options);

struct TPipeReaderToWriterByBatchesOptions
{
    TRowBatchReadOptions StartingOptions;
    TCallback<void(TRowBatchReadOptions* mutableOptions, TDuration timeForBatch)> OptionsUpdater;
    std::function<TError(TError readerError)> ReaderErrorWrapper;
    //! Used only for testing.
    TDuration PipeDelay;
};

void PipeReaderToWriterByBatches(
    const NApi::IRowBatchReaderPtr& reader,
    const NFormats::ISchemalessFormatWriterPtr& writer,
    const TPipeReaderToWriterByBatchesOptions& options);

i64 PipeInputToOutput(
    IInputStream* input,
    IOutputStream* output,
    i64 bufferBlockSize);

i64 PipeInputToOutput(
    const NConcurrency::IAsyncInputStreamPtr& input,
    IOutputStream* output,
    i64 bufferBlockSize);

i64 PipeInputToOutput(
    const NConcurrency::IAsyncZeroCopyInputStreamPtr& input,
    IOutputStream* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
