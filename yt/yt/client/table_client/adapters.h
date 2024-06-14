#pragma once

#include "public.h"
#include "unversioned_writer.h"

#include <yt/yt/client/api/table_reader.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemalessFromApiWriterAdapter(
    NApi::ITableWriterPtr underlyingWriter);

NApi::ITableWriterPtr CreateApiFromSchemalessWriterAdapter(
    IUnversionedWriterPtr underlyingWriter);

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
    const NApi::ITableReaderPtr& reader,
    const IUnversionedRowsetWriterPtr& writer,
    const TPipeReaderToWriterOptions& options);

//! Parameter #pipeDelay is used only for testing.
void PipeReaderToWriterByBatches(
    const NApi::ITableReaderPtr& reader,
    const NFormats::ISchemalessFormatWriterPtr& writer,
    const TRowBatchReadOptions& options,
    TDuration pipeDelay = TDuration::Zero());

void PipeInputToOutput(
    IInputStream* input,
    IOutputStream* output,
    i64 bufferBlockSize);

void PipeInputToOutput(
    const NConcurrency::IAsyncInputStreamPtr& input,
    IOutputStream* output,
    i64 bufferBlockSize);

void PipeInputToOutput(
    const NConcurrency::IAsyncZeroCopyInputStreamPtr& input,
    IOutputStream* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
