#include "rowset_writer.h"

#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSimpleRowsetWriter::TSimpleRowsetWriter(IMemoryChunkProviderPtr chunkProvider)
    : RowBuffer_(New<TRowBuffer>(TSchemafulRowsetWriterBufferTag(), std::move(chunkProvider)))
{ }

TSharedRange<TUnversionedRow> TSimpleRowsetWriter::GetRows() const
{
    return MakeSharedRange(Rows_, RowBuffer_);
}

TFuture<TSharedRange<TUnversionedRow>> TSimpleRowsetWriter::GetResult() const
{
    return Result_.ToFuture();
}

TFuture<void> TSimpleRowsetWriter::Close()
{
    Result_.TrySet(GetRows());
    return OKFuture;
}

bool TSimpleRowsetWriter::Write(TRange<TUnversionedRow> rows)
{
    for (auto row : rows) {
        Rows_.push_back(RowBuffer_->CaptureRow(row));
    }
    return true;
}

TFuture<void> TSimpleRowsetWriter::GetReadyEvent()
{
    return OKFuture;
}

void TSimpleRowsetWriter::Fail(const TError& error)
{
    Result_.TrySet(error);
}

std::optional<TRowsDigest> TSimpleRowsetWriter::GetDigest() const
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
