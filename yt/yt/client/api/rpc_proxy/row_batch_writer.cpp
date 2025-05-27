#include "row_batch_writer.h"
#include "row_stream.h"
#include "wire_row_stream.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TRowBatchWriter::TRowBatchWriter(IAsyncZeroCopyOutputStreamPtr underlying)
    : Underlying_(std::move(underlying))
    , Encoder_(CreateWireRowStreamEncoder(NameTable_))
{
    YT_VERIFY(Underlying_);
    NameTable_->SetEnableColumnNameValidation();
}

bool TRowBatchWriter::Write(TRange<TUnversionedRow> rows)
{
    YT_VERIFY(!Closed_);
    YT_VERIFY(ReadyEvent_.IsSet() && ReadyEvent_.Get().IsOK());

    auto batch = CreateBatchFromUnversionedRows(TSharedRange<TUnversionedRow>(rows, nullptr));

    auto block = Encoder_->Encode(batch, nullptr);

    ReadyEvent_ = NewPromise<void>();
    ReadyEvent_.TrySetFrom(Underlying_->Write(std::move(block)));

    return ReadyEvent_.IsSet() && ReadyEvent_.Get().IsOK();
}

TFuture<void> TRowBatchWriter::GetReadyEvent()
{
    return ReadyEvent_;
}

TFuture<void> TRowBatchWriter::Close()
{
    YT_VERIFY(!Closed_);
    Closed_ = true;

    return Underlying_->Close();
}

const TNameTablePtr& TRowBatchWriter::GetNameTable() const
{
    return NameTable_;
}

////////////////////////////////////////////////////////////////////////////////

IRowBatchWriterPtr CreateRowBatchWriter(
    IAsyncZeroCopyOutputStreamPtr outputStream)
{
    return New<TRowBatchWriter>(std::move(outputStream));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
