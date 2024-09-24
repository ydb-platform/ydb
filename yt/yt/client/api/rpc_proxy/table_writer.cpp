#include "table_writer.h"
#include "helpers.h"
#include "row_stream.h"
#include "wire_row_stream.h"

#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTableWriter
    : public ITableWriter
{
public:
    TTableWriter(
        IAsyncZeroCopyOutputStreamPtr underlying,
        TTableSchemaPtr schema)
        : Underlying_(std::move(underlying))
        , Schema_(std::move(schema))
        , Encoder_(CreateWireRowStreamEncoder(NameTable_))
    {
        YT_VERIFY(Underlying_);
        NameTable_->SetEnableColumnNameValidation();
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        YT_VERIFY(!Closed_);
        YT_VERIFY(ReadyEvent_.IsSet() && ReadyEvent_.Get().IsOK());

        auto batch = CreateBatchFromUnversionedRows(TSharedRange<TUnversionedRow>(rows, nullptr));

        auto block = Encoder_->Encode(batch, nullptr);

        ReadyEvent_ = NewPromise<void>();
        ReadyEvent_.TrySetFrom(Underlying_->Write(std::move(block)));

        return ReadyEvent_.IsSet() && ReadyEvent_.Get().IsOK();
    }

    TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    TFuture<void> Close() override
    {
        YT_VERIFY(!Closed_);
        Closed_ = true;

        return Underlying_->Close();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

private:
    const IAsyncZeroCopyOutputStreamPtr Underlying_;
    const TTableSchemaPtr Schema_;

    const TNameTablePtr NameTable_ = New<TNameTable>();
    const IRowStreamEncoderPtr Encoder_;

    TPromise<void> ReadyEvent_ = MakePromise<void>(TError());
    bool Closed_ = false;
};

ITableWriterPtr CreateTableWriter(
    IAsyncZeroCopyOutputStreamPtr outputStream,
    TTableSchemaPtr schema)
{
    return New<TTableWriter>(std::move(outputStream), std::move(schema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
