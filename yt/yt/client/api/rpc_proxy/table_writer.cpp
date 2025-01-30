#include "table_writer.h"
#include "row_batch_writer.h"

#include <yt/yt/client/api/table_writer.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTableWriter
    : public TRowBatchWriter
    , public ITableWriter
{
public:
    TTableWriter(
        IAsyncZeroCopyOutputStreamPtr underlying,
        TTableSchemaPtr schema)
        : TRowBatchWriter(std::move(underlying))
        , Schema_(std::move(schema))
    {
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

private:
    const TTableSchemaPtr Schema_;
};

ITableWriterPtr CreateTableWriter(
    IAsyncZeroCopyOutputStreamPtr outputStream,
    TTableSchemaPtr schema)
{
    return New<TTableWriter>(std::move(outputStream), std::move(schema));
}

////////////////////////////////////////////////////////////////////////////////

class TTableFragmentWriter
    : public TRowBatchWriter
    , public ITableFragmentWriter
{
public:
    TTableFragmentWriter(
        IAsyncZeroCopyOutputStreamPtr underlying,
        TTableSchemaPtr schema,
        TFuture<TSignedWriteFragmentResultPtr> asyncResult)
        : TRowBatchWriter(std::move(underlying))
        , Schema_(std::move(schema))
        , AsyncResult_(std::move(asyncResult))
    {
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    TFuture<void> Close() override
    {
        return AllSucceeded<void>({TRowBatchWriter::Close(), AsyncResult_.AsVoid()});
    }

    TSignedWriteFragmentResultPtr GetWriteFragmentResult() const override
    {
        YT_VERIFY(AsyncResult_.IsSet());
        auto resultOrError = AsyncResult_.Get();
        YT_VERIFY(resultOrError.IsOK());
        return resultOrError.Value();
    }

private:
    const TTableSchemaPtr Schema_;
    const TFuture<TSignedWriteFragmentResultPtr> AsyncResult_;
};

ITableFragmentWriterPtr CreateTableFragmentWriter(
    NConcurrency::IAsyncZeroCopyOutputStreamPtr outputStream,
    TTableSchemaPtr tableSchema,
    TFuture<TSignedWriteFragmentResultPtr> asyncResult)
{
    return New<TTableFragmentWriter>(std::move(outputStream), std::move(tableSchema), std::move(asyncResult));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
