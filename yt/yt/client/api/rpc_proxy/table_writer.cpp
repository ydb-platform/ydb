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

} // namespace NYT::NApi::NRpcProxy
