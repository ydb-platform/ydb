#include "table_reader.h"
#include "row_batch_reader.h"
#include "helpers.h"

#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public TRowBatchReader
    , public ITableReader
{
public:
    TTableReader(
        IAsyncZeroCopyInputStreamPtr underlying,
        i64 startRowIndex,
        const std::vector<std::string>& omittedInaccessibleColumns,
        TTableSchemaPtr schema,
        const NProto::TRowsetStatistics& statistics)
        : TRowBatchReader(std::move(underlying), /*isStreamWithStatistics*/ true)
        , StartRowIndex_(startRowIndex)
        , TableSchema_(std::move(schema))
        , OmittedInaccessibleColumns_(omittedInaccessibleColumns)
    {
        ApplyStatistics(statistics);
    }

    i64 GetStartRowIndex() const override
    {
        return StartRowIndex_;
    }

    i64 GetTotalRowCount() const override
    {
        return TotalRowCount_;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = DataStatistics_;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);

        return dataStatistics;
    }

    const TTableSchemaPtr& GetTableSchema() const override
    {
        return TableSchema_;
    }

    const std::vector<std::string>& GetOmittedInaccessibleColumns() const override
    {
        return OmittedInaccessibleColumns_;
    }

private:
    const i64 StartRowIndex_;
    const TTableSchemaPtr TableSchema_;
    const std::vector<std::string> OmittedInaccessibleColumns_;

    NChunkClient::NProto::TDataStatistics DataStatistics_;
    i64 TotalRowCount_;

    void ApplyStatistics(const NProto::TRowsetStatistics& statistics) override
    {
        TotalRowCount_ = statistics.total_row_count();
        DataStatistics_ = statistics.data_statistics();
    }
};

TFuture<ITableReaderPtr> CreateTableReader(IAsyncZeroCopyInputStreamPtr inputStream)
{
    return inputStream->Read().Apply(BIND([=] (const TSharedRef& metaRef) {
        NApi::NRpcProxy::NProto::TRspReadTableMeta meta;
        if (!TryDeserializeProto(&meta, metaRef)) {
            THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
        }

        return New<TTableReader>(
            inputStream,
            meta.start_row_index(),
            FromProto<std::vector<std::string>>(meta.omitted_inaccessible_columns()),
             NYT::FromProto<TTableSchemaPtr>(meta.schema()),
            meta.statistics());
    })).As<ITableReaderPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
