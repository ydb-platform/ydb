#include "table_partition_reader.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NApi {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTablePartitionReader
    : public ITablePartitionReader
{
public:
    TTablePartitionReader(
        IRowBatchReaderPtr rowBatchReader,
        std::vector<TTableSchemaPtr> tableSchemas,
        std::vector<TColumnNameFilter> columnFilters)
        : RowBatchReader_(std::move(rowBatchReader))
        , TableSchemas_(std::move(tableSchemas))
        , ColumnFilters_(std::move(columnFilters))
    {
        YT_VERIFY(TableSchemas_.size() == ColumnFilters_.size());
    }

    TFuture<void> GetReadyEvent() const override
    {
        return RowBatchReader_->GetReadyEvent();
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return RowBatchReader_->Read(options);
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return RowBatchReader_->GetNameTable();
    }

    std::vector<TTableSchemaPtr> GetTableSchemas() const override
    {
        if (TableSchemas_.empty()) {
            THROW_ERROR_EXCEPTION("Table schemas are unknown");
        }
        return TableSchemas_;
    }

    std::vector<TColumnNameFilter> GetColumnFilters() const override
    {
        if (ColumnFilters_.empty()) {
            THROW_ERROR_EXCEPTION("Column filters are unknown");
        }
        return ColumnFilters_;
    }

private:
    const IRowBatchReaderPtr RowBatchReader_;
    const std::vector<TTableSchemaPtr> TableSchemas_;
    const std::vector<TColumnNameFilter> ColumnFilters_;
};

ITablePartitionReaderPtr CreateTablePartitionReader(
    const IRowBatchReaderPtr& rowBatchReader,
    const std::vector<TTableSchemaPtr>& tableSchemas,
    const std::vector<TColumnNameFilter>& columnFilters)
{
    return New<TTablePartitionReader>(rowBatchReader, tableSchemas, columnFilters);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
