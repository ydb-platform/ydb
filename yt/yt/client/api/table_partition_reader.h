#pragma once

#include "public.h"
#include "private.h"

#include "row_batch_reader.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class ITablePartitionReader
    : public virtual IRowBatchReader
{
private:
    // Functions below are for internal usage only.
    virtual std::vector<NTableClient::TTableSchemaPtr> GetTableSchemas() const = 0;
    virtual std::vector<NTableClient::TColumnNameFilter> GetColumnFilters() const = 0;

    friend std::vector<NTableClient::TTableSchemaPtr> NDetail::GetTableSchemas(const ITablePartitionReaderPtr&);
    friend std::vector<NTableClient::TColumnNameFilter> NDetail::GetColumnFilters(const ITablePartitionReaderPtr&);
};

DEFINE_REFCOUNTED_TYPE(ITablePartitionReader)

ITablePartitionReaderPtr CreateTablePartitionReader(
    const IRowBatchReaderPtr& rowBatchReader,
    const std::vector<NTableClient::TTableSchemaPtr>& tableSchemas,
    const std::vector<NTableClient::TColumnNameFilter>& columnFilters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
