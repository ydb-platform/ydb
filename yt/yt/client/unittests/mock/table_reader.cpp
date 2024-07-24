#include "table_reader.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TMockTableReader::TMockTableReader(const NTableClient::TTableSchemaPtr& schema)
    : Schema_(schema)
    , NameTable_(NTableClient::TNameTable::FromSchema(*Schema_))
{}

const NTableClient::TNameTablePtr& TMockTableReader::GetNameTable() const
{
    return NameTable_;
}

const NTableClient::TTableSchemaPtr& TMockTableReader::GetTableSchema() const
{
    return Schema_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
