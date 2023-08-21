#pragma once
#include "converter.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

namespace NYT::NConverters {

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateInt64ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema);

std::unique_ptr<IColumnConverter> CreateUint64ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConverters
