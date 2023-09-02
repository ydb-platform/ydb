#pragma once

#include "column_converter.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NColumnConverters {

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateInt64ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema);

std::unique_ptr<IColumnConverter> CreateUint64ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
