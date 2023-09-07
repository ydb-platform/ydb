#pragma once

#include "column_converter.h"

namespace NYT::NColumnConverters {

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateFloatingPoint32ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema);

IColumnConverterPtr CreateFloatingPoint64ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
