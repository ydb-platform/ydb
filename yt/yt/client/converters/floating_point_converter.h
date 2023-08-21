#pragma once

#include "converter.h"

namespace NYT::NConverters {

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateFloatingPoint32ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema);

IColumnConverterPtr CreateFloatingPoint64ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConverters
