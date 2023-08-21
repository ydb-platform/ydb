#pragma once

#include "converter.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NConverters {

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateStringConverter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema);

IColumnConverterPtr CreateAnyConverter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema);

IColumnConverterPtr CreateCompositeConverter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConverters
