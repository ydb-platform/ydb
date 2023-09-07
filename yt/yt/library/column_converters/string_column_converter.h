#pragma once

#include "column_converter.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NColumnConverters {

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

} // namespace NYT::NColumnConverters
