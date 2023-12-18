#pragma once

#include "column_converter.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NColumnConverters {

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateInt64ColumnConverter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    int columnOffset);

std::unique_ptr<IColumnConverter> CreateUint64ColumnConverter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    int columnOffset);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
