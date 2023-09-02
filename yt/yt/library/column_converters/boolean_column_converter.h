#pragma once

#include "column_converter.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NColumnConverters {

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateBooleanColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
