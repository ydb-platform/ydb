#pragma once

#include "column_converter.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NColumnConverters {

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateBooleanColumnConverter(int columnId, const NTableClient::TColumnSchema& columnSchema, int columnOffset);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
