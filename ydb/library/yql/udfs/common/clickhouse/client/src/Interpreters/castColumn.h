#pragma once

#include <Core/ColumnWithTypeAndName.h>

namespace NDB
{

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, const FormatSettings & format_settings);
ColumnPtr castColumnAccurate(const ColumnWithTypeAndName & arg, const DataTypePtr & type, const FormatSettings & format_settings);
ColumnPtr castColumnAccurateOrNull(const ColumnWithTypeAndName & arg, const DataTypePtr & type, const FormatSettings & format_settings);

}
