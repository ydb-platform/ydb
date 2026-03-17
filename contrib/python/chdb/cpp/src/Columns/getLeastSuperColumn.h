#pragma once

#include <Core/ColumnWithTypeAndName.h>


namespace DB_CHDB
{

/// getLeastSupertype + related column changes
ColumnWithTypeAndName getLeastSuperColumn(const std::vector<const ColumnWithTypeAndName *> & columns);

}
