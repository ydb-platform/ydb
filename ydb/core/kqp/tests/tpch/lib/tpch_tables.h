#pragma once

#include <ydb-cpp-sdk/client/table/table.h>

namespace NYdb::NTpch {

extern const THashMap<TStringBuf, NTable::TTableDescription> TABLES;

} // NYdb::NTpch
