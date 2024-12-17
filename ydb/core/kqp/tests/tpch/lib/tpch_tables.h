#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdb::NTpch {

extern const THashMap<TStringBuf, NTable::TTableDescription> TABLES;

} // NYdb::NTpch
