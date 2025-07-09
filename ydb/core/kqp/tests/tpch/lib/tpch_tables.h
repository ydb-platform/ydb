#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/hash.h>
namespace NYdb::NTpch {

extern const THashMap<TStringBuf, NTable::TTableDescription> TABLES;

} // NYdb::NTpch
