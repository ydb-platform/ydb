#pragma once

#include <ydb/core/ymq/base/table_info.h>

namespace NKikimr::NSQS {

TVector<TTable> GetStandardTables(ui64 shards, ui64 partitions, bool enableAutosplit, ui64 sizeToSplit);

TVector<TTable> GetStandardTableNames(ui64 shards);

} // namespace NKikimr::NSQS
