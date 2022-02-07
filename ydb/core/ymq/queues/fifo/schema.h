#pragma once

#include <ydb/core/ymq/base/table_info.h>

namespace NKikimr::NSQS {

TVector<TTable> GetFifoTables();

} // namespace NKikimr::NSQS
