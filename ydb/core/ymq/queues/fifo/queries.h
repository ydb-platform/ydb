#pragma once

#include <ydb/core/ymq/base/query_id.h>

namespace NKikimr::NSQS {

const char* GetFifoQueryById(size_t id);

} // namespace NKikimr::NSQS
