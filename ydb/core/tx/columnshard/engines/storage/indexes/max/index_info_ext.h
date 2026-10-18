#pragma once
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap::NIndexes::NMax {

std::shared_ptr<TIndexMeta> GetIndexMeta(const TIndexInfo& indexInfo, const ui32 columnId);

}   // namespace NKikimr::NOlap::NIndexes::NMax
