#pragma once
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/common.h>

namespace NKikimr::NOlap::NIndexes {

std::vector<std::shared_ptr<TSkipIndex>> FindSkipIndexes(
    const TIndexInfo& indexInfo, const NRequest::TOriginalDataAddress& originalDataAddress, const NArrow::NSSA::TIndexCheckOperation& op);

}   // namespace NKikimr::NOlap::NIndexes
