#pragma once

#include <util/generic/hash.h>
#include <util/system/types.h>

#include <cstddef>
#include <memory>

namespace NKikimr::NOlap {
class TWrittenPortionInfo;
using TWrittenPortionsById = THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>;
}
