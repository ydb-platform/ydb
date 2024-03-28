#pragma once

#include "defs.h"
#include "two_part_description.h"

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/map.h>

namespace NKikimr {

IActor* CreateSchemeBoardPopulator(
    const ui64 owner,
    const ui64 generation,
    std::vector<std::pair<TPathId, NSchemeBoard::TTwoPartDescription>>&& twoPartDescriptions,
    const ui64 maxPathId
);

} // NKikimr
