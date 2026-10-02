#pragma once

#include "defs.h"
#include "two_part_description.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/map.h>

namespace NKikimr {

namespace NSchemeBoard {

    bool ShouldIgnore(const TStateStorageInfo::TRingGroup& ringGroup);
    bool ShouldIgnoreInQuorum(const TStateStorageInfo::TRingGroup& ringGroup);
    bool IsMajorityReached(const TStateStorageInfo::TRingGroup& ringGroup, ui32 ringGroupAcks);

}

IActor* CreateSchemeBoardPopulator(
    const ui64 owner,
    const ui64 generation,
    std::vector<std::pair<TPathId, NSchemeBoard::TTwoPartDescription>>&& twoPartDescriptions,
    const ui64 maxPathId
);

} // NKikimr
