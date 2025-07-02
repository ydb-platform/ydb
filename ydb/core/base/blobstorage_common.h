#pragma once

#include <ydb/core/base/id_wrapper.h>

namespace NKikimr{

    struct TGroupIdTag;
    using TGroupId = TIdWrapper<ui32, TGroupIdTag>;

    struct TBridgePileTag;
    using TBridgePileId = TIdWrapper<ui32, TBridgePileTag>;
    
    inline bool IsDynamicGroup(TGroupId groupId) {
        return groupId.GetRawId() & 0x80000000;
    }
}


