#pragma once

#include <ydb/core/base/id_wrapper.h>

namespace NKikimr{

    using TGroupId = TIdWrapper<ui32, TGroupIdTag>;
    
    inline bool IsDynamicGroup(TGroupId groupId) {
        return groupId.GetRawId() & 0x80000000;
    }
}


