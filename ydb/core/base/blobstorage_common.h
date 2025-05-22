#pragma once

#include <ydb/core/base/id_wrapper.h>

namespace NKikimr{

    struct TGroupIdTag;
    using TGroupId = TIdWrapper<ui32, TGroupIdTag>;

    struct TBridgePileTag;
    using TBridgePileId = TIdWrapper<ui32, TBridgePileTag>;
    
}


