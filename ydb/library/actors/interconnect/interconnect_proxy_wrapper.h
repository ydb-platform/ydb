#pragma once

#include "interconnect_common.h"

#include <ydb/library/actors/core/actorsystem.h>

namespace NActors {

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 poolId,
        class TInterconnectMock *mock = nullptr);

}
