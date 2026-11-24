#pragma once

#include "interconnect_common.h"
#include "interconnect_session_pool_mapping.h"

#include <ydb/library/actors/core/actorsystem.h>

namespace NActors {

    using TIcProxyCreator = std::function<IActor*(const ui32, TInterconnectProxyCommon::TPtr common, IActor**)>;

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common,
        TInterconnectSessionPoolMapping poolMapping);

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 poolId);

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common,
        TInterconnectSessionPoolMapping poolMapping, TIcProxyCreator proxyCreator); // for tests

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 poolId,
        TIcProxyCreator proxyCreator); // for tests
}
