#pragma once

#include "interconnect_common.h"
#include "interconnect_session_pool_mapping.h"

#include <ydb/library/actors/core/actorsystem.h>

namespace NActors {

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common,
        TInterconnectSessionPoolMapping poolMapping, class TInterconnectMock *mock = nullptr);

    TProxyWrapperFactory CreateProxyWrapperFactory(TIntrusivePtr<TInterconnectProxyCommon> common, ui32 poolId,
        class TInterconnectMock *mock = nullptr);

}
