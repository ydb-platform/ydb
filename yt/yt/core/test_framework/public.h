#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using TServiceMap = THashMap<std::string, NRpc::IServicePtr>;
using TRealmIdServiceMap = THashMap<NRpc::TRealmId, TServiceMap>;

DECLARE_REFCOUNTED_CLASS(TTestService);
DECLARE_REFCOUNTED_CLASS(TTestChannelFactory);
DECLARE_REFCOUNTED_CLASS(TTestChannel);
DECLARE_REFCOUNTED_CLASS(TTestClientRequestControl);

namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTestBus);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

