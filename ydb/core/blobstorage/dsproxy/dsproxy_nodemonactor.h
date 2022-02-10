#pragma once

#include "defs.h"
#include "dsproxy_nodemon.h"

namespace NKikimr {

IActor* CreateDsProxyNodeMon(TIntrusivePtr<TDsProxyNodeMon> mon);

} // NKikimr

