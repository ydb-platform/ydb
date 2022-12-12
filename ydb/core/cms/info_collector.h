#pragma once

#include "defs.h"

namespace NKikimr::NCms {

IActor* CreateInfoCollector(const TActorId& client, const TDuration& timeout);

} // namespace NKikimr::NCms
