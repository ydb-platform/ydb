#pragma once
#include "defs.h"
#include <ydb/core/base/quoter.h>

namespace NKikimr {

struct TQuoterServiceConfig {
    TDuration ScheduleTickSize = TDuration::MilliSeconds(1);
 };

IActor* CreateQuoterService(const TQuoterServiceConfig &config = TQuoterServiceConfig());

}
