#pragma once

#include <util/datetime/base.h>

namespace NKikimr::NTokenManager {

struct TTokenProviderSettings {
    TDuration SuccessRefreshPeriod;
    TDuration MinErrorRefreshPeriod;
    TDuration MaxErrorRefreshPeriod;
    TDuration RequestTimeout;
};

} // NKikimr::NTokenManager
