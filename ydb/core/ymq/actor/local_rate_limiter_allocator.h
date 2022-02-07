#pragma once
#include "defs.h"
#include <util/system/types.h>

namespace NKikimr::NSQS {

// Properly allocates resource id for local rate limiter
class TLocalRateLimiterResource {
public:
    TLocalRateLimiterResource();
    explicit TLocalRateLimiterResource(ui32 rate);
    TLocalRateLimiterResource(const TLocalRateLimiterResource&) = delete;
    TLocalRateLimiterResource(TLocalRateLimiterResource&&);
    ~TLocalRateLimiterResource();

    TLocalRateLimiterResource& operator=(const TLocalRateLimiterResource&) = delete;
    TLocalRateLimiterResource& operator=(TLocalRateLimiterResource&&);

    operator ui64() const {
        return ResourceId;
    }

private:
    ui32 Rate;
    ui32 Tag;
    ui64 ResourceId;
};

} // namespace NKikimr::NSQS
