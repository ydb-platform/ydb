#include "local_rate_limiter_allocator.h"

#include <ydb/core/quoter/public/quoter.h>

#include <util/generic/hash_multi_map.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <limits>

namespace NKikimr::NSQS {
namespace {

constexpr ui32 SQS_TAG_START = 1 << 31;

ui32 NextTag = SQS_TAG_START;
THashMultiMap<ui32, ui32> FreeRateToTag;
TAdaptiveLock RatesLock;

ui32 AllocateLocalRateLimiterTag(ui32 rate) {
    Y_ABORT_UNLESS(rate != std::numeric_limits<ui32>::max());
    auto lock = Guard(RatesLock);
    auto freeTagIt = FreeRateToTag.find(rate);
    if (freeTagIt != FreeRateToTag.end()) {
        const ui32 tag = freeTagIt->second;
        FreeRateToTag.erase(freeTagIt);
        return tag;
    }
    return NextTag++;
}

void FreeLocalRateLimiterTag(ui32 rate, ui32 tag) {
    auto lock = Guard(RatesLock);
    FreeRateToTag.emplace(rate, tag);
}

}

TLocalRateLimiterResource::TLocalRateLimiterResource()
    : Rate(std::numeric_limits<ui32>::max())
    , Tag(std::numeric_limits<ui32>::max())
    , ResourceId(std::numeric_limits<ui64>::max())
{
}

TLocalRateLimiterResource::TLocalRateLimiterResource(ui32 rate)
    : Rate(rate)
    , Tag(AllocateLocalRateLimiterTag(Rate))
    , ResourceId(TEvQuota::TResourceLeaf::MakeTaggedRateRes(Tag, Rate))
{
}

TLocalRateLimiterResource::TLocalRateLimiterResource(TLocalRateLimiterResource&& res)
    : Rate(res.Rate)
    , Tag(res.Tag)
    , ResourceId(res.ResourceId)
{
    res.Rate = std::numeric_limits<ui32>::max();
    res.Tag = std::numeric_limits<ui32>::max();
    res.ResourceId = std::numeric_limits<ui64>::max();
}

TLocalRateLimiterResource& TLocalRateLimiterResource::operator=(TLocalRateLimiterResource&& res) {
    if (this != &res) {
        if (Rate != std::numeric_limits<ui32>::max()) {
            FreeLocalRateLimiterTag(Rate, Tag);
        }
        Rate = res.Rate;
        Tag = res.Tag;
        ResourceId = res.ResourceId;
        res.Rate = std::numeric_limits<ui32>::max();
        res.Tag = std::numeric_limits<ui32>::max();
        res.ResourceId = std::numeric_limits<ui64>::max();
    }
    return *this;
}

TLocalRateLimiterResource::~TLocalRateLimiterResource() {
    if (Rate != std::numeric_limits<ui32>::max()) {
        FreeLocalRateLimiterTag(Rate, Tag);
    }
}

} // namespace NKikimr::NSQS
