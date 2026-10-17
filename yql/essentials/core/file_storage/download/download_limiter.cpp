#include "download_limiter.h"

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/streams/special/throttle.h>

#include <util/system/guard.h>
#include <util/system/mutex.h>

namespace NYql {

class TDownloadLimiter::TImpl {
public:
    explicit TImpl(NSize::TSize bytesPerSecond)
        : Throttle_(TThrottle::TOptions::FromMaxPerInterval(
              bytesPerSecond.GetValue() == 0 ? Max<size_t>() : bytesPerSecond.GetValue(),
              TDuration::Seconds(1)))
    {
    }

    ui64 GetQuota(ui64 maxBytes) {
        if (maxBytes == 0) {
            return 0;
        }
        const auto guard = Guard(Mutex_);
        return Throttle_.GetQuota(maxBytes);
    }

private:
    TMutex Mutex_;
    TThrottle Throttle_;
};

TDownloadLimiter::TDownloadLimiter(NSize::TSize bytesPerSecond)
    : Impl_(MakeAtomicShared<TImpl>(bytesPerSecond))
{
}

TDownloadLimiter::TDownloadLimiter(const TDownloadLimiter&) = default;
TDownloadLimiter::TDownloadLimiter(TDownloadLimiter&&) noexcept = default;
TDownloadLimiter& TDownloadLimiter::operator=(const TDownloadLimiter&) = default;
TDownloadLimiter& TDownloadLimiter::operator=(TDownloadLimiter&&) noexcept = default;
TDownloadLimiter::~TDownloadLimiter() = default;

ui64 TDownloadLimiter::GetQuota(ui64 maxBytes) const {
    YQL_ENSURE(Impl_, "Download limiter is not initialized");
    return Impl_->GetQuota(maxBytes);
}

} // namespace NYql
