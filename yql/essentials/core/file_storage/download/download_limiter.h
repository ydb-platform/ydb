#pragma once

#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <util/generic/ptr.h>

namespace NYql {

// Copies share a single thread-safe download quota.
class TDownloadLimiter {
public:
    explicit TDownloadLimiter(NSize::TSize bytesPerSecond);
    TDownloadLimiter(const TDownloadLimiter&);
    TDownloadLimiter(TDownloadLimiter&&) noexcept;
    TDownloadLimiter& operator=(const TDownloadLimiter&);
    TDownloadLimiter& operator=(TDownloadLimiter&&) noexcept;
    ~TDownloadLimiter();

    [[nodiscard]] ui64 GetQuota(ui64 maxBytes) const;

private:
    class TImpl;
    TAtomicSharedPtr<TImpl> Impl_;
};

} // namespace NYql
