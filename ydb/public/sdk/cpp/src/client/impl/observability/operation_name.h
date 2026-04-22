#pragma once

#include <string>
#include <string_view>

namespace NYdb::inline Dev::NObservability {

inline std::string NormalizeOperationName(const std::string& requestName) noexcept {
    static constexpr std::string_view kPrefix = "ydb.";
    if (requestName.size() >= kPrefix.size()
        && std::string_view(requestName.data(), kPrefix.size()) == kPrefix)
    {
        return requestName;
    }
    return std::string(kPrefix) + requestName;
}

} // namespace NYdb::NObservability
