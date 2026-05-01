#include "operation_name.h"

#include <string_view>

namespace NYdb::inline Dev::NObservability {

std::string NormalizeOperationName(const std::string& requestName) {
    static constexpr std::string_view kPrefix = "ydb.";
    if (requestName.size() >= kPrefix.size()
        && std::string_view(requestName.data(), kPrefix.size()) == kPrefix)
    {
        return requestName;
    }
    return std::string(kPrefix) + requestName;
}

} // namespace NYdb::NObservability
