#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <cstddef>
#include <string_view>

namespace NYdb::inline Dev::NObservability {

inline std::string_view CategorizeErrorType(EStatus status) noexcept {
    return static_cast<std::size_t>(status) >= TRANSPORT_STATUSES_FIRST
        ? std::string_view("transport_error")
        : std::string_view("ydb_error");
}

} // namespace NYdb::NObservability
