#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <string_view>

namespace NYdb::inline Dev::NObservability {

// Maps EStatus to OTel-style error.type category:
// "transport_error" for client/transport-layer statuses (>= TRANSPORT_STATUSES_FIRST),
// "ydb_error"       for server-side YDB statuses.
std::string_view CategorizeErrorType(EStatus status) noexcept;

// Canonical values for the OTel `error.type` attribute.
inline constexpr std::string_view kErrorTypeYdb = "ydb_error";
inline constexpr std::string_view kErrorTypeTransport = "transport_error";

} // namespace NYdb::NObservability
