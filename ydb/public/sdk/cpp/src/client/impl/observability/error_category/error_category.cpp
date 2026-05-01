#include "error_category.h"

#include <cstddef>

namespace NYdb::inline Dev::NObservability {

std::string_view CategorizeErrorType(EStatus status) noexcept {
    return static_cast<std::size_t>(status) >= TRANSPORT_STATUSES_FIRST
        ? std::string_view("transport_error")
        : std::string_view("ydb_error");
}

} // namespace NYdb::NObservability
