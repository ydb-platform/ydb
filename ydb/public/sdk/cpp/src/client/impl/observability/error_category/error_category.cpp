#include "error_category.h"

#include <cstddef>

namespace NYdb::inline Dev::NObservability {

std::string_view CategorizeErrorType(EStatus status) noexcept {
    return static_cast<std::size_t>(status) >= TRANSPORT_STATUSES_FIRST
        ? kErrorTypeTransport
        : kErrorTypeYdb;
}

} // namespace NYdb::NObservability
