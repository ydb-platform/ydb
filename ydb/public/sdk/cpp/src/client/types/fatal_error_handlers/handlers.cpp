#include <ydb-cpp-sdk/client/types/fatal_error_handlers/handlers.h>
#include <ydb-cpp-sdk/client/types/exceptions/exceptions.h>

namespace NYdb::inline V3 {

void ThrowFatalError(const std::string& str) {
    throw TContractViolation(str);
}

} // namespace NYdb
