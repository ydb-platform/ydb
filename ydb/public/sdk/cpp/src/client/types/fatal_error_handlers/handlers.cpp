#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fatal_error_handlers/handlers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

namespace NYdb::inline Dev {

void ThrowFatalError(const std::string& str) {
    throw TContractViolation(str);
}

} // namespace NYdb
