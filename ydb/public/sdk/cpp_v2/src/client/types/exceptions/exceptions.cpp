#include <ydb-cpp-sdk/client/types/exceptions/exceptions.h>

namespace NYdb {

TYdbException::TYdbException(const std::string& reason) {
    Append(reason);
}

TContractViolation::TContractViolation(const std::string& reason)
    : TYdbException(reason) {}

} // namespace NYdb
