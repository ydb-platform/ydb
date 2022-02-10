#include "exceptions.h"

namespace NYdb {

TYdbException::TYdbException(const TString& reason) {
    Append(reason);
}

TContractViolation::TContractViolation(const TString& reason)
    : TYdbException(reason) {}

} // namespace NYdb
