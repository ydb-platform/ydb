#include "handlers.h"
#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>

namespace NYdb {

void ThrowFatalError(const TString& str) {
    throw TContractViolation(str);
}

} // namespace NYdb
