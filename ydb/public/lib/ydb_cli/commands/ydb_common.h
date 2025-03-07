#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/types/operation/operation.h>

namespace NYdb {
namespace NConsoleClient {

inline void ThrowOnError(const NYdb::Dev::TOperation& operation) {
    if (!operation.Ready())
        return;
    NStatusHelpers::ThrowOnError(operation.Status());
}

inline bool ThrowOnErrorAndCheckEOS(NYdb::Dev::TStreamPartStatus status) {
    if (!status.IsSuccess()) {
        if (status.EOS()) {
            return true;
        }
        throw NStatusHelpers::TYdbErrorException(status) << static_cast<NYdb::Dev::TStatus>(status);
    } else if (status.GetIssues()) {
        Cerr << static_cast<NYdb::Dev::TStatus>(status);
    }
    return false;
}

}
}
