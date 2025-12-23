#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

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

inline TDuration ParseDuration(TStringBuf str) {
    StripInPlace(str);
    if (!str.empty() && !IsAsciiAlpha(str.back())) {
        throw TMisuseException() << "Duration must end with a unit name (ex. 'h' for hours, 's' for seconds)";
    }
    return TDuration::Parse(str);
}

} // namespace NYdb::NConsoleClient
