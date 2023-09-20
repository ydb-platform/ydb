#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

namespace NYdb {
namespace NConsoleClient {

class TYdbErrorException : public yexception {
public:
    TYdbErrorException(NYdb::TStatus status)
        : Status(std::move(status))
    { }

    friend IOutputStream& operator<<(IOutputStream& out, const TYdbErrorException& e) {
        return out << e.Status;
    }

private:
    NYdb::TStatus Status;
};

inline void ThrowOnError(NYdb::TStatus status) {
    if (!status.IsSuccess()) {
        throw TYdbErrorException(status) << status;
    } else if (status.GetIssues()) {
        Cerr << status;
    }
}

inline void ThrowOnError(const NYdb::TOperation& operation) {
    if (!operation.Ready())
        return;
    ThrowOnError(operation.Status());
}

}
}
