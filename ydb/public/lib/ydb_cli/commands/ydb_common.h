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
        out << "Status: " << e.Status.GetStatus() << Endl;
        if (e.Status.GetIssues()) {
            out << "Issues: " << Endl;
            e.Status.GetIssues().PrintTo(out);
        }
        return out;
    }

private:
    NYdb::TStatus Status;
};

inline void ThrowOnError(NYdb::TStatus status) {
    if (status.IsSuccess()) {
        if (status.GetIssues()) {
            Cerr << "Status: " << status.GetStatus() << Endl;
            Cerr << "Issues: " << Endl;
            status.GetIssues().PrintTo(Cerr);
        }
    } else {
        throw TYdbErrorException(std::move(status));
    }
}

inline void ThrowOnError(const NYdb::TOperation& operation) {
    if (!operation.Ready())
        return;
    ThrowOnError(operation.Status());
}

}
}
