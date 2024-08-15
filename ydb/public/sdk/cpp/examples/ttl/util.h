#pragma once

#include <ydb-cpp-sdk/client/table/table.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <filesystem>

namespace NExample {

using namespace NYdb;
using namespace NYdb::NTable;

class TYdbErrorException : public yexception {
public:
    TYdbErrorException(const TStatus& status)
        : Status(status) {}

    TStatus Status;
};

inline void ThrowOnError(const TStatus& status) {
    if (!status.IsSuccess()) {
        throw TYdbErrorException(status) << status;
    }
}

inline void PrintStatus(const TStatus& status) {
    std::cerr << "Status: " << ToString(status.GetStatus()) << std::endl;
    std::cerr << status.GetIssues().ToString();
}

inline std::string JoinPath(const std::string& basePath, const std::string& path) {
    if (basePath.empty()) {
        return path;
    }

    std::filesystem::path prefixPathSplit(basePath);
    prefixPathSplit /= path;

    return prefixPathSplit;
}

}
