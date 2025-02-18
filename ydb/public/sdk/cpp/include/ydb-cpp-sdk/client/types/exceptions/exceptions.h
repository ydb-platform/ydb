#pragma once

#include <ydb-cpp-sdk/client/types/fwd.h>

#include <util/generic/yexception.h>

namespace NYdb::inline V3 {

class TYdbException : public yexception {
public:
    using yexception::yexception;
    TYdbException(const std::string& reason);
};

class TContractViolation : public TYdbException {
public:
    TContractViolation(const std::string& reason);
};

} // namespace NYdb
