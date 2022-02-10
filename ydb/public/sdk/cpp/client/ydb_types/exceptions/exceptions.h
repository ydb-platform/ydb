#pragma once

#include <util/generic/yexception.h>

namespace NYdb {

class TYdbException : public yexception {
public:
    using yexception::yexception;
    TYdbException(const TString& reason);
};

class TContractViolation : public TYdbException {
public:
    TContractViolation(const TString& reason);
};

} // namespace NYdb
