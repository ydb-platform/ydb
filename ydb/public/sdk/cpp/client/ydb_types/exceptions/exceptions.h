#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/fwd.h>

#include <util/generic/yexception.h>

namespace NYdb::inline V2 {

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
