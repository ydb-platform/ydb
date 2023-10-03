#pragma once

#include <util/generic/string.h>

namespace NDB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

}

namespace NYql::NSerialization {

class TSerializationInterval {
public:
    enum class EUnit
    {
        MICROSECONDS,
        MILLISECONDS,
        SECONDS,
        MINUTES,
        HOURS,
        DAYS,
        WEEKS
    };

public:
    static EUnit ToUnit(const TString& unit);
};

NDB::DataTypePtr GetInterval(TSerializationInterval::EUnit unit);

}
