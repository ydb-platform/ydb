#pragma once

#include <CHDBPoco/Timespan.h>

namespace DB_CHDB
{

struct IHTTPContext
{
    virtual uint64_t getMaxHstsAge() const = 0;
    virtual uint64_t getMaxUriSize() const = 0;
    virtual uint64_t getMaxFields() const = 0;
    virtual uint64_t getMaxFieldNameSize() const = 0;
    virtual uint64_t getMaxFieldValueSize() const = 0;
    virtual CHDBPoco::Timespan getReceiveTimeout() const = 0;
    virtual CHDBPoco::Timespan getSendTimeout() const = 0;

    virtual ~IHTTPContext() = default;
};

using HTTPContextPtr = std::shared_ptr<IHTTPContext>;

}
