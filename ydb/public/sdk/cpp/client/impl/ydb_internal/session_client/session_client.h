#pragma once

#include <functional>

#include <util/datetime/base.h>

namespace NYdb {

class TKqpSessionCommon;

class ISessionClient {
public:
    virtual ~ISessionClient() = default;
    virtual void DeleteSession(TKqpSessionCommon* sessionImpl) = 0;
    // TODO: Try to remove from ISessionClient
    virtual bool ReturnSession(TKqpSessionCommon* sessionImpl) = 0;
};

}
