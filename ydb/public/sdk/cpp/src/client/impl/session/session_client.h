#pragma once

#include <memory>

namespace NYdb::inline Dev {

class TKqpSessionCommon;

class ISessionClient {
public:
    virtual ~ISessionClient() = default;

    virtual void DeleteSession(TKqpSessionCommon* sessionImpl) = 0;

    virtual bool ReturnSession(TKqpSessionCommon* sessionImpl) = 0;
};

}
