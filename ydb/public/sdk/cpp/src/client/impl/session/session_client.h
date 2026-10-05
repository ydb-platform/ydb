#pragma once

#include <cstdint>
#include <string_view>

namespace NYdb::inline Dev {

class TKqpSessionCommon;

class ISessionClient {
public:
    virtual ~ISessionClient() = default;

    virtual void DeleteSession(TKqpSessionCommon* sessionImpl) = 0;

    virtual void PessimizeNode(std::uint64_t nodeId) = 0;

    virtual void RecordSessionClosed(std::string_view) {
    }

    // TODO: Try to remove from ISessionClient
    virtual bool ReturnSession(TKqpSessionCommon* sessionImpl) = 0;
};

}
