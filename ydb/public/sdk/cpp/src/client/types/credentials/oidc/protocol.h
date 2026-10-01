#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/threading/cancellation/cancellation_token.h>

#include <functional>

namespace NYdb::inline Dev::NOidc::NPrivate {

class TProtocol {
public:
    TProtocol(const TOidcConfig& config, NThreading::TCancellationToken cancellation);
    ~TProtocol();

    TTokenCache Refresh(const TOAuthToken& refresh);
    TTokenCache ClientGrant();
    TTokenCache DeviceGrant(const std::function<bool(TDuration)>& wait);

private:
    NJson::TJsonValue Request(const std::string& endpoint, const TCgiParameters* form, bool authenticate, TInstant deadline);
    void Discover();
    TTokenCache TokenRequest(TCgiParameters form, const std::optional<TOAuthToken>& refresh, TInstant deadline);

    const TOidcConfig& Config;
    NThreading::TCancellationToken Cancellation;
    std::string TokenEndpoint;
    std::string DeviceEndpoint;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
