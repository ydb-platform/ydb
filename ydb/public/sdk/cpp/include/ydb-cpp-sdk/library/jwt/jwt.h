#pragma once

#include <util/datetime/base.h>

namespace NYdb {
inline namespace Dev {

struct TJwtParams {
    std::string PrivKey;
    std::string PubKey;
    std::string AccountId;
    std::string KeyId;
};

TJwtParams ParseJwtParams(const std::string& jsonParamsStr);
std::string MakeSignedJwt(
    const TJwtParams& params,
    const TDuration& lifetime = TDuration::Hours(1)
);

}
}
