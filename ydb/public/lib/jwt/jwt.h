#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>
#include <util/datetime/base.h>

namespace NYdb {

struct TJwtParams {
    TStringType PrivKey;
    TStringType PubKey;
    TStringType AccountId;
    TStringType KeyId;
};

TJwtParams ParseJwtParams(const TStringType& jsonParamsStr);
TStringType MakeSignedJwt(
    const TJwtParams& params,
    const TDuration& lifetime = TDuration::Hours(1)
);

} // namespace NYdb
