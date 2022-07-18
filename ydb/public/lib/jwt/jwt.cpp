#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>

#include "jwt.h"

#include <library/cpp/json/json_reader.h>
#include <util/string/builder.h>

namespace NYdb {

TStringType MakeSignedJwt(const TJwtParams& params, const TDuration& lifetime) {
    // this constant works on all envs (internal IAM, preprod cloud, prod cloud)
    // according to potamus@, it's recommended audience
    static const TStringType AUDIENCE{"https://iam.api.cloud.yandex.net/iam/v1/tokens"};
    const auto now = std::chrono::system_clock::now();
    const auto expire = now + std::chrono::milliseconds(lifetime.MilliSeconds());
    const auto token = jwt::create()
        .set_key_id(params.KeyId)
        .set_issuer(params.AccountId)
        .set_issued_at(now)
        .set_audience(AUDIENCE)
        .set_expires_at(expire)
        .sign(jwt::algorithm::ps256(params.PubKey, params.PrivKey));
    return TStringType{token};
}

TJwtParams ParseJwtParams(const TStringType& jsonParamsStr) {
    NJson::TJsonValue json;
    NJson::ReadJsonTree(jsonParamsStr, &json, true);
    auto map = json.GetMap();
    TJwtParams result;
    auto iter = map.find("id");
    if (iter == map.end())
        ythrow yexception() << "doesn't have \"id\" key";
    result.KeyId = iter->second.GetString();
    iter = map.find("service_account_id");
    if (iter == map.end()) {
        iter = map.find("user_account_id");
        if (iter == map.end()) {
            ythrow yexception() << "doesn't have \"service_account_id\" nor \"user_account_id\" key";
        }
    } else if (map.find("user_account_id") != map.end()) {
        ythrow yexception() << "both \"service_account_id\" and \"user_account_id\" keys are provided";
    }
    result.AccountId = iter->second.GetString();
    iter = map.find("public_key");
    if (iter == map.end())
        ythrow yexception() << "doesn't have \"public_key\" key";
    result.PubKey = iter->second.GetString();
    iter = map.find("private_key");
    if (iter == map.end())
        ythrow yexception() << "doesn't have \"private_key\" key";
    result.PrivKey = iter->second.GetString();
    return result;
}

} // namespace NYdb

