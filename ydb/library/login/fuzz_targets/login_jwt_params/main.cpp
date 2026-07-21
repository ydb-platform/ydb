#include <ydb/library/login/fuzz_targets/login_fuzz_common.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/json/json_writer.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/str.h>

#include <ydb/library/login/login.h>
#include <ydb/public/lib/jwt/jwt.h>

namespace {

NYdb::TJwtParams GetValidJwtParams() {
    static const NYdb::TJwtParams params = [] {
        NLogin::TLoginProvider provider;
        provider.RotateKeys();
        const auto& key = provider.Keys.back();
        return NYdb::TJwtParams{
            .PrivKey = std::string(key.PrivateKey.data(), key.PrivateKey.size()),
            .PubKey = std::string(key.PublicKey.data(), key.PublicKey.size()),
            .AccountId = "service-account",
            .KeyId = "test-key",
        };
    }();
    return params;
}

TString BuildJwtParamsJson(FuzzedDataProvider& fdp, bool useValidKeys) {
    const auto valid = GetValidJwtParams();
    const TString keyId = NAuthSecurityFuzz::ConsumeToken(fdp, 24);
    const TString accountId = NAuthSecurityFuzz::ConsumeToken(fdp, 24);
    TString publicKey;
    TString privateKey;
    if (useValidKeys) {
        publicKey = TString(valid.PubKey.data(), valid.PubKey.size());
        privateKey = TString(valid.PrivKey.data(), valid.PrivKey.size());
    } else {
        publicKey = fdp.ConsumeRandomLengthString(2048);
        privateKey = fdp.ConsumeRandomLengthString(4096);
    }

    NJson::TJsonValue json(NJson::JSON_MAP);
    json["id"] = keyId;
    if (fdp.ConsumeBool()) {
        json["service_account_id"] = accountId;
    }
    if (fdp.ConsumeBool()) {
        json["user_account_id"] = accountId;
    }
    json["public_key"] = publicKey;
    json["private_key"] = privateKey;
    if (fdp.ConsumeBool()) {
        json[NAuthSecurityFuzz::ConsumeToken(fdp, 8)] = NAuthSecurityFuzz::ConsumeToken(fdp, 16);
    }

    TStringStream output;
    NJson::WriteJson(&output, &json, false, true);
    return output.Str();
}

void TrySignAndDecode(const NYdb::TJwtParams& params, TDuration lifetime) {
    const std::string token = NYdb::MakeSignedJwt(params, lifetime);
    const TString tokenString(token.data(), token.size());
    (void)NLogin::TLoginProvider::CanDecodeToken(tokenString);
    (void)NLogin::TLoginProvider::GetTokenAudience(tokenString);
    (void)NLogin::TLoginProvider::GetTokenExpiresAt(tokenString);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    TString raw(reinterpret_cast<const char*>(data), size);
    FuzzedDataProvider fdp(data, size);

    try {
        const auto parsed = NYdb::ParseJwtParams(std::string(raw.data(), raw.size()));
        if (fdp.ConsumeBool()) {
            TrySignAndDecode(parsed, TDuration::MilliSeconds(fdp.ConsumeIntegralInRange<ui32>(0, 600000)));
        }
    } catch (...) {
    }

    try {
        const TString json = BuildJwtParamsJson(fdp, fdp.ConsumeBool());
        const auto parsed = NYdb::ParseJwtParams(std::string(json.data(), json.size()));
        TrySignAndDecode(parsed, TDuration::MilliSeconds(fdp.ConsumeIntegralInRange<ui32>(0, 600000)));
    } catch (...) {
    }

    try {
        auto params = GetValidJwtParams();
        const TString accountId = NAuthSecurityFuzz::ConsumeToken(fdp, 32);
        const TString keyId = NAuthSecurityFuzz::ConsumeToken(fdp, 32);
        params.AccountId.assign(accountId.data(), accountId.size());
        params.KeyId.assign(keyId.data(), keyId.size());
        TrySignAndDecode(params, TDuration::MilliSeconds(fdp.ConsumeIntegralInRange<ui32>(0, 600000)));
    } catch (...) {
    }

    return 0;
}
