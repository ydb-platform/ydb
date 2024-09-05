#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>

namespace NSQLTranslationV1 {

struct TTieringRule {
    TString TierName;
    TString SerializedDurationForEvict;
};

TString SerializeTieringRules(const std::vector<TTieringRule>& input);

class TTierSettings {
private:
    NKikimrSchemeOp::TStorageTierConfig Config;

public:
    TTierSettings(const TString& tierName) {
        Config.SetName(tierName);
    }

    bool StoreSetting(const TString& fieldName, const TString& value) {
        auto field = to_lower(fieldName);

        if (field == "scheme") {
            return StoreScheme(value);
        }
        if (field == "verify_ssl") {
            return StoreVerifySSL(value);
        }
        if (field == "endpoint") {
            return StoreEndpoint(value);
        }
        if (field == "bucket") {
            return StoreBucket(value);
        }
        if (field == "access_key") {
            return StoreAccessKey(value);
        }
        if (field == "secret_key") {
            return StoreSecretKey(value);
        }
        if (field == "proxy_host") {
            return StoreProxyHost(value);
        }
        if (field == "proxy_port") {
            return StoreProxyPort(value);
        }
        if (field == "proxy_scheme") {
            return StoreProxyScheme(value);
        }
        return false;
    }

    TString Serialize() const {
        return Config.DebugString();
    }

private:
    bool StoreScheme(const TString& value) {
        NKikimrSchemeOp::TS3Settings_EScheme parsed;
        if (!NKikimrSchemeOp::TS3Settings::EScheme_Parse(value, &parsed)) {
            return false;
        }
        Config.MutableObjectStorage()->SetScheme(parsed);
        return true;
    }

    bool StoreVerifySSL(const TString& value) {
        bool parsed;
        if (!ParseBoolean(value, &parsed)) {
            return false;
        }
        Config.MutableObjectStorage()->SetVerifySSL(parsed);
        return true;
    }

    bool StoreEndpoint(const TString& value) {
        Config.MutableObjectStorage()->SetEndpoint(value);
        return true;
    }

    bool StoreBucket(const TString& value) {
        Config.MutableObjectStorage()->SetBucket(value);
        return true;
    }

    bool StoreAccessKey(const TString& value) {
        Config.MutableObjectStorage()->SetAccessKey(value);
        return true;
    }

    bool StoreSecretKey(const TString& value) {
        Config.MutableObjectStorage()->SetSecretKey(value);
        return true;
    }

    bool StoreProxyHost(const TString& value) {
        Config.MutableObjectStorage()->SetProxyHost(value);
        return true;
    }

    bool StoreProxyPort(const TString& value) {
        ui32 parsed;
        if (!ParseUint32(value, &parsed)) {
            return false;
        }
        Config.MutableObjectStorage()->SetProxyPort(parsed);
        return true;
    }

    bool StoreProxyScheme(const TString& value) {
        NKikimrSchemeOp::TS3Settings_EScheme parsed;
        if (!NKikimrSchemeOp::TS3Settings::EScheme_Parse(value, &parsed)) {
            return false;
        }
        Config.MutableObjectStorage()->SetProxyScheme(parsed);
        return true;
    }

    static bool ParseBoolean(const TString& str, bool* result) {
        if (str == "TRUE") {
            *result = true;
            return true;
        }
        if (str == "FALSE") {
            *result = false;
            return true;
        }
        return false;
    }

    static bool ParseUint32(const TString& str, ui32* result) {
        ui64 parsed;
        try {
            parsed = std::stoull(str);
        } catch (const std::exception&) {
            return false;
        }
        if (parsed > Max<ui32>()) {
            return false;
        }
        *result = parsed;
        return true;
    }
};
}
