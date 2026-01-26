#include "hashes_checker.h"

#include <format>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/string/builder.h>

namespace {

bool IsBase64(const std::string& value) {
    try {
        Base64StrictDecode(value);
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

namespace NLogin {

using namespace NLoginProto;

THashSecret SplitPasswordHash(const TStringBuf hash) {
    size_t pos = hash.find('$');
    if (pos == NPOS) {
        return {};
    }

    const TStringBuf initParams = hash.substr(0, pos);
    const TStringBuf hashValues = hash.substr(pos + 1);
    if (initParams.empty() || hashValues.empty()) {
        return {};
    }

    return { TString(initParams), TString(hashValues) };
}

TArgonSecret ParseArgonHash(const TStringBuf argonHash) {
    auto hashSecret = SplitPasswordHash(argonHash);
    if (hashSecret.HashInitParams.empty() || hashSecret.HashValues.empty()) {
        return {};
    }

    TString salt = std::move(hashSecret.HashInitParams);
    TString hash = std::move(hashSecret.HashValues);
    return { std::move(salt), std::move(hash) };
}

TMaybe<TString> ArgonHashToNewFormat(const TStringBuf oldArgonHash) {
    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(oldArgonHash, &json)) {
        return Nothing();
    }

    if (!json.Has("type") || !json.Has("salt") || !json.Has("hash") || json["type"] != "argon2id") {
        return Nothing();
    }

    return json["salt"].GetString() + "$" + json["hash"].GetString();
}

TString HashedPasswordFromNewArgonHashFormat(const TString& argonHash) {
    NJson::TJsonValue hashes;
    hashes["argon2id"] = argonHash;
    hashes["version"] = HASHES_JSON_SCHEMA_VERSION;
    return Base64Encode(NJson::WriteJson(hashes, false));
}

TMaybe<TString> ArgonHashToOldFormat(const TStringBuf newArgonHash) {
    auto argonSecret = ParseArgonHash(newArgonHash);
    if (argonSecret.Salt.empty() || argonSecret.Hash.empty()) {
        return Nothing();
    }

    NJson::TJsonValue json;
    json["type"] = "argon2id";
    json["salt"] = std::move(argonSecret.Salt);
    json["hash"] = std::move(argonSecret.Hash);
    return NJson::WriteJson(json, false);
}

TMaybe<THashes> ConvertHashes(const TString& hash) {
    if (IsBase64(hash)) { // new format
        NJson::TJsonValue hashes;
        if (!NJson::ReadJsonTree(Base64StrictDecode(hash), &hashes)) {
            return Nothing();
        }

        if (hashes.Has("argon2id") && hashes["argon2id"].GetType() == NJson::JSON_STRING) {
            if (auto argonHash = ArgonHashToOldFormat(hashes["argon2id"].GetString())) {
                return THashes(*argonHash, hash);
            } else {
                return Nothing();
            }
        }

        return Nothing();
    } else { // old format
        if (auto argonHash = ArgonHashToNewFormat(hash)) {
            return THashes(hash, HashedPasswordFromNewArgonHashFormat(*argonHash));
        }

        return Nothing();
    }
}

TScramInitHashParams ParseScramHashInitParams(const TStringBuf hashInitParams) {
    size_t pos = hashInitParams.find(':');
    if (pos == NPOS) {
        return {};
    }

    const TStringBuf iterationsCount = hashInitParams.substr(0, pos);
    const TStringBuf salt = hashInitParams.substr(pos + 1);
    if (iterationsCount.empty() || salt.empty()) {
        return {};
    }

    return { TString(iterationsCount), TString(salt) };
}

TScramHashValues ParseScramHashValues(const TStringBuf hashValues) {
    size_t pos = hashValues.find(':');
    if (pos == NPOS) {
        return {};
    }

    const TStringBuf storedKey = hashValues.substr(0, pos);
    const TStringBuf serverKey = hashValues.substr(pos + 1);
    if (storedKey.empty() || serverKey.empty()) {
        return {};
    }

    return { TString(storedKey), TString(serverKey) };
}

TScramSecret ParseScramHash(const TStringBuf scramHash) {
    auto hashSecret = SplitPasswordHash(scramHash);
    if (hashSecret.HashInitParams.empty() || hashSecret.HashValues.empty()) {
        return {};
    }

    auto initParams = ParseScramHashInitParams(hashSecret.HashInitParams);
    if (initParams.IterationsCount.empty() || initParams.Salt.empty()) {
        return {};
    }

    auto hashValues = ParseScramHashValues(hashSecret.HashValues);
    if (hashValues.StoredKey.empty() || hashValues.ServerKey.empty()) {
        return {};
    }

    return { std::move(initParams.IterationsCount), std::move(initParams.Salt),
        std::move(hashValues.StoredKey), std::move(hashValues.ServerKey)
    };
}

THashMap<EHashType::HashType, TString> MakePasswordHashesMap(const TString& hashes) {
    THashMap<EHashType::HashType, TString> result;

    NJson::TJsonValue json;
    NJson::ReadJsonTree(Base64StrictDecode(hashes), &json);

    for (const auto& [fieldName, value] : json.GetMap()) {
        if (fieldName == "version") {
            continue;
        }

        const auto& hashTypeDescription = HashesRegistry.HashNamesMap.at(fieldName);
        result[hashTypeDescription.Type] = value.GetString();
    }

    return result;
}

THashesChecker::TResult THashesChecker::OldFormatCheck(const TString& hash) {
    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(hash, &json)) {
        return {.Success = false, .Error = "Cannot parse hash value; it should be in JSON-format"};
    }

    if (json.GetType() != NJson::JSON_MAP
        || json.GetMap().size() != 3
        || !json.Has("type")
        || !json.Has("salt")
        || !json.Has("hash")
        || json["type"].GetType() != NJson::JSON_STRING
        || json["salt"].GetType() != NJson::JSON_STRING
        || json["hash"].GetType() != NJson::JSON_STRING
    ) {
        return {.Success = false,
                .Error = "There should be strictly three fields here: salt, hash and type"};
    }

    const auto& argonHashDescription = HashesRegistry.HashNamesMap.at("argon2id");
    if (json["type"].GetStringRobust() != argonHashDescription.Name) {
        return {.Success = false,
                .Error = std::format("Field 'type' must be equal 'argon2id'")};
    }

    const auto& hashField = json["hash"].GetStringRobust();
    const auto& saltField = json["salt"].GetStringRobust();

    const auto hashSize64 = Base64EncodeBufSize(argonHashDescription.HashSize) - 1;
    const auto saltSize64 = Base64EncodeBufSize(argonHashDescription.SaltSize) - 1;

    if (hashField.size() != hashSize64) {
        std::string error = std::format("Length of field 'hash' is {}, but it must be equal {}", hashField.size(), hashSize64);
        return {.Success = false, .Error = std::move(error)};
    }

    if (saltField.size() != saltSize64) {
        std::string error = std::format("Length of field 'salt' is {}, but it must be equal {}", saltField.size(), saltSize64);
        return {.Success = false, .Error = std::move(error)};
    }

    if (!IsBase64(hashField)) {
        return {.Success = false,
                .Error = "Field 'hash' must be in base64 format"};
    }

    if (!IsBase64(saltField)) {
        return {.Success = false,
                .Error = "Field 'salt' must be in base64 format"};
    }

    return {.Success = true};
};

THashesChecker::TResult THashesChecker::NewFormatCheck(const TString& hashes) {
    if (!IsBase64(hashes)) {
        return {.Success = false, .Error = "Cannot parse hashes value; it should be JSON in base64 encoding"};
    }

    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(Base64StrictDecode(hashes), &json)) {
        return {.Success = false, .Error = "Cannot parse hashes value; it should be JSON in base64 encoding"};
    }

    if (json.GetType() != NJson::JSON_MAP) {
        return {.Success = false, .Error = "Hashes must be stored in JSON map"};
    }

    if (!json.Has("version") || !json["version"].IsUInteger()) {
        return {.Success = false, .Error = "Field 'version' must be in JSON map and have numeric type"};
    }

    if (json["version"].GetUInteger() != HASHES_JSON_SCHEMA_VERSION) {
        TStringBuilder error;
        error << "Unsupported JSON schema version. It must be equal to " << HASHES_JSON_SCHEMA_VERSION;
        return {.Success = false, .Error = std::move(error)};
    }

    for (const auto& [fieldName, value] : json.GetMap()) {
        if (fieldName == "version") {
            continue;
        } else if (HashesRegistry.HashNamesMap.contains(fieldName)) {
            const auto& hashTypeDescription = HashesRegistry.HashNamesMap.at(fieldName);
            if (!value.IsString()) {
                return {.Success = false, .Error = "Hash '" + fieldName + "' isn't stored in string format"};
            }

            switch (hashTypeDescription.Class) {
            case EHashClass::Argon: {
                const auto argonSecret = ParseArgonHash(value.GetString());
                if (argonSecret.Salt.empty() || argonSecret.Hash.empty()) {
                    return {.Success = false,
                            .Error = "Argon hash has to have '<salt>$<hash>' format"};
                }

                if (!IsBase64(argonSecret.Salt)) {
                    return {.Success = false, .Error = "Salt in Argon hash must be in base64 encoding"};
                }

                TString salt = Base64StrictDecode(argonSecret.Salt);
                if (salt.size() != hashTypeDescription.SaltSize) {
                    TStringBuilder error;
                    error << "Salt in Argon hash must be " << hashTypeDescription.SaltSize << " bytes long";
                    return {.Success = false, .Error = std::move(error)};
                }

                if (!IsBase64(argonSecret.Hash)) {
                    return {.Success = false, .Error = "Hash in Argon hash must be in base64 encoding"};
                }

                TString hash = Base64StrictDecode(argonSecret.Hash);
                if (hash.size() != hashTypeDescription.HashSize) {
                    TStringBuilder error;
                    error << "Hash in Argon hash must be " << hashTypeDescription.HashSize << " bytes long";
                    return {.Success = false, .Error = std::move(error)};
                }
                break;
            }
            case EHashClass::Scram: {
                const auto scramSecret = ParseScramHash(value.GetString());
                if (scramSecret.IterationsCount.empty() || scramSecret.Salt.empty()
                    || scramSecret.StoredKey.empty() || scramSecret.ServerKey.empty())
                {
                    return {.Success = false,
                            .Error = "Scram hash has to have '<iterations>:<salt>$<storedkey>:<serverkey>' format"};
                }

                ui32 iterationsCount;
                if (!TryFromString(scramSecret.IterationsCount, iterationsCount)
                    || iterationsCount != hashTypeDescription.IterationsCount)
                {
                    TStringBuilder error;
                    error << "Iterations in Scram hash must be equal to " << hashTypeDescription.IterationsCount;
                    return {.Success = false, .Error = std::move(error)};
                }

                if (!IsBase64(scramSecret.Salt)) {
                    return {.Success = false, .Error = "Salt in Scram hash must be in base64 encoding"};
                }

                TString salt = Base64StrictDecode(scramSecret.Salt);
                if (salt.size() != hashTypeDescription.SaltSize) {
                    TStringBuilder error;
                    error << "Salt in Scram hash must be " << hashTypeDescription.SaltSize << " bytes long";
                    return {.Success = false, .Error = std::move(error)};
                }

                if (!IsBase64(scramSecret.StoredKey)) {
                    return {.Success = false, .Error = "StoredKey in Scram hash must be in base64 encoding"};
                }

                TString storedKey = Base64StrictDecode(scramSecret.StoredKey);
                if (storedKey.size() != hashTypeDescription.HashSize) {
                    TStringBuilder error;
                    error << "StoredKey in Scram hash must be " << hashTypeDescription.HashSize << " bytes long";
                    return {.Success = false, .Error = std::move(error)};
                }

                if (!IsBase64(scramSecret.ServerKey)) {
                    return {.Success = false, .Error = "ServerKey in Scram hash must be in base64 encoding"};
                }

                TString serverKey = Base64StrictDecode(scramSecret.ServerKey);
                if (serverKey.size() != hashTypeDescription.HashSize) {
                    TStringBuilder error;
                    error << "ServerKey in Scram hash must be " << hashTypeDescription.HashSize << " bytes long";
                    return {.Success = false, .Error = std::move(error)};
                }
                break;
            }
            default:
                break;
            }
        } else {
            return {.Success = false, .Error = "Unknown field name '" + fieldName + "' in JSON"};
        }
    }

    return {.Success = true};
}

} // namespace NLogin
