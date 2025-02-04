#include "hash_checker.h"

#include <unordered_set>
#include <iostream>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>

#include <format>

#include <library/cpp/string_utils/base64/base64.h>

namespace NLogin {
bool THashChecker::IsBase64(const std::string& value) {
    try {
        Base64StrictDecode(value);
        return true;
    } catch (...) {
        return false;
    }
}

THashChecker::TResult THashChecker::Check(const TString& hash) {
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

    if (json["type"].GetStringRobust() != "argon2id") {
        return {.Success = false,
                .Error = "Field \'type\' must be equal \"argon2id\""};
    }

    const auto& hashField = json["hash"].GetStringRobust();
    const auto& saltField = json["salt"].GetStringRobust();

    constexpr auto hashSize64 = Base64EncodeBufSize(HASH_SIZE) - 1;
    constexpr auto saltSize64 = Base64EncodeBufSize(SALT_SIZE) - 1;

    if (hashField.size() != hashSize64) {
        std::string error = std::format("Length of field \'hash\' is {}, but it must be equal {}", hashField.size(), hashSize64);
        return {.Success = false, .Error = std::move(error)};
    }

    if (saltField.size() != saltSize64) {
        std::string error = std::format("Length of field \'salt\' is {}, but it must be equal {}", saltField.size(), saltSize64);
        return {.Success = false, .Error = std::move(error)};
    }

    if (!IsBase64(hashField)) {
        return {.Success = false,
                .Error = "Field \'hash\' must be in base64 format"};
    }

    if (!IsBase64(saltField)) {
        return {.Success = false,
                .Error = "Field \'salt\' must be in base64 format"};
    }

    return {.Success = true};
};
} // NLogin
