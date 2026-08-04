#pragma once

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/string.h>

#include <ydb/library/login/login.h>

#include <cctype>
#include <vector>

namespace NAuthSecurityFuzz {

inline const TString& ValidOldArgonHash() {
    static const TString value = R"json(
        {
            "hash":"ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw=",
            "salt":"HTkpQjtVJgBoA0CZu+i3zg==",
            "type":"argon2id"
        }
    )json";
    return value;
}

inline const TString& ValidHashesJson() {
    static const TString value = R"json(
        {
            "version": 1,
            "argon2id": "flbr3YnA9kG67qegwDTaYg==$wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=",
            "scram-sha-256": "4096:s0QSrrFVkMTh3k2TTk860A==$LmCubRpIYV1zHMLucTtu7XjhB+PgWwH8ABCYGyVF1mo=:eUrie0C98tEFgygSOtom/fwPmgnMxeq53l7YTFfYncc="
        }
    )json";
    return value;
}

inline const TString& ValidPasswordHashes() {
    static const TString value = Base64Encode(ValidHashesJson());
    return value;
}

inline const TString& ValidArgonHashValue() {
    static const TString value = "wsTryyX+vdkLiZ4PfYabvgVwHf8tbxBVVtDluhiz3fo=";
    return value;
}

inline const TString& ValidScramServerKey() {
    static const TString value = "eUrie0C98tEFgygSOtom/fwPmgnMxeq53l7YTFfYncc=";
    return value;
}

inline const TString& ValidScramClientProof() {
    static const TString value = "AJgthTHWf0jz/bMHwrWDOHk9SQPpPpvGx937mEzFnCQ=";
    return value;
}

inline const TString& ValidScramAuthMessage() {
    static const TString value = "n=user,r=clientnonce,r=clientservernonce,s=s0QSrrFVkMTh3k2TTk860A==,i=4096,c=biws,r=clientservernonce";
    return value;
}

inline char SanitizeNameChar(char ch, bool strongOnly) {
    if (strongOnly) {
        if (ch >= 'a' && ch <= 'z') {
            return ch;
        }
        if (ch >= '0' && ch <= '9') {
            return ch;
        }
        return static_cast<char>('a' + (static_cast<unsigned char>(ch) % 26));
    }

    if (std::isalnum(static_cast<unsigned char>(ch))) {
        return ch;
    }
    if (ch == '-' || ch == '_') {
        return ch;
    }
    return static_cast<char>('a' + (static_cast<unsigned char>(ch) % 26));
}

inline TString ConsumeName(FuzzedDataProvider& fdp, size_t maxLen = 16, bool strongOnly = false) {
    TString name = fdp.ConsumeRandomLengthString(maxLen);
    if (name.empty()) {
        return strongOnly ? TString("user0") : TString("User_0");
    }

    for (char& ch : name) {
        ch = SanitizeNameChar(ch, strongOnly);
    }

    if (strongOnly && name.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789") != TString::npos) {
        for (char& ch : name) {
            ch = SanitizeNameChar(ch, true);
        }
    }

    if (name[0] == '-' || name[0] == '_') {
        name[0] = strongOnly ? 'a' : 'A';
    }

    return name;
}

inline TString ConsumeToken(FuzzedDataProvider& fdp, size_t maxLen = 32, const TStringBuf alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-:/+=") {
    TString token = fdp.ConsumeRandomLengthString(maxLen);
    if (token.empty()) {
        return "x";
    }

    for (char& ch : token) {
        ch = alphabet[static_cast<unsigned char>(ch) % alphabet.size()];
    }
    return token;
}

inline TString PickKnownOrNew(FuzzedDataProvider& fdp, const std::vector<TString>& known, bool strongOnly = false) {
    if (!known.empty() && fdp.ConsumeBool()) {
        return known[fdp.ConsumeIntegralInRange<size_t>(0, known.size() - 1)];
    }
    return ConsumeName(fdp, 16, strongOnly);
}

inline void PopulateValidHashedUser(NLogin::TLoginProvider::TCreateUserRequest& request, bool canLogin = true) {
    request.HashedPassword = ValidPasswordHashes();
    request.CanLogin = canLogin;
}

inline void EnsureHashedUser(NLogin::TLoginProvider& provider, const TString& user, bool canLogin = true) {
    NLogin::TLoginProvider::TCreateUserRequest request;
    request.User = user;
    PopulateValidHashedUser(request, canLogin);
    provider.CreateUser(request);
}

} // namespace NAuthSecurityFuzz
