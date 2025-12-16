#pragma once

#include <util/system/types.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <ydb/library/login/hashes_checker/hash_types.h>

namespace NLogin {

constexpr ui16 HASHES_JSON_SCHEMA_VERSION = 1;

struct TArgonSecret {
    TString Salt;
    TString Hash;
};

struct TScramSecret {
    TString IterationsCount;
    TString Salt;
    TString StoredKey;
    TString ServerKey;
};

struct THashes {
    TString OldHashFormat;
    TString NewHashFormat;
};

TMaybe<TString> ArgonHashToNewFormat(const TStringBuf oldArgonHash);
TMaybe<TString> ArgonHashToOldFormat(const TStringBuf newArgonHash);
TMaybe<THashes> ConvertHashes(const TString& hash);

TArgonSecret ParseArgonHash(const TStringBuf hash);
TScramSecret ParseScramHash(const TStringBuf hash);

class THashesChecker {
public:
    struct TResult {
        bool Success = true;
        TString Error;
    };

    THashesChecker();
    TResult OldFormatCheck(const TString& hash) const;
    TResult NewFormatCheck(const TString& hashes) const;
    const THashTypeDescription* GetHashParams(const TString& hashName) const;

private:
    THashMap<TStringBuf, const THashTypeDescription&> AvailableHashTypes;
};

} // NLogin
