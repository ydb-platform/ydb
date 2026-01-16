#pragma once

#include <util/system/types.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <ydb/library/login/hashes_checker/hash_types.h>

namespace NLogin {

constexpr ui16 HASHES_JSON_SCHEMA_VERSION = 1;

struct THashSecret {
    TString HashInitParams;
    TString HashValues;
};

struct TArgonSecret {
    TString Salt;
    TString Hash;
};

struct TScramInitHashParams {
    TString IterationsCount;
    TString Salt;
};

struct TScramHashValues {
    TString StoredKey;
    TString ServerKey;
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
TString HashedPasswordFromNewArgonHashFormat(const TString& argonHash);
TMaybe<TString> ArgonHashToOldFormat(const TStringBuf newArgonHash);
TMaybe<THashes> ConvertHashes(const TString& hash);

THashSecret SplitPasswordHash(const TStringBuf hash);
TArgonSecret ParseArgonHash(const TStringBuf argonHash);
TScramInitHashParams ParseScramHashInitParams(const TStringBuf hashInitParams);
TScramHashValues ParseScramHashValues(const TStringBuf hashValues);
TScramSecret ParseScramHash(const TStringBuf scramHash);
THashMap<NLoginProto::EHashType::HashType, TString> MakePasswordHashesMap(const TString& hashes);

class THashesChecker {
public:
    struct TResult {
        bool Success = true;
        TString Error;
    };

static TResult OldFormatCheck(const TString& hash);
static TResult NewFormatCheck(const TString& hashes);

};

} // NLogin
