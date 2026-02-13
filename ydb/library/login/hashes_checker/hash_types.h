#pragma once

#include <ydb/library/login/protos/login.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NLogin {

enum class EHashClass {
    Argon,
    Scram,
};

struct THashTypeDescription {
    const EHashClass Class;
    const NLoginProto::EHashType::HashType Type;
    const TString Name;
    const ui32 IterationsCount;
    const ui32 SaltSize;
    const ui32 HashSize;
    const bool IsEmptyPasswordAllowed;
};

struct THashTypesRegistry {

    THashTypesRegistry();

    static const TVector<THashTypeDescription> HashTypeDescriptions;

    THashMap<NLoginProto::EHashType::HashType, const THashTypeDescription&> HashTypesMap;
    THashMap<TStringBuf, const THashTypeDescription&> HashNamesMap;

} const extern HashesRegistry;

} // namespace NLogin
