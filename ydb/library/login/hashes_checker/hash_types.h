#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NLogin {

enum class EHashClass {
    Argon,
    Scram,
};

enum class EHashType {
    Argon,
    ScramSha256,
};

struct THashTypeDescription {
    const EHashClass Class;
    const EHashType Type;
    const TString Name;
    const ui32 IterationsCount;
    const ui32 SaltSize;
    const ui32 HashSize;
    const bool IsEmptyPasswordAllowed;
};

struct THashTypesRegistry {

    THashTypesRegistry();

    static const TVector<THashTypeDescription> HashTypeDescriptions;

    THashMap<EHashType, const THashTypeDescription&> HashTypesMap;
    THashMap<TStringBuf, const THashTypeDescription&> HashNamesMap;

} const extern HashesRegistry;

} // namespace NLogin
