#pragma once

#include <util/system/types.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

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
    const bool IsNullPasswordAllowed;
};

const extern TVector<THashTypeDescription> HashesRegistry;

} // namespace NLogin
