#pragma once

#include <util/generic/fwd.h>

namespace NKikimr {

namespace NMiniKQL {
    class IBuiltinFunctionRegistry;
}

void SerializeMetadata(const NMiniKQL::IBuiltinFunctionRegistry& funcRegistry, TString* out);
void DeserializeMetadata(TStringBuf buffer, NMiniKQL::IBuiltinFunctionRegistry& funcRegistry);

} // namespace NKikimr
