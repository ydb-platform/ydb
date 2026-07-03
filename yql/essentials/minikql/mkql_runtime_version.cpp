#include "mkql_runtime_version.h"

template <>
void Out<NKikimr::NMiniKQL::TRuntimeVersion>(IOutputStream& out, const NKikimr::NMiniKQL::TRuntimeVersion& value) {
    out << value.Value();
}
