#pragma once
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NMiniKQL {

struct TBinaryFunctionArgsDesc {
    NUdf::TDataTypeId SchemeType;
    bool IsLeftOptional;
    bool IsRightOptional;
};

TBinaryFunctionArgsDesc CheckBinaryFunctionArgs(TType* left, TType* right,
                                                bool allowOptionalInput, bool requiresBooleanArgs);

} // namespace NKikimr::NMiniKQL
