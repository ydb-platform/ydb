#pragma once
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr {
namespace NMiniKQL {

struct TBinaryFunctionArgsDesc {
    NUdf::TDataTypeId SchemeType;
    bool IsLeftOptional;
    bool IsRightOptional;
};

TBinaryFunctionArgsDesc CheckBinaryFunctionArgs(TType* left, TType* right,
    bool allowOptionalInput, bool requiresBooleanArgs);

}
}
