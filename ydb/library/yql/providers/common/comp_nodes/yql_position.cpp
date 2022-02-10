#include "yql_position.h"
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

NYql::TPosition ExtractPosition(TCallable& callable) {
    const TStringBuf file = AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef();
    const ui32 row = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    const ui32 column = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().Get<ui32>();
    return NYql::TPosition(column, row, TString(file));
}

}
}
