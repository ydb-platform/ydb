#pragma once

#include <yql/essentials/minikql/mkql_node.h>

#include <arrow/datum.h>
#include <arrow/type.h>

namespace NKikimr::NMiniKQL {

class IBuiltinFunctionRegistry;

bool FindArrowFunction(TStringBuf name, const TArrayRef<TType*>& inputTypes, TType* outputType, const IBuiltinFunctionRegistry& registry);
bool ConvertInputArrowType(TType* blockType, arrow20::TypeHolder& out);
bool HasArrowCast(TType* from, TType* to);
} // namespace NKikimr::NMiniKQL
