#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>
#include <arrow/datum.h>

namespace NKikimr::NMiniKQL {

class IBuiltinFunctionRegistry;

bool FindArrowFunction(TStringBuf name, const TArrayRef<TType*>& inputTypes, TType* outputType, const IBuiltinFunctionRegistry& registry);
bool ConvertInputArrowType(TType* blockType, bool& isOptional, arrow::ValueDescr& descr);
bool HasArrowCast(TType* from, TType* to);
}
