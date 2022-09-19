#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>
#include <arrow/datum.h>

namespace NKikimr::NMiniKQL {

bool FindArrowFunction(TStringBuf name, const TArrayRef<TType*>& inputTypes, TType*& outputType, TTypeEnvironment& env);
bool ConvertInputArrowType(TType* type, bool& isOptional, arrow::ValueDescr& descr);

}
