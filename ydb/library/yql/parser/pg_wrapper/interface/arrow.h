#pragma once
#include <ydb/library/yql/minikql/mkql_node.h>
#include <arrow/datum.h>

namespace NYql {

arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool);

} // NYql
