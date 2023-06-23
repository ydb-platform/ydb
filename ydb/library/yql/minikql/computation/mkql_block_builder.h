#pragma once

#include "mkql_block_item.h"
#include "mkql_computation_node.h"

#include <ydb/library/yql/minikql/mkql_node.h>

#include <ydb/library/yql/public/udf/arrow/block_builder.h>

#include <arrow/array/data.h>

#include <limits>

namespace NKikimr {
namespace NMiniKQL {

using NYql::NUdf::IArrayBuilder;
using NYql::NUdf::TInputBuffer;
using NYql::NUdf::MakeArrayBuilder;

}
}
