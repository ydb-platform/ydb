#pragma once

#include "mkql_block_item.h"
#include "mkql_computation_node.h"

#include <yql/essentials/minikql/mkql_node.h>

#include <yql/essentials/public/udf/arrow/block_builder.h>

#include <arrow/array/data.h>

#include <limits>

namespace NKikimr::NMiniKQL {

using NYql::NUdf::IArrayBuilder;
using NYql::NUdf::MakeArrayBuilder;
using NYql::NUdf::TInputBuffer;

} // namespace NKikimr::NMiniKQL
