#pragma once

#include <library/cpp/yson/public.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <util/generic/vector.h>

#include "yql_codec_results.h"

namespace NYql {
namespace NCommon {

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TPgType* type,
    const TVector<ui32>* structPositions);

} // namespace NCommon
} // namespace NYql
