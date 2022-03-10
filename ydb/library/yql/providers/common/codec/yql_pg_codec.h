#pragma once

#include <library/cpp/yson/public.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <util/generic/vector.h>

#include "yql_codec_results.h"
#include "yql_codec_buf.h"

namespace NYql {
namespace NCommon {

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TPgType* type,
    const TVector<ui32>* structPositions);

void WriteYsonValueInTableFormatPg(TOutputBuf& buf, NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value);

NUdf::TUnboxedValue ReadYsonValuePg(NKikimr::NMiniKQL::TPgType* type, char cmd, TInputBuf& buf);

} // namespace NCommon
} // namespace NYql
