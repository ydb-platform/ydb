#pragma once

#include <library/cpp/yson/public.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <util/generic/vector.h>

#include "yql_codec_results.h"
#include "yql_codec_buf.h"

namespace NYql {
namespace NCommon {

TString PgValueToString(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);
NUdf::TUnboxedValue PgValueFromString(const TStringBuf text, ui32 pgTypeId);

TString PgValueToNativeText(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);
NUdf::TUnboxedValue PgValueFromNativeText(const TStringBuf text, ui32 pgTypeId);

TString PgValueToNativeBinary(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);
NUdf::TUnboxedValue PgValueFromNativeBinary(const TStringBuf binary, ui32 pgTypeId);

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TPgType* type,
    const TVector<ui32>* structPositions);

void WriteYsonValueInTableFormatPg(TOutputBuf& buf, NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value);

NUdf::TUnboxedValue ReadYsonValueInTableFormatPg(NKikimr::NMiniKQL::TPgType* type, char cmd, TInputBuf& buf);
NUdf::TUnboxedValue ReadYsonValuePg(NKikimr::NMiniKQL::TPgType* type, char cmd, TInputBuf& buf);

extern "C" void ReadSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, NKikimr::NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf);
extern "C" void WriteSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf);

} // namespace NCommon
} // namespace NYql
