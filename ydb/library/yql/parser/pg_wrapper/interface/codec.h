#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NMiniKQL {

class TPgType;

} // NMiniKQL
} // NKikimr

namespace NYql {
namespace NCommon {

class TInputBuf;
class TOutputBuf;
class TYsonResultWriter;

TString PgValueToString(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);
NUdf::TUnboxedValue PgValueFromString(const TStringBuf text, ui32 pgTypeId);

TString PgValueToNativeText(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);
NUdf::TUnboxedValue PgValueFromNativeText(const TStringBuf text, ui32 pgTypeId);

TString PgValueToNativeBinary(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId);
NUdf::TUnboxedValue PgValueFromNativeBinary(const TStringBuf binary, ui32 pgTypeId);

TString PgValueCoerce(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId, i32 typMod, TMaybe<TString>* error);

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TPgType* type,
    const TVector<ui32>* structPositions);

void WriteYsonValueInTableFormatPg(TOutputBuf& buf, NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, bool topLevel);

NUdf::TUnboxedValue ReadYsonValueInTableFormatPg(NKikimr::NMiniKQL::TPgType* type, char cmd, TInputBuf& buf);
NUdf::TUnboxedValue ReadYsonValuePg(NKikimr::NMiniKQL::TPgType* type, char cmd, TInputBuf& buf);

void SkipSkiffPg(NKikimr::NMiniKQL::TPgType* type, TInputBuf& buf);

NKikimr::NUdf::TUnboxedValue ReadSkiffPg(NKikimr::NMiniKQL::TPgType* type, TInputBuf& buf);
void WriteSkiffPg(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, TOutputBuf& buf);

extern "C" void ReadSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, NKikimr::NUdf::TUnboxedValue& value, TInputBuf& buf);
extern "C" void WriteSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, TOutputBuf& buf);

} // namespace NCommon
} // namespace NYql
