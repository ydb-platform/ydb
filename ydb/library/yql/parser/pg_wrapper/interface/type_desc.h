#pragma once

#include <ydb/core/scheme_types/scheme_type_desc.h>

#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>

namespace NKikimr::NPg {

using TTypeDesc = NScheme::TTypeDesc;

ui32 PgTypeIdFromTypeDesc(const TTypeDesc* typeDesc);
const TTypeDesc* TypeDescFromPgTypeId(ui32 pgTypeId);

TString PgTypeNameFromTypeDesc(const TTypeDesc* typeDesc, const TString& typeMod = {});
const TTypeDesc* TypeDescFromPgTypeName(const TStringBuf name);
TString TypeModFromPgTypeName(const TStringBuf name);

bool TypeDescIsComparable(const TTypeDesc* typeDesc);
i32  TypeDescGetTypeLen(const TTypeDesc* typeDesc);
ui32 TypeDescGetStoredSize(const TTypeDesc* typeDesc);
bool TypeDescNeedsCoercion(const TTypeDesc* typeDesc);

int PgNativeBinaryCompare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR, const TTypeDesc* typeDesc);

ui64 PgNativeBinaryHash(const char* data, size_t size, const TTypeDesc* typeDesc);

struct TTypeModResult {
    i32 Typmod = -1;
    TMaybe<TString> Error;
};

TTypeModResult BinaryTypeModFromTextTypeMod(const TString& str, const TTypeDesc* typeDesc);

TMaybe<TString> PgNativeBinaryValidate(const TStringBuf binary, const TTypeDesc* typeDesc);

struct TCoerceResult {
    TMaybe<TString> NewValue;
    TMaybe<TString> Error;
};

TCoerceResult PgNativeBinaryCoerce(const TStringBuf binary, const TTypeDesc* typeDesc, i32 typmod);

struct TConvertResult {
    TString Str;
    TMaybe<TString> Error;
};

TConvertResult PgNativeBinaryFromNativeText(const TString& str, const TTypeDesc* typeDesc);
TConvertResult PgNativeBinaryFromNativeText(const TString& str, ui32 pgTypeId);
TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, const TTypeDesc* typeDesc);
TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, ui32 pgTypeId);

} // namespace NKikimr::NPg
