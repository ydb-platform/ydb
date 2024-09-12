#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>

namespace NKikimr::NPg {

struct ITypeDesc;

ui32 PgTypeIdFromTypeDesc(const ITypeDesc* typeDesc);
const ITypeDesc* TypeDescFromPgTypeId(ui32 pgTypeId);

TString PgTypeNameFromTypeDesc(const ITypeDesc* typeDesc, const TString& typeMod = {});
const ITypeDesc* TypeDescFromPgTypeName(const TStringBuf name);
TString TypeModFromPgTypeName(const TStringBuf name);

bool TypeDescIsComparable(const ITypeDesc* typeDesc);
i32  TypeDescGetTypeLen(const ITypeDesc* typeDesc);
ui32 TypeDescGetStoredSize(const ITypeDesc* typeDesc);
bool TypeDescNeedsCoercion(const ITypeDesc* typeDesc);

int PgNativeBinaryCompare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR, const ITypeDesc* typeDesc);

ui64 PgNativeBinaryHash(const char* data, size_t size, const ITypeDesc* typeDesc);

struct TTypeModResult {
    i32 Typmod = -1;
    TMaybe<TString> Error;
};

TTypeModResult BinaryTypeModFromTextTypeMod(const TString& str, const ITypeDesc* typeDesc);

TMaybe<TString> PgNativeBinaryValidate(const TStringBuf binary, const ITypeDesc* typeDesc);

struct TCoerceResult {
    TMaybe<TString> NewValue;
    TMaybe<TString> Error;
};

TCoerceResult PgNativeBinaryCoerce(const TStringBuf binary, const ITypeDesc* typeDesc, i32 typmod);

struct TConvertResult {
    TString Str;
    TMaybe<TString> Error;
};

TConvertResult PgNativeBinaryFromNativeText(const TString& str, const ITypeDesc* typeDesc);
TConvertResult PgNativeBinaryFromNativeText(const TString& str, ui32 pgTypeId);
TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, const ITypeDesc* typeDesc);
TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, ui32 pgTypeId);

} // namespace NKikimr::NPg
