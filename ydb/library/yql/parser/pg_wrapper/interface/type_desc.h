#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>

namespace NKikimr::NPg {

ui32 PgTypeIdFromTypeDesc(const void* typeDesc);
const void* TypeDescFromPgTypeId(ui32 pgTypeId);

TString PgTypeNameFromTypeDesc(const void* typeDesc, const TString& typeMod = {});
const void* TypeDescFromPgTypeName(const TStringBuf name);
TString TypeModFromPgTypeName(const TStringBuf name);

bool TypeDescIsComparable(const void* typeDesc);
i32  TypeDescGetTypeLen(const void* typeDesc);
ui32 TypeDescGetStoredSize(const void* typeDesc);
bool TypeDescNeedsCoercion(const void* typeDesc);

int PgNativeBinaryCompare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR, const void* typeDesc);

ui64 PgNativeBinaryHash(const char* data, size_t size, const void* typeDesc);

struct TTypeModResult {
    i32 Typmod = -1;
    TMaybe<TString> Error;
};

TTypeModResult BinaryTypeModFromTextTypeMod(const TString& str, const void* typeDesc);

TMaybe<TString> PgNativeBinaryValidate(const TStringBuf binary, const void* typeDesc);

struct TCoerceResult {
    TMaybe<TString> NewValue;
    TMaybe<TString> Error;
};

TCoerceResult PgNativeBinaryCoerce(const TStringBuf binary, const void* typeDesc, i32 typmod);

struct TConvertResult {
    TString Str;
    TMaybe<TString> Error;
};

TConvertResult PgNativeBinaryFromNativeText(const TString& str, const void* typeDesc);
TConvertResult PgNativeBinaryFromNativeText(const TString& str, ui32 pgTypeId);
TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, const void* typeDesc);
TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, ui32 pgTypeId);

} // namespace NKikimr::NPg
