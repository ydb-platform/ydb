#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>

namespace NKikimr::NPg {

ui32 PgTypeIdFromTypeDesc(void* typeDesc);
void* TypeDescFromPgTypeId(ui32 pgTypeId);

TString PgTypeNameFromTypeDesc(void* typeDesc, const TString& typeMod = {});
void* TypeDescFromPgTypeName(const TStringBuf name);
TString TypeModFromPgTypeName(const TStringBuf name);

bool TypeDescIsComparable(void* typeDesc);
i32  TypeDescGetTypeLen(void* typeDesc);
ui32 TypeDescGetStoredSize(void* typeDesc);
bool TypeDescNeedsCoercion(void* typeDesc);

int PgNativeBinaryCompare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR, void* typeDesc);

ui64 PgNativeBinaryHash(const char* data, size_t size, void* typeDesc);

struct TTypeModResult {
    i32 Typmod = -1;
    TMaybe<TString> Error;
};

TTypeModResult BinaryTypeModFromTextTypeMod(const TString& str, void* typeDesc);

TMaybe<TString> PgNativeBinaryValidate(const TStringBuf binary, void* typeDesc);

struct TCoerceResult {
    TMaybe<TString> NewValue;
    TMaybe<TString> Error;
};

TCoerceResult PgNativeBinaryCoerce(const TStringBuf binary, void* typeDesc, i32 typmod);

struct TConvertResult {
    TString Str;
    TMaybe<TString> Error;
};

TConvertResult PgNativeBinaryFromNativeText(const TString& str, ui32 pgTypeId);
TConvertResult PgNativeTextFromNativeBinary(const TString& binary, ui32 pgTypeId);

} // namespace NKikimr::NPg
