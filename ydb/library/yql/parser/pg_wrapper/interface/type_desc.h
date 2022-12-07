#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr::NPg {

ui32 PgTypeIdFromTypeDesc(void* typeDesc);
void* TypeDescFromPgTypeId(ui32 pgTypeId);

const char* PgTypeNameFromTypeDesc(void* typeDesc);
void* TypeDescFromPgTypeName(const TStringBuf name);

bool TypeDescIsComparable(void* typeDesc);
ui32 TypeDescGetStoredSize(void* typeDesc);

int PgNativeBinaryCompare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR, void* typeDesc);

ui64 PgNativeBinaryHash(const char* data, size_t size, void* typeDesc);

// for tests
TString PgNativeBinaryFromNativeText(const TString& str, ui32 pgTypeId);
TString PgNativeTextFromNativeBinary(const TString& binary, ui32 pgTypeId);

} // namespace NKikimr::NPg
