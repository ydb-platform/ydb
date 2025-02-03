#pragma once

#include <util/system/types.h>

const int MAX_BYTES_FOR_UI32 = 5; // max count of bytes to Pack(ui32) or PackU32()
const int MAX_BYTES_FOR_UI64 = 9; // max count of bytes to PackU64()

////////////////////////////////////////////////////////////////////////////////
//
// Pack functions
//

int Pack(ui8* buf, ui32 value);
ui32 Unpack(const ui8*& data);

////////////////////////////////////////////////////////////////////////////////
//
// PackU32:
//   1. Preserves sort order
//   2. Slightly more compact than 'Pack'
//

int PackU32(ui32 value, void* buf);
int UnpackU32(ui32* value, const void* buf);
int SkipU32(const void* buf);

int PackU64(ui64 value, void* buf);
int UnpackU64(ui64* value, const void* buf);
int SkipU64(const void* buf);
