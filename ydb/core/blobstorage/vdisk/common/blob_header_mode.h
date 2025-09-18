#pragma once

#include "defs.h"

namespace NKikimr {

    enum class EBlobHeaderMode {
        OLD_HEADER, // old 5-byte header
        NO_HEADER, // no header at all
        XXH3_64BIT_HEADER, // header with 8-byte checksum (XXH3 64 bits)
    };

} // NKikimr
