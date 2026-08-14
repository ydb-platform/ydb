#pragma once

#include "defs.h"

namespace NKikimr {

    enum class EBlobHeaderMode {
        OLD_HEADER, // old 5-byte header
        NO_HEADER, // no header at all
    };

} // NKikimr
