#pragma once

#include <util/stream/output.h>

namespace NKikimr {

    void FormatHumanReadable(IOutputStream& out, ui64 number, ui32 base, unsigned fracw /*0...6*/, const char *suffixes[]);

} // NKikimr
