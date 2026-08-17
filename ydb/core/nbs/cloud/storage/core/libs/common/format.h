#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

TString FormatTimestamp(TInstant ts);
TString FormatDuration(TDuration duration);
TString FormatByteSize(ui64 size);

}   // namespace NYdb::NBS
