#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct TDiskDescription
{
    TString DiskId;
    ui64 TabletId = 0;
    ui32 Generation = 0;

    [[nodiscard]] TString Print() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS
