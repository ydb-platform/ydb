
#include "disk_description.h"

#include <util/string/builder.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

TString TDiskDescription::Print() const
{
    TStringBuilder builder;
    builder << "DiskId: " << DiskId << ", tbl: " << TabletId << "/"
            << Generation;
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS
