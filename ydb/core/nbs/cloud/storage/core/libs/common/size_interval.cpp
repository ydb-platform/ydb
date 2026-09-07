#include "size_interval.h"

#include "format.h"

#include <util/string/builder.h>

namespace NYdb::NBS {

TString ToString(const TSizeInterval& interval)
{
    if (interval.Start + 1 == interval.End) {
        return FormatByteSize(interval.Start);
    }
    return TStringBuilder() << FormatByteSize(interval.Start) << "-"
                            << FormatByteSize(interval.End);
}

}   // namespace NYdb::NBS
