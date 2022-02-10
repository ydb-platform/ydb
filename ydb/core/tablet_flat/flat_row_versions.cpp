#include "flat_row_versions.h"

#include <util/stream/output.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NTable {

    TString TRowVersionRanges::ToString() const {
        return TStringBuilder() << *this;
    }

}
}

Y_DECLARE_OUT_SPEC(, NKikimr::NTable::TRowVersionRanges, stream, value) {
    stream << "TRowVersionRanges{";
    bool first = true;
    for (const auto& item : value) {
        if (first) {
            first = false;
        } else {
            stream << ',';
        }
        stream << ' ' << '[' << item.Lower << ',' << ' ' << item.Upper << ')';
    }
    stream << ' ' << '}';
}
