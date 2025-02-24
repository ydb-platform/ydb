#include "span_circlebuf_stats.h"
#include <util/stream/str.h>

namespace NRetro {

TString TSpanCircleBufStats::ToString() const {
    TStringStream str;
    str << "TSpanCircleBufStats{";
    str << " Writes# " << Writes;
    str << " SpansRejectedDueToOversize# " << SpansRejectedDueToOversize;
    str << " FailedLocks# " << FailedLocks;
    str << " SuccessfulWrites# " << SuccessfulWrites;
    str << " Reads# " << Reads;
    str << " Overflows# " << Overflows;
    str << " }";
    return str.Str();
}

} // namespace NRetro
