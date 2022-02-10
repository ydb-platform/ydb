#include "log_histogram_snapshot.h"

#include <util/stream/output.h>

#include <iostream>


namespace {

template <typename TStream>
auto& Output(TStream& o, const NMonitoring::TLogHistogramSnapshot& hist) {
    o << TStringBuf("{");

    for (auto i = 0u; i < hist.Count(); ++i) {
        o << hist.UpperBound(i) << TStringBuf(": ") << hist.Bucket(i);
        o << TStringBuf(", ");
    }

    o << TStringBuf("zeros: ") << hist.ZerosCount();

    o << TStringBuf("}");

    return o;
}

} // namespace

std::ostream& operator<<(std::ostream& os, const NMonitoring::TLogHistogramSnapshot& hist) {
    return Output(os, hist);
}

template <>
void Out<NMonitoring::TLogHistogramSnapshot>(IOutputStream& os, const NMonitoring::TLogHistogramSnapshot& hist) {
    Output(os, hist);
}
