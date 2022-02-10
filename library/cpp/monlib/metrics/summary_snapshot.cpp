#include "summary_snapshot.h"

#include <util/stream/output.h>

#include <iostream>


namespace {

template <typename TStream>
auto& Output(TStream& o, const NMonitoring::ISummaryDoubleSnapshot& s) {
    o << TStringBuf("{");

    o << TStringBuf("sum: ") << s.GetSum() << TStringBuf(", ");
    o << TStringBuf("min: ") << s.GetMin() << TStringBuf(", ");
    o << TStringBuf("max: ") << s.GetMax() << TStringBuf(", ");
    o << TStringBuf("last: ") << s.GetLast() << TStringBuf(", ");
    o << TStringBuf("count: ") << s.GetCount();

    o << TStringBuf("}");

    return o;
}

} // namespace

std::ostream& operator<<(std::ostream& o, const NMonitoring::ISummaryDoubleSnapshot& s) {
    return Output(o, s);
}

template <>
void Out<NMonitoring::ISummaryDoubleSnapshot>(IOutputStream& o, const NMonitoring::ISummaryDoubleSnapshot& s) {
    Output(o, s);
}
