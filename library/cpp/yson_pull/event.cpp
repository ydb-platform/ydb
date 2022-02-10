#include "event.h"

#include <library/cpp/yson_pull/detail/cescape.h>

#include <util/stream/output.h>

using namespace NYsonPull;

template <>
void Out<TEvent>(IOutputStream& out, const TEvent& value) {
    out << '(' << value.Type();
    if (value.Type() == EEventType::Scalar) {
        out << ' ' << value.AsScalar();
    } else if (value.Type() == EEventType::Key) {
        out << ' ' << NYsonPull::NDetail::NCEscape::quote(value.AsString());
    }
    out << ')';
}
