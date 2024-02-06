#pragma once

#include "library/cpp/deprecated/atomic/atomic.h"

#include <util/generic/ptr.h>

namespace NKikimr::NJaegerTracing {

struct TControl
    : public TThrRefBase
{
    TControl(ui64 initial = 0)
        : Value(initial)
    {}

    ui64 Get() const {
        return AtomicGet(Value);
    }

    void Set(ui64 newValue) {
        AtomicSet(Value, newValue);
    }

    TAtomic Value;
};

} // namespace NKikimr::NJaegerTracing
