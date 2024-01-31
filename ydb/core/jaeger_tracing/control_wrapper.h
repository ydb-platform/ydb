#pragma once

#include "control.h"

namespace NKikimr {
namespace NJaegerTracing {

class TControlWrapper {
public:
    TControlWrapper(ui64 initial = 0) : Control(MakeIntrusive<TControl>(initial)) {}

    ui64 Get() const {
        return Control->Get();
    }

    void Set(ui64 newValue) {
        Control->Set(newValue);
    }

private:
    TIntrusivePtr<TControl> Control;
};

} // namespace NJaegerTracing
} // namespace NKikimr
