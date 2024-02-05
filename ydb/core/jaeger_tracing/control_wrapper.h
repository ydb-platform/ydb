#pragma once

#include "control.h"

namespace NKikimr::NJaegerTracing {

class TControlWrapper {
public:
    explicit TControlWrapper(ui64 initial = 0) : Control(MakeIntrusive<TControl>(initial)) {}

    ui64 Get() const {
        return Control->Get();
    }

    void Set(ui64 newValue) {
        Control->Set(newValue);
    }

private:
    TIntrusivePtr<TControl> Control;
};

} // namespace NKikimr::NJaegerTracing
