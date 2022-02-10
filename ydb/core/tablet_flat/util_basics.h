#pragma once

#include <util/generic/ptr.h>
#include <util/generic/array_ref.h>

namespace NKikimr {
    struct IDestructable {
        virtual ~IDestructable() = default;
    };
}
