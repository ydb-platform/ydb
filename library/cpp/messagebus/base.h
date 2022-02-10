#pragma once

#include <util/system/defaults.h>

namespace NBus {
    /// millis since epoch
    using TBusInstant = ui64;
    /// returns time in milliseconds
    TBusInstant Now();

}
