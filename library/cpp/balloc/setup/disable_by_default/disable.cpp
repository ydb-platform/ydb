#include <library/cpp/balloc/setup/enable.h>

#include <util/system/compiler.h>

namespace NAllocSetup {
    // Overriding a weak symbol defined in library/cpp/balloc/setup/enable.cpp.
    // Don't link with this object if your platform doesn't support weak linkage.
    extern const bool EnableByDefault = false;
}
