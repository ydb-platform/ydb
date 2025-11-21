#include "colors.h"
#include <util/generic/maybe.h>
#include <util/system/env.h>

namespace NYdb {
namespace NConsoleClient {

NColorizer::TColors& AutoColors(IOutputStream& out) {
    auto& result = NColorizer::AutoColors(out);
    // no-color.org
    if (TryGetEnv("NO_COLOR").Defined()) {
        result.Disable();
    }
    return result;
}

}
}