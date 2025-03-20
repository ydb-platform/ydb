#include "time_provider.h"
#include <util/system/env.h>

namespace NYql {

TIntrusivePtr<ITimeProvider> GetTimeProvider() {
    static TIntrusivePtr<ITimeProvider> Provider = !!GetEnv("YQL_DETERMINISTIC_MODE") ? CreateDeterministicTimeProvider(1) : CreateDefaultTimeProvider();
    return Provider;
}

} // namespace NYql
