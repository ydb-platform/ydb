#pragma once
#include <library/cpp/time_provider/time_provider.h>

namespace NYql {

TIntrusivePtr<ITimeProvider> GetTimeProvider();

} // namespace NYql
