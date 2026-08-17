#pragma once

#include <library/cpp/random_provider/random_provider.h>

TIntrusivePtr<IRandomProvider> CreateMutexGuardedDeterministicRandomProvider(ui64 seed);
