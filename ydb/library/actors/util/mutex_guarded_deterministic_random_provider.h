#pragma once

#include <library/cpp/random/random_provider.h>

TIntrusivePtr<IRandomProvider> CreateMutexGuardedDeterministicRandomProvider(ui64 seed);
