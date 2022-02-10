#pragma once

#include <library/cpp/messagebus/rain_check/core/rain_check.h>

#include <library/cpp/messagebus/misc/test_sync.h>

template <typename TSelf>
struct TTestEnvTemplate: public NRainCheck::TSimpleEnvTemplate<TSelf> {
    TTestSync TestSync;
};

struct TTestEnv: public TTestEnvTemplate<TTestEnv> {
};
