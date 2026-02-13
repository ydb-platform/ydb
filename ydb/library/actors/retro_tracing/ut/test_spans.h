#pragma once

#include <ydb/library/actors/retro_tracing/typed_retro_span.h>

enum ETestRetroSpanType : ui32 {
    Test1 = 11,
    Test2
};

class TTestSpan1 : public NRetroTracing::TTypedRetroSpan<TTestSpan1, Test1> {
public:
    ui64 Var = 0;
};

class TTestSpan2 : public NRetroTracing::TTypedRetroSpan<TTestSpan2, Test2> {
public:
    ui32 Var1 = 0;
    ui64 Var2 = 0;
};
