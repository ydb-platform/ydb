#pragma once

#include <ydb/library/actors/retro_tracing/typed_retro_span.h>

enum ETestRetroSpanType : ui32 {
    Test1 = 0,
    Test2
};

class TTestSpan1 : public NRetroTracing::TTypedRetroSpan<Test1, TTestSpan1> {
public:
    ui64 Var1 = 0;
    ui32 Var2 = 0;
};

class TTestSpan2 : public NRetroTracing::TTypedRetroSpan<Test2, TTestSpan2> {
public:
    ui8 Var3 = 0;
};
