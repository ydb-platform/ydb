#include <library/cpp/testing/unittest/registar.h>
#include <util/system/compiler.h>
#include <ydb/library/retro_tracing/retro_tracing.h>
#include <ydb/library/retro_tracing/retro_span.h>

namespace NRetro {

Y_UNIT_TEST_SUITE(SpanCircleBuf) {
    Y_UNIT_TEST(Simple) {
        // TODO: make the actual test
        InitRetroTracing();
        TRetroSpanDSProxyRequest span(TMonotonic::Zero(), 0);
        ReadSpansOfTrace(0);
    }
}

} // namespace NKikimr
