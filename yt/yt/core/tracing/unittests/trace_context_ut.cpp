#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTraceContextTest, ParseTraceParent)
{
    TSpanContext spanContext;

    EXPECT_TRUE(TryParseTraceParent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            spanContext));

    TTraceId expectedTraceId;
    ASSERT_TRUE(TTraceId::FromStringHex32("4bf92f3577b34da6a3ce929d0e0e4736", &expectedTraceId));
    EXPECT_TRUE(spanContext.TraceId == expectedTraceId);
    EXPECT_EQ(spanContext.SpanId, 0x00f067aa0ba902b7ULL);
    EXPECT_TRUE(spanContext.Sampled);
    EXPECT_FALSE(spanContext.Debug);
}

TEST(TTraceContextTest, ParseLegacyTraceParent)
{
    TSpanContext spanContext;

    EXPECT_TRUE(TryParseTraceParent(
            "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
            spanContext));
    EXPECT_FALSE(spanContext.Sampled);
}

TEST(TTraceContextTest, RejectMalformedTraceParent)
{
    for (auto traceParent : {
             "malformed",
             "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
             "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
             "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
             "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",
         }) {
        TSpanContext spanContext;
        EXPECT_FALSE(TryParseTraceParent(traceParent, spanContext));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
