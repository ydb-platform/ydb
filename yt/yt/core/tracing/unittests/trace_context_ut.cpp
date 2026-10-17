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

TEST(TTraceContextTest, FormatTraceParent)
{
    TSpanContext spanContext{
        .TraceId = TTraceId::FromStringHex32("4bf92f3577b34da6a3ce929d0e0e4736"),
        .SpanId = 0x00f067aa0ba902b7ULL,
        .Debug = true,
    };

    auto check = [&] (bool sampled, const std::string& expectedTraceParent) {
        spanContext.Sampled = sampled;

        auto traceParent = FormatTraceParent(spanContext);
        EXPECT_EQ(expectedTraceParent, traceParent);

        TSpanContext parsedSpanContext;
        ASSERT_TRUE(TryParseTraceParent(traceParent, parsedSpanContext));
        EXPECT_TRUE(parsedSpanContext.TraceId == spanContext.TraceId);
        EXPECT_EQ(parsedSpanContext.SpanId, spanContext.SpanId);
        EXPECT_EQ(parsedSpanContext.Sampled, spanContext.Sampled);
        EXPECT_FALSE(parsedSpanContext.Debug);
    };

    check(/*sampled*/ false, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    check(/*sampled*/ true, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
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
