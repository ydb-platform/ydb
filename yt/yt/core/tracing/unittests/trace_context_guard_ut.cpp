#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTracing {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTraceContextGuardTest, Simple)
{
    EXPECT_EQ(TryGetCurrentTraceContext(), nullptr);
    {
        auto traceContext = TTraceContext::NewRoot("t1");
        TTraceContextGuard guard1(traceContext);
        EXPECT_EQ(TryGetCurrentTraceContext()->GetTraceId(), traceContext->GetTraceId());
    }
    EXPECT_EQ(TryGetCurrentTraceContext(), nullptr);
}

TEST(TTraceContextGuardTest, Inclusion)
{
    auto ctx1 = TTraceContext::NewRoot("t1");
    auto ctx2 = TTraceContext::NewRoot("t2");
    auto ctx3 = TTraceContext::NewRoot("t3");
    TTraceContextGuard guard1(ctx1);
    EXPECT_EQ(TryGetCurrentTraceContext()->GetTraceId(), ctx1->GetTraceId());
    {
        TTraceContextGuard guard2(ctx2);
        EXPECT_EQ(TryGetCurrentTraceContext()->GetTraceId(), ctx2->GetTraceId());
        {
            TTraceContextGuard guard3(ctx3);
            EXPECT_EQ(TryGetCurrentTraceContext()->GetTraceId(), ctx3->GetTraceId());
        }
        EXPECT_EQ(TryGetCurrentTraceContext()->GetTraceId(), ctx2->GetTraceId());
    }
    EXPECT_EQ(TryGetCurrentTraceContext()->GetTraceId(), ctx1->GetTraceId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTracing
