#include <gtest/gtest.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTracing {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSamplerTest, PerUser)
{
    auto config = New<TSamplerConfig>();
    config->MinPerUserSamples = 1;
    config->MinPerUserSamplesPeriod = TDuration::MilliSeconds(100);

    auto sampler = New<TSampler>(config);

    auto traceContext = TTraceContext::NewRoot("Test");

    sampler->SampleTraceContext("prime", traceContext);
    ASSERT_TRUE(traceContext->IsSampled());

    traceContext->SetSampled(false);

    sampler->SampleTraceContext("prime", traceContext);
    ASSERT_FALSE(traceContext->IsSampled());

    Sleep(TDuration::MilliSeconds(400));

    sampler->SampleTraceContext("prime", traceContext);
    ASSERT_TRUE(traceContext->IsSampled());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTracing
