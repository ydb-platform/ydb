#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/tracing/allocation_tags.h>
#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTracing {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAllocationTagsTest, GetSetAllocationTags)
{
    auto traceContext = TTraceContext::NewRoot("Root");
    TTraceContextGuard guard(traceContext);

    ASSERT_EQ(traceContext->FindAllocationTag<TString>("a"), std::nullopt);

    traceContext->SetAllocationTags({{"user", "first"}, {"sometag", "my"}});
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>("memory_tag"), std::nullopt);
    ASSERT_EQ(traceContext->FindAllocationTag<TString>("user"),  "first");
    ASSERT_EQ(traceContext->FindAllocationTag<TString>("sometag"),  "my");
    ASSERT_EQ(traceContext->FindAllocationTag<TString>("other"),  std::nullopt);
    ASSERT_EQ(traceContext->FindAllocationTag<int>("other"),  std::nullopt);

    traceContext->SetAllocationTag<TString>("a", "e");

    ASSERT_EQ(traceContext->FindAllocationTag<TString>("a"),  "e");

    traceContext->RemoveAllocationTag<TString>("a");
    ASSERT_EQ(traceContext->FindAllocationTag<TString>("a"), std::nullopt);

    traceContext->RemoveAllocationTag<TString>("user");
    traceContext->RemoveAllocationTag<TString>("sometag");
    ASSERT_EQ(traceContext->FindAllocationTag<TString>("user"),  std::nullopt);
    ASSERT_EQ(traceContext->FindAllocationTag<TString>("sometag"),  std::nullopt);
    ASSERT_TRUE(traceContext->GetAllocationTags().empty());

    traceContext->SetAllocationTag<TMemoryTag>("memory_tag", TMemoryTag{1});
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>("memory_tag"),  TMemoryTag{1});
    ASSERT_FALSE(traceContext->GetAllocationTags().empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTracing
