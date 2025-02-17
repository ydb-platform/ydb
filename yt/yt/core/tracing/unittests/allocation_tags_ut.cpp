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

    ASSERT_EQ(traceContext->FindAllocationTag<std::string>("a"), std::nullopt);

    traceContext->SetAllocationTags({{"user", "first"}, {"sometag", "my"}});
    ASSERT_EQ(traceContext->FindAllocationTag<int>("memory_tag"), std::nullopt);
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>("user"),  "first");
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>("sometag"),  "my");
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>("other"),  std::nullopt);
    ASSERT_EQ(traceContext->FindAllocationTag<int>("other"),  std::nullopt);

    traceContext->SetAllocationTag<std::string>("a", "e");

    ASSERT_EQ(traceContext->FindAllocationTag<std::string>("a"),  "e");

    traceContext->RemoveAllocationTag("a");
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>("a"), std::nullopt);

    traceContext->RemoveAllocationTag("user");
    traceContext->RemoveAllocationTag("sometag");
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>("user"),  std::nullopt);
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>("sometag"),  std::nullopt);
    ASSERT_TRUE(traceContext->GetAllocationTags().empty());

    traceContext->SetAllocationTag<int>("memory_tag", 1);
    ASSERT_EQ(traceContext->FindAllocationTag<int>("memory_tag"),  1);
    ASSERT_FALSE(traceContext->GetAllocationTags().empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTracing
