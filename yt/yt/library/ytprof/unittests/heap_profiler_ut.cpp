#include <gtest/gtest.h>

#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/symbolize.h>
#include <yt/yt/library/ytprof/profile.h>

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/tracing/allocation_tags.h>
#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/testing/common/env.h>

#include <util/string/cast.h>
#include <util/stream/file.h>
#include <util/generic/hash_set.h>
#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

#include <tcmalloc/common.h>

#include <absl/debugging/stacktrace.h>

namespace NYT::NYTProf {
namespace {

using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

const std::string MemoryAllocationTagKey = "memory_allocation_tag";
const std::vector<std::string> MemoryAllocationTagValues = {"0", "1", "2", "3", "4", "5", "6", "7"};

////////////////////////////////////////////////////////////////////////////////

template <size_t Index>
Y_NO_INLINE auto BlowHeap()
{
    std::vector<std::string> data;
    for (int i = 0; i < 10240; i++) {
        data.push_back(std::string(1_KB, 'x'));
    }
    return data;
}

TEST(THeapProfilerTest, ReadProfile)
{
    absl::SetStackUnwinder(AbslStackUnwinder);
    tcmalloc::MallocExtension::SetProfileSamplingRate(256_KB);

    auto token = tcmalloc::MallocExtension::StartAllocationProfiling();

    EnableMemoryProfilingTags();

    auto traceContext = TTraceContext::NewRoot("Root");
    TTraceContextGuard guard(traceContext);

    traceContext->SetAllocationTags({{"user", "first"}, {"sometag", "my"}});

    auto h0 = BlowHeap<0>();

    int tag = 1;
    traceContext->SetAllocationTags({{"user", "second"}, {"sometag", "notmy"}, {MemoryAllocationTagKey, ToString(tag)}});
    auto currentTag = traceContext->FindAllocationTag<int>(MemoryAllocationTagKey);
    ASSERT_EQ(currentTag, tag);

    auto h1 = BlowHeap<1>();

    traceContext->SetAllocationTagList(nullptr);

    auto h2 = BlowHeap<2>();
    h2.clear();

    auto usage = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTagKey, ToString(tag));
    ASSERT_GE(usage, 5_MB);

    auto dumpProfile = [] (auto name, auto type) {
        auto profile = ReadHeapProfile(type);

        TFileOutput output(GetOutputPath() / name);
        WriteProfile(&output, profile);
        output.Finish();
    };

    dumpProfile("heap.pb.gz", tcmalloc::ProfileType::kHeap);
    dumpProfile("peak.pb.gz", tcmalloc::ProfileType::kPeakHeap);
    dumpProfile("fragmentation.pb.gz", tcmalloc::ProfileType::kFragmentation);
    dumpProfile("allocations.pb.gz", tcmalloc::ProfileType::kAllocations);

    auto profile = std::move(token).Stop();

    TFileOutput output(GetOutputPath() / "allocations.pb.gz");
    WriteProfile(&output, ConvertAllocationProfile(profile));
    output.Finish();
}

TEST(THeapProfilerTest, AllocationTags)
{
    EnableMemoryProfilingTags();
    auto traceContext = TTraceContext::NewRoot("Root");
    TTraceContextGuard guard(traceContext);

    ASSERT_EQ(traceContext->FindAllocationTag<std::string>(MemoryAllocationTagKey), std::nullopt);
    traceContext->SetAllocationTags({{"user", "first user"}, {MemoryAllocationTagKey, MemoryAllocationTagValues[0]}});
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>("user"), "first user");
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>(MemoryAllocationTagKey), MemoryAllocationTagValues[0]);

    std::vector<std::vector<std::string>> heap;
    heap.push_back(BlowHeap<0>());

    traceContext->SetAllocationTags({{"user", "second user"}, {MemoryAllocationTagKey, MemoryAllocationTagValues[1]}});
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>(MemoryAllocationTagKey), 1);

    heap.push_back(BlowHeap<1>());

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[0]);

    auto usage1 = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTagKey, MemoryAllocationTagValues[1]);

    ASSERT_NEAR(usage1, 12_MB, 8_MB);

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[2]);
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>(MemoryAllocationTagKey), MemoryAllocationTagValues[2]);

    {
        volatile auto h = BlowHeap<2>();
    }

    traceContext->SetAllocationTagList(nullptr);
    ASSERT_EQ(traceContext->FindAllocationTag<std::string>(MemoryAllocationTagKey), std::nullopt);

    heap.push_back(BlowHeap<0>());

    {
        auto slice = CollectMemoryUsageSnapshot()->GetUsageSlice(MemoryAllocationTagKey);
        ASSERT_EQ(slice[MemoryAllocationTagValues[1]], usage1);
        ASSERT_LE(slice[MemoryAllocationTagValues[2]], 1_MB);
    }

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[6]);

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[3]);
    heap.push_back(BlowHeap<3>());

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[4]);
    heap.push_back(BlowHeap<4>());

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[7]);

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[5]);
    heap.push_back(BlowHeap<5>());

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[4]);
    heap.push_back(BlowHeap<4>());

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[7]);

    traceContext->SetAllocationTagList(nullptr);

    auto slice = CollectMemoryUsageSnapshot()->GetUsageSlice(MemoryAllocationTagKey);

    constexpr auto maxDifference = 10_MB;
    ASSERT_NEAR(slice[MemoryAllocationTagValues[1]], slice[MemoryAllocationTagValues[3]], maxDifference);
    ASSERT_NEAR(slice[MemoryAllocationTagValues[3]], slice[MemoryAllocationTagValues[5]], maxDifference);
    ASSERT_NEAR(slice[MemoryAllocationTagValues[1]], slice[MemoryAllocationTagValues[5]], maxDifference);

    ASSERT_NEAR(slice[MemoryAllocationTagValues[4]], 20_MB, 15_MB);

    ASSERT_NEAR(slice[MemoryAllocationTagValues[4]], slice[MemoryAllocationTagValues[1]] +  slice[MemoryAllocationTagValues[3]], 2 * maxDifference);
    ASSERT_NEAR(slice[MemoryAllocationTagValues[4]], slice[MemoryAllocationTagValues[1]] +  slice[MemoryAllocationTagValues[5]], 2 * maxDifference);
    ASSERT_NEAR(slice[MemoryAllocationTagValues[4]], slice[MemoryAllocationTagValues[3]] +  slice[MemoryAllocationTagValues[5]], 2 * maxDifference);

    ASSERT_LE(slice[MemoryAllocationTagValues[6]], 1_MB);
    ASSERT_LE(slice[MemoryAllocationTagValues[7]], 1_MB);
}

template <size_t Index>
Y_NO_INLINE auto BlowHeap(int64_t megabytes)
{
    std::vector<std::string> data;
    megabytes <<= 10;
    for (int64_t i = 0; i < megabytes; i++) {
        data.push_back(std::string(1_KB, 'x'));
    }
    return data;
}

TEST(THeapProfilerTest, HugeAllocationsTagsWithMemoryTag)
{
    EnableMemoryProfilingTags();
    auto traceContext = TTraceContext::NewRoot("Root");
    TCurrentTraceContextGuard guard(traceContext);

    std::vector<std::vector<std::string>> heap;

    heap.push_back(BlowHeap<0>());

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[1]);
    ASSERT_EQ(traceContext->FindAllocationTag<int>(MemoryAllocationTagKey), 1);

    heap.push_back(BlowHeap<1>(100));

    {
        traceContext->SetAllocationTagList(nullptr);
        auto usage = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTagKey, MemoryAllocationTagValues[1]);
        ASSERT_GE(usage, 100_MB);
        ASSERT_LE(usage, 150_MB);
    }

    traceContext->SetAllocationTag(MemoryAllocationTagKey, MemoryAllocationTagValues[2]);
    heap.push_back(BlowHeap<1>(1000));

    traceContext->SetAllocationTagList(nullptr);
    auto usage = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTagKey, MemoryAllocationTagValues[2]);
    ASSERT_GE(usage, 1000_MB);
    ASSERT_LE(usage, 1300_MB);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTProf
