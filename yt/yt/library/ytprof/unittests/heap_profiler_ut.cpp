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

constexpr auto MemoryAllocationTag = "memory_allocation_tag";
const std::vector<TString> MemoryAllocationTags = {"0", "1", "2", "3", "4", "5", "6", "7"};

////////////////////////////////////////////////////////////////////////////////

template <size_t Index>
Y_NO_INLINE auto BlowHeap()
{
    std::vector<TString> data;
    for (int i = 0; i < 10240; i++) {
        data.push_back(TString(1024, 'x'));
    }
    return data;
}

TEST(HeapProfiler, ReadProfile)
{
    absl::SetStackUnwinder(AbslStackUnwinder);
    tcmalloc::MallocExtension::SetProfileSamplingRate(256_KB);

    auto token = tcmalloc::MallocExtension::StartAllocationProfiling();

    EnableMemoryProfilingTags();
    auto traceContext = TTraceContext::NewRoot("Root");
    TTraceContextGuard guard(traceContext);

    traceContext->SetAllocationTags({{"user", "first"}, {"sometag", "my"}});

    auto h0 = BlowHeap<0>();

    auto tag = TMemoryTag(1);
    traceContext->SetAllocationTags({{"user", "second"}, {"sometag", "notmy"}, {MemoryAllocationTag, ToString(tag)}});
    auto currentTag = traceContext->FindAllocationTag<TMemoryTag>(MemoryAllocationTag);
    ASSERT_EQ(currentTag, tag);

    auto h1 = BlowHeap<1>();

    traceContext->ClearAllocationTagsPtr();

    auto h2 = BlowHeap<2>();
    h2.clear();

    auto usage = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTag, ToString(tag));
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

TEST(HeapProfiler, AllocationTagsWithMemoryTag)
{
    EnableMemoryProfilingTags();
    auto traceContext = TTraceContext::NewRoot("Root");
    TTraceContextGuard guard(traceContext);

    ASSERT_EQ(traceContext->FindAllocationTag<TString>(MemoryAllocationTag), std::nullopt);
    traceContext->SetAllocationTags({{"user", "first user"}, {MemoryAllocationTag, MemoryAllocationTags[0]}});
    ASSERT_EQ(traceContext->FindAllocationTag<TString>("user"), "first user");
    ASSERT_EQ(traceContext->FindAllocationTag<TString>(MemoryAllocationTag), MemoryAllocationTags[0]);

    std::vector<std::vector<TString>> heap;
    heap.push_back(BlowHeap<0>());

    traceContext->SetAllocationTags({{"user", "second user"}, {MemoryAllocationTag, MemoryAllocationTags[1]}});
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>(MemoryAllocationTag), 1);

    heap.push_back(BlowHeap<1>());

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[0]);

    auto usage1 = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTag, MemoryAllocationTags[1]);

    ASSERT_NEAR(usage1, 12_MB, 8_MB);

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[2]);
    ASSERT_EQ(traceContext->FindAllocationTag<TString>(MemoryAllocationTag), MemoryAllocationTags[2]);

    {
        volatile auto h = BlowHeap<2>();
    }

    traceContext->ClearAllocationTagsPtr();
    ASSERT_EQ(traceContext->FindAllocationTag<TString>(MemoryAllocationTag), std::nullopt);

    heap.push_back(BlowHeap<0>());

    {
        auto snapshot = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTag);
        ASSERT_EQ(snapshot[MemoryAllocationTags[1]], usage1);
        ASSERT_LE(snapshot[MemoryAllocationTags[2]], 1_MB);
    }

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[6]);

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[3]);
    heap.push_back(BlowHeap<3>());

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[4]);
    heap.push_back(BlowHeap<4>());

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[7]);

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[5]);
    heap.push_back(BlowHeap<5>());

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[4]);
    heap.push_back(BlowHeap<4>());

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[7]);

    traceContext->SetAllocationTagsPtr(nullptr);

    auto snapshot = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTag);

    constexpr auto maxDifference = 10_MB;
    ASSERT_NEAR(snapshot[MemoryAllocationTags[1]], snapshot[MemoryAllocationTags[3]], maxDifference);
    ASSERT_NEAR(snapshot[MemoryAllocationTags[3]], snapshot[MemoryAllocationTags[5]], maxDifference);
    ASSERT_NEAR(snapshot[MemoryAllocationTags[1]], snapshot[MemoryAllocationTags[5]], maxDifference);

    ASSERT_NEAR(snapshot[MemoryAllocationTags[4]], 20_MB, 15_MB);

    ASSERT_NEAR(snapshot[MemoryAllocationTags[4]], snapshot[MemoryAllocationTags[1]] +  snapshot[MemoryAllocationTags[3]], 2 * maxDifference);
    ASSERT_NEAR(snapshot[MemoryAllocationTags[4]], snapshot[MemoryAllocationTags[1]] +  snapshot[MemoryAllocationTags[5]], 2 * maxDifference);
    ASSERT_NEAR(snapshot[MemoryAllocationTags[4]], snapshot[MemoryAllocationTags[3]] +  snapshot[MemoryAllocationTags[5]], 2 * maxDifference);

    ASSERT_LE(snapshot[MemoryAllocationTags[6]], 1_MB);
    ASSERT_LE(snapshot[MemoryAllocationTags[7]], 1_MB);
}

template <size_t Index>
Y_NO_INLINE auto BlowHeap(int64_t megabytes)
{
    std::vector<TString> data;
    megabytes <<= 10;
    for (int64_t i = 0; i < megabytes; i++) {
        data.push_back(TString( 1024, 'x'));
    }
    return data;
}

TEST(HeapProfiler, HugeAllocationsTagsWithMemoryTag)
{
    EnableMemoryProfilingTags();
    auto traceContext = TTraceContext::NewRoot("Root");
    TCurrentTraceContextGuard guard(traceContext);

    std::vector<std::vector<TString>> heap;

    heap.push_back(BlowHeap<0>());

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[1]);
    ASSERT_EQ(traceContext->FindAllocationTag<TMemoryTag>(MemoryAllocationTag), 1);

    heap.push_back(BlowHeap<1>(100));

    {
        traceContext->SetAllocationTagsPtr(nullptr);
        auto usage = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTag, MemoryAllocationTags[1]);
        ASSERT_GE(usage, 100_MB);
        ASSERT_LE(usage, 150_MB);
    }

    traceContext->SetAllocationTag(MemoryAllocationTag, MemoryAllocationTags[2]);
    heap.push_back(BlowHeap<1>(1000));

    traceContext->SetAllocationTagsPtr(nullptr);
    auto usage = CollectMemoryUsageSnapshot()->GetUsage(MemoryAllocationTag, MemoryAllocationTags[2]);
    ASSERT_GE(usage, 1000_MB);
    ASSERT_LE(usage, 1300_MB);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTProf
