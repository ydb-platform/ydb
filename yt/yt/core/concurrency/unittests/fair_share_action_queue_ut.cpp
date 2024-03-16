#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <util/datetime/base.h>

#include <algorithm>
#include <array>
#include <ranges>
#include <utility>

namespace NYT::NConcurrency {
namespace {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TTestFairShareActionQueue
    : public ::testing::Test
{
public:
    TSolomonExporterConfigPtr CreateExporterConfig()
    {
        auto config = New<TSolomonExporterConfig>();
        config->GridStep = TDuration::Seconds(1);
        config->EnableCoreProfilingCompatibility = true;
        config->EnableSelfProfiling = false;

        return config;
    }

    template <class F>
    void SyncRunCallback(IInvokerPtr invoker, F&& func)
    {
        WaitFor(BIND(std::forward<F>(func)).AsyncVia(invoker).Run()).ThrowOnError();
    }

    auto GetSensors(TString json)
    {
        for (auto& c : json) {
            if (c == ':') {
                c = '=';
            } else if (c == ',') {
                c = ';';
            }
        }

        auto yson = NYson::TYsonString(json);

        auto list = NYTree::ConvertToNode(yson)->AsMap()->FindChild("sensors");

        EXPECT_TRUE(list);

        return list->AsList()->GetChildren();
    }

    template <class EQueues, class EBuckets>
    void VerifyJson(TString json, THashMap<EBuckets, std::vector<EQueues>> bucketToQueues)
    {
        TEnumIndexedArray<EQueues, int> enqueuedPerQueue;
        TEnumIndexedArray<EBuckets, int> enqueuedPerBucket;
        int enqueuedPerThread;
        int enqueuedTotal;

        for (const auto& entry : GetSensors(json)) {
            auto mapEntry = entry->AsMap();
            auto labels = mapEntry->FindChild("labels")->AsMap();

            auto sensor = labels->FindChildValue<TString>("sensor");

            if (!sensor ||
                sensor != "yt.action_queue.enqueued")
            {
                continue;
            }

            auto value = mapEntry->FindChildValue<int>("value");
            EXPECT_TRUE(value);

            if (auto threadName = labels->FindChildValue<TString>("thread")) {
                EXPECT_EQ(threadName, "ActionQueue");

                if (auto bucketName = labels->FindChildValue<TString>("bucket")) {
                    if (auto queueName = labels->FindChildValue<TString>("queue")) {
                        enqueuedPerQueue[TEnumTraits<EQueues>::FromString(*queueName)] = *value;
                        continue;
                    }

                    enqueuedPerBucket[TEnumTraits<EBuckets>::FromString(*bucketName)] = *value;
                    continue;
                }

                enqueuedPerThread = *value;
            } else {
                enqueuedTotal = *value;
            }
        }

        EXPECT_EQ(enqueuedPerThread, enqueuedTotal);

        int totalFromBuckets = 0;
        for (auto bucketValue : enqueuedPerBucket) {
            totalFromBuckets += bucketValue;
        }
        EXPECT_EQ(enqueuedPerThread, totalFromBuckets);

        for (auto bucket : TEnumTraits<EBuckets>::GetDomainValues()) {
            int totalFromBucketQueues = 0;

            for (auto queue : bucketToQueues[bucket]) {
                totalFromBucketQueues += enqueuedPerQueue[queue];
            }
            EXPECT_EQ(enqueuedPerBucket[bucket], totalFromBucketQueues);
        }
    }
};

DEFINE_ENUM(EBuckets,
    ((Bucket1) (0))
    ((Bucket2) (1))
);

DEFINE_ENUM(EQueues,
    ((Queue1) (0))
    ((Queue2) (1))
    ((Queue3) (2))
);

TEST_F(TTestFairShareActionQueue, TestProfiling)
{
    auto registry = New<TSolomonRegistry>();

    THashMap<EBuckets, std::vector<EQueues>> bucketToQueues{};
    bucketToQueues[EBuckets::Bucket1] = std::vector{EQueues::Queue1, EQueues::Queue2};
    bucketToQueues[EBuckets::Bucket2] = std::vector{EQueues::Queue3};
    auto queue = CreateEnumIndexedFairShareActionQueue<EQueues>("ActionQueue", bucketToQueues, registry);

    auto config = CreateExporterConfig();
    auto exporter = New<TSolomonExporter>(config, registry);

    exporter->Start();

    for (auto value : TEnumTraits<EQueues>::GetDomainValues()) {
        SyncRunCallback(queue->GetInvoker(value), [] {});
    }

    Sleep(TDuration::Seconds(5));

    auto json = exporter->ReadJson();
    ASSERT_TRUE(json);

    exporter->Stop();

    VerifyJson(*json, std::move(bucketToQueues));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
