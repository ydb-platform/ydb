#include <yt/yt/library/profiling/testing.h>
#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/profiling/sensors_owner/sensors_owner.h>

#include <yt/yt/library/profiling/solomon/config.h>
#include <yt/yt/library/profiling/solomon/exporter.h>
#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/fair_share_hierarchical_queue.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/yson/async_writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/serialize.h>
#include <yt/yt/core/ytree/attribute_consumer.h>
#include <yt/yt/core/ytree/request_complexity_limiter.h>
#include <yt/yt/core/ytree/request_complexity_limits.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/virtual.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/string/format.h>

#include <random>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TTestTag = std::string;

////////////////////////////////////////////////////////////////////////////////

class TResourceHolder
    : public TRefCounted
{
public:
    TResourceHolder(i64 totalResources)
        : TotalResources_(totalResources)
    { }

    i64 GetTotalResources() const
    {
        return TotalResources_;
    }

    i64 GetFreeResources() const
    {
        return TotalResources_ - UsedResources_;
    }

    bool AcquireResource(i64 value)
    {
        auto guard = Guard(Lock_);
        if (TotalResources_ < UsedResources_ + value) {
            return false;
        }

        UsedResources_ += value;
        return true;
    }

    void ReleaseResource(i64 value)
    {
        auto guard = Guard(Lock_);
        UsedResources_ -= value;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    i64 TotalResources_;
    i64 UsedResources_ = 0;
};

DECLARE_REFCOUNTED_CLASS(TResourceHolder);
DEFINE_REFCOUNTED_TYPE(TResourceHolder);

////////////////////////////////////////////////////////////////////////////////

class TFairShareHierarchicalSlotQueueTest
    : public ::testing::Test
{
protected:
    const TFairShareHierarchicalSchedulerPtr<TTestTag> HierarchicalScheduler_ = CreateFairShareHierarchicalScheduler<TTestTag>(
        New<TFairShareHierarchicalSchedulerDynamicConfig>());
    const TFairShareHierarchicalSlotQueuePtr<TTestTag> Queue_ = CreateFairShareHierarchicalSlotQueue(
        HierarchicalScheduler_);

    const TResourceHolderPtr MemoryResourceHolder_ = New<TResourceHolder>(512_MB);
    const TResourceHolderPtr QueueSizeResourceHolder_ = New<TResourceHolder>(128);
};

////////////////////////////////////////////////////////////////////////////////

class TMockFairShareResource
    : public IFairShareHierarchicalSlotQueueResource
{
public:
    TMockFairShareResource(
        TResourceHolderPtr resourceHolder,
        i64 needResources)
        : ResourceHolder_(std::move(resourceHolder))
        , NeedResources_(needResources)
    { }

    bool IsAcquired() const override
    {
        return IsAcquired_;
    }

    i64 GetNeedResourcesCount() const override
    {
        return NeedResources_;
    }

    i64 GetFreeResourcesCount() const override
    {
        return ResourceHolder_->GetFreeResources();
    }

    i64 GetTotalResourcesCount() const override
    {
        return ResourceHolder_->GetTotalResources();
    }

    TError AcquireResource() override
    {
        auto result = ResourceHolder_->AcquireResource(NeedResources_);

        if (result) {
            IsAcquired_ = true;
            return TError();
        }

        return TError("Acquire failed");
    }

    void ReleaseResource() override
    {
        if (IsAcquired_) {
            ResourceHolder_->ReleaseResource(NeedResources_);
            IsAcquired_ = false;
        }
    }

private:
    TResourceHolderPtr ResourceHolder_;
    i64 NeedResources_;
    bool IsAcquired_ = false;
};

TEST_F(TFairShareHierarchicalSlotQueueTest, EnqueueDequeue)
{
    auto resource1 = New<TMockFairShareResource>(MemoryResourceHolder_, 10);
    auto resource2 = New<TMockFairShareResource>(QueueSizeResourceHolder_, 1);
    std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources = {resource1, resource2};
    std::vector<TFairShareHierarchyLevel<TTestTag>> levels = {
        {"root", 1.0},
        {"user1", 0.5}
    };

    auto result = Queue_->EnqueueSlot(20_MB, resources, levels);
    EXPECT_TRUE(result.IsOK());

    EXPECT_TRUE(resource1->IsAcquired());
    EXPECT_TRUE(resource2->IsAcquired());

    auto slot = Queue_->PeekSlot({result.Value()->GetSlotId()});
    EXPECT_NE(slot, nullptr);

    slot->ReleaseResources();
    EXPECT_FALSE(resource1->IsAcquired());
    EXPECT_FALSE(resource2->IsAcquired());
}

TEST_F(TFairShareHierarchicalSlotQueueTest, PriorityOrder)
{
    auto resource1 = New<TMockFairShareResource>(MemoryResourceHolder_, 10);
    auto resource2 = New<TMockFairShareResource>(QueueSizeResourceHolder_, 1);

    std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources = {resource1, resource2};

    std::vector<TFairShareHierarchyLevel<TTestTag>> highPriorityLevels = {
        {"root", 1.0},
        {"user1", 0.8}
    };
    auto highPriorityItem = Queue_->EnqueueSlot(20_MB, resources, highPriorityLevels);
    EXPECT_TRUE(highPriorityItem.IsOK());

    std::vector<TFairShareHierarchyLevel<TTestTag>> lowPriorityLevels = {
        {"root", 1.0},
        {"user2", 0.5}
    };
    auto lowPriorityItem = Queue_->EnqueueSlot(20_MB, resources, lowPriorityLevels);
    EXPECT_TRUE(lowPriorityItem.IsOK());

    auto slot = Queue_->PeekSlot({lowPriorityItem.Value()->GetSlotId(), highPriorityItem.Value()->GetSlotId()});
    EXPECT_NE(slot, nullptr);

    EXPECT_EQ(slot->GetLevels()[1].GetWeight(), 0.8);
}

TEST_F(TFairShareHierarchicalSlotQueueTest, Preemption)
{
    std::vector<TFairShareHierarchyLevel<TTestTag>> lowPriorityLevels = {
        {"root", 1.0},
        {"user1", 0.5}
    };

    auto lowPriorityItem = Queue_->EnqueueSlot(
        20_MB,
        {
            New<TMockFairShareResource>(MemoryResourceHolder_, 20_MB),
            New<TMockFairShareResource>(QueueSizeResourceHolder_, 1)
        },
        lowPriorityLevels);
    EXPECT_TRUE(lowPriorityItem.IsOK());

    std::vector<TFairShareHierarchyLevel<TTestTag>> highPriorityLevels = {
        {"root", 1.0},
        {"user2", 0.8}
    };

    auto highPriorityItem = Queue_->EnqueueSlot(
        20_MB,
        {
            New<TMockFairShareResource>(MemoryResourceHolder_, 510_MB),
            New<TMockFairShareResource>(QueueSizeResourceHolder_, 1)
        },
        highPriorityLevels);
    EXPECT_TRUE(highPriorityItem.IsOK());

    auto slot = Queue_->PeekSlot({lowPriorityItem.Value()->GetSlotId(), highPriorityItem.Value()->GetSlotId()});
    EXPECT_NE(slot, nullptr);
    EXPECT_EQ(slot->GetLevels()[1].GetWeight(), 0.8);
}

TEST_F(TFairShareHierarchicalSlotQueueTest, ConsumedWeightUpdate)
{
    // Create resources.
    auto resource1 = New<TMockFairShareResource>(MemoryResourceHolder_, 10);
    auto resource2 = New<TMockFairShareResource>(QueueSizeResourceHolder_, 1);
    std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources = {resource1, resource2};
    std::vector<TFairShareHierarchyLevel<TTestTag>> levels = {
        {"root", 1.0},
        {"user1", 0.5}
    };

    auto result = Queue_->EnqueueSlot(20_MB, resources, levels);
    EXPECT_TRUE(result.IsOK());

    auto bucket = HierarchicalScheduler_->GetBucket(std::vector<TTestTag>({"root", "user1"}));
    EXPECT_EQ(bucket->SlotWindowSize, static_cast<i64>(20_MB));

    auto slot = Queue_->PeekSlot({result.Value()->GetSlotId()});
    EXPECT_NE(slot, nullptr);
}

TEST_F(TFairShareHierarchicalSlotQueueTest, ErrorHandling)
{
    auto resource1 = New<TMockFairShareResource>(MemoryResourceHolder_, 1024_MB);
    auto resource2 = New<TMockFairShareResource>(QueueSizeResourceHolder_, 1);
    std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources = {resource1, resource2};
    std::vector<TFairShareHierarchyLevel<TTestTag>> levels = {
        {"root", 1.0},
        {"user1", 0.5}
    };

    auto errorResult = Queue_->EnqueueSlot(1024_MB, resources, levels);
    EXPECT_FALSE(errorResult.IsOK());
}

TEST_F(TFairShareHierarchicalSlotQueueTest, TrimFunctionalityAndMetrics)
{
    constexpr i64 TotalMemory = 512_MB;
    constexpr i64 QueueSizeLimit = 128;

    auto memoryResourceHolder = New<TResourceHolder>(TotalMemory);
    auto queueSizeResourceHolder = New<TResourceHolder>(QueueSizeLimit);
    auto profiler = NProfiling::TProfiler("/test_trim");
    auto config = New<TFairShareHierarchicalSchedulerDynamicConfig>();
    config->WindowSize = TDuration::Seconds(2);
    auto hierarchicalScheduler = CreateFairShareHierarchicalScheduler<TTestTag>(
        config,
        profiler);
    auto queue = CreateFairShareHierarchicalSlotQueue(hierarchicalScheduler, profiler);

    auto resource1 = New<TMockFairShareResource>(memoryResourceHolder, 10);
    auto resource2 = New<TMockFairShareResource>(queueSizeResourceHolder, 1);

    std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources = {resource1, resource2};
    std::vector<TFairShareHierarchyLevel<TTestTag>> levels = {
        {"root", 1.0},
        {"user1", 0.5}
    };

    auto slot = queue->EnqueueSlot(10_MB, resources, levels);
    EXPECT_TRUE(slot.IsOK());
    EXPECT_FALSE(queue->IsEmpty());

    queue->DequeueSlot(slot.Value());

    Sleep(TDuration::Seconds(4));

    EXPECT_NE(0, hierarchicalScheduler->GetBucket({})->SlotWindowSize);

    hierarchicalScheduler->TrimLog();

    EXPECT_EQ(0, hierarchicalScheduler->GetBucket({})->SlotWindowSize);
    EXPECT_TRUE(queue->IsEmpty());
}

struct TStressTestConfig
{
    int NumQueues = 1;
    int NumThreadsPerQueue = 1;
    int NumRequestsPerThread = 2048;

    i64 TotalMemory = 512_MB;
    i64 MaxRequestSize = 16_MB;
    i64 QueueSizeLimit = 128;

    int EnqueuePercent = 80;

    std::vector<int> TagHierarchyCounts = {3};
    std::optional<int> SplitRequests;
};

class TFairShareHierarchicalSlotQueueStressTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TStressTestConfig>
{ };

std::vector<std::vector<TTestTag>> GenerateTagLists(
    std::vector<TTestTag> tagList,
    std::vector<int> tagHierarchyCounts) {
    std::vector<std::vector<TTestTag>> tagLists;

    tagLists.push_back(tagList);
    if (tagHierarchyCounts.empty()) {
        return tagLists;
    } else {
        auto it = tagHierarchyCounts.begin();
        auto count = *it;
        tagHierarchyCounts.erase(it);
        auto size = tagList.size();
        for (int i = 0; i < count; ++i) {
            auto newList = tagList;
            newList.push_back(Format("tag_%v_%v", size, i));
            auto subTagLists = GenerateTagLists(newList, tagHierarchyCounts);
            auto list = GenerateTagLists(
                newList,
                tagHierarchyCounts);
            tagLists.insert(tagLists.end(), list.begin(), list.end());
        }

        return tagLists;
    }
}

struct TNodeStat
{
    TGuid RequestId;

    std::vector<std::pair<TTestTag, double>> Levels{};
    std::atomic<i64> SlotBandwidth;
    std::atomic<i64> SlotCount;
    std::atomic<i64> RequestBandwidth;
    std::atomic<i64> RequestCount;
};

void VerifyBucketTree(
    std::vector<TTestTag> tagList,
    const TFairShareHierarchicalScheduler<TTestTag>::TFairShareHierarchicalSlotQueueBucketNodePtr& current,
    const THashMap<TGuid, TNodeStat>& stats)
{
    for (auto& [_, bucket] : current->Buckets) {
        auto weight = (bucket->SummarySlotWeight / bucket->SlotWindowLogCount) / 100;
        {
            auto part = 1.0 * bucket->SlotWindowSize / current->SlotWindowSize;
            EXPECT_NEAR(
                weight,
                part,
                0.15);
        }

        {
            auto part = 1.0 * bucket->RequestWindowSize / current->RequestWindowSize;
            EXPECT_NEAR(
                weight,
                part,
                0.15);
        }
    }

    for (auto& [tag, child] : current->Buckets) {
        auto list = tagList;
        list.push_back(tag);
        VerifyBucketTree(list, child, stats);
    }
}

TEST_P(TFairShareHierarchicalSlotQueueStressTest, StressTest)
{
    auto& config = GetParam();

    int numQueues = config.NumQueues;
    int numThreadsPerQueue = config.NumThreadsPerQueue;
    int numRequestsPerThread = config.NumRequestsPerThread;

    i64 totalMemory = config.TotalMemory;
    i64 maxRequestSize = config.MaxRequestSize;
    i64 queueSizeLimit = config.QueueSizeLimit;

    int enqueuePercent = config.EnqueuePercent;

    std::vector<int> tagHierarchyCounts = config.TagHierarchyCounts;
    std::optional<int> splitRequests = config.SplitRequests;

    const NLogging::TLogger Logger("StressTest");

    TSpinLock SlotToStatLock;
    THashMap<TGuid, TGuid> SlotIdToStatId;

    std::mt19937 randomGenerator(142857);

    auto splitSegment = [&] (int n, int m) {
        std::uniform_int_distribution<> dist(1, n - 1);

        std::vector<int> points;
        for (int i = 0; i < m - 1; ++i) {
            points.push_back(dist(randomGenerator));
        }

        std::sort(points.begin(), points.end());

        points.insert(points.begin(), 0);
        points.push_back(n);

        std::vector<int> lengths;
        for (size_t i = 1; i < points.size(); ++i) {
            lengths.push_back(points[i] - points[i - 1]);
        }

        return lengths;
    };

    THashMap<std::string, NYT::NProfiling::TShardConfigPtr> shards;
    auto addShardConfig = [&shards] (const std::string& shardName) {
        auto shardConfig = New<NProfiling::TShardConfig>();
        shardConfig->GridStep = TDuration::Seconds(5);
        shardConfig->Filter = {shardName};

        shards.try_emplace(shardName, shardConfig);
    };
    addShardConfig("/");

    auto solomonConfig = New<NProfiling::TSolomonExporterConfig>();
    solomonConfig->GridStep = TDuration::Seconds(5);
    solomonConfig->EnableCoreProfilingCompatibility = true;
    solomonConfig->EnableSelfProfiling = false;
    solomonConfig->Shards = std::move(shards);

    auto registry = New<NProfiling::TSolomonRegistry>();
    NProfiling::TProfiler profiler(registry, "/");
    auto exporter = NYT::New<NProfiling::TSolomonExporter>(solomonConfig, registry);

    exporter->Start();

    auto tagLists = GenerateTagLists({}, tagHierarchyCounts);
    auto it = tagLists.begin();
    while (it != tagLists.end()) {
        if (it->size() != tagHierarchyCounts.size()) {
            it = tagLists.erase(it);
        } else {
            ++it;
        }
    }

    THashMap<int, THashMap<TTestTag, double>> tagHierarchy;

    for (int i = 0; i < std::ssize(tagHierarchyCounts); ++i) {
        std::vector<int> lengths = splitSegment(100, tagHierarchyCounts[i]);
        for (int j = 0; j < tagHierarchyCounts[i]; ++j) {
            auto tag = Format("tag_%v_%v", i, j);
            tagHierarchy[i].emplace(tag, lengths[j]);
            YT_LOG_DEBUG("Tag: %v, Weight: %v", tag, lengths[j]);
        }
    }

    for (auto& tagList : tagLists) {
        YT_LOG_DEBUG("TagList: %v", tagList);
    }

    THashMap<TGuid, TNodeStat> stats;

    for (auto& tagList : tagLists) {
        std::vector<std::pair<TTestTag, double>> levels;
        int index = 0;
        for (auto& tag : tagList) {
            levels.push_back({tag, tagHierarchy[index][tag]});
            index++;
        }

        auto guid = TGuid::Create();
        stats[guid].Levels = levels;
        stats[guid].RequestId = guid;
        stats[guid].SlotBandwidth = 0;
        stats[guid].SlotCount = 0;
        stats[guid].RequestBandwidth = 0;
        stats[guid].RequestCount = 0;

        YT_LOG_DEBUG("Levels: %v", levels);
    }

    std::uniform_int_distribution<i64> sizeDist(1_MB, maxRequestSize);

    auto schedulerConfig = New<TFairShareHierarchicalSchedulerDynamicConfig>();
    schedulerConfig->WindowSize = TDuration::Seconds(5);

    auto hierarchicalScheduler = CreateFairShareHierarchicalScheduler<TTestTag>(
        schedulerConfig,
        profiler.WithPrefix("/scheduler"));

    struct TQueueRequests
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        THashMap<TGuid, int> SlotIdToRequestCount;
        THashSet<TGuid> SlotIds;
        THashMap<TGuid, i64> SlotIdToRequestSize;
    };

    std::vector<TFairShareHierarchicalSlotQueuePtr<TTestTag>> rawQueues;
    std::vector<TQueueRequests> queueRequests(numQueues);
    std::vector<TResourceHolderPtr> memoryResourceHolders;
    std::vector<TResourceHolderPtr> queueSizeResourceHolders;
    for (int i = 0; i < numQueues; ++i) {
        rawQueues.push_back(CreateFairShareHierarchicalSlotQueue(
            hierarchicalScheduler,
            profiler.WithPrefix(Format("/queue_%v", i))));
        memoryResourceHolders.push_back(New<TResourceHolder>(totalMemory));
        queueSizeResourceHolders.push_back(New<TResourceHolder>(queueSizeLimit));
    }

    const std::vector<TFairShareHierarchicalSlotQueuePtr<TTestTag>> queues = rawQueues;

    auto keys = GetKeys(stats);
    auto createRandomRequest = [&] (int queueIndex) mutable {
        auto queue = queues[queueIndex];
        size_t tagListIndex = randomGenerator() % keys.size();
        auto tagsKey = keys[tagListIndex];
        auto& stat = stats[tagsKey];
        auto tags = stats[tagsKey].Levels;

        i64 size = sizeDist(randomGenerator);

        auto memoryResource = New<TMockFairShareResource>(memoryResourceHolders[queueIndex], size);
        auto queueSizeResource = New<TMockFairShareResource>(queueSizeResourceHolders[queueIndex], 1);
        std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources = {queueSizeResource};

        std::vector<TFairShareHierarchyLevel<TTestTag>> levels;
        levels.reserve(tags.size());
        for (auto& tag : tags) {
            levels.push_back(TFairShareHierarchyLevel<TTestTag>(tag.first, tag.second));
        }

        YT_LOG_TRACE("Try to put slot (Size: %v, Levels: %v)", size, tags);

        auto slot = queue->EnqueueSlot(
            size,
            std::move(resources),
            std::move(levels));

        if (slot.IsOK()) {
            auto slotId = slot.Value()->GetSlotId();
            stat.SlotBandwidth += size;
            stat.SlotCount++;

            {
                auto guard = Guard(SlotToStatLock);
                EmplaceOrCrash(SlotIdToStatId, slotId, tagsKey);
            }

            {
                auto& requests = queueRequests[queueIndex];
                auto guard = Guard(requests.Lock);
                int requestCount = splitRequests.value_or(1);
                i64 requestSize = size / requestCount;
                EmplaceOrCrash(requests.SlotIdToRequestCount, slotId, requestCount);
                EmplaceOrCrash(requests.SlotIdToRequestSize, slotId, requestSize);
                EmplaceOrCrash(requests.SlotIds, slotId);
            };

            slot.Value()->SubscribeCancelled(BIND([
                &queueRequests,
                queueIndex = queueIndex,
                tagsKey = tagsKey,
                slotId = slotId,
                size = size,
                &stats] (const TError& /*error*/) {
                auto& stat = stats[tagsKey];
                stat.SlotBandwidth -= size;
                stat.SlotCount--;
                {
                    auto& requests = queueRequests[queueIndex];
                    auto guard = Guard(requests.Lock);
                    requests.SlotIds.erase(slotId);
                    requests.SlotIdToRequestCount.erase(slotId);
                    requests.SlotIdToRequestSize.erase(slotId);
                };
            }));

            YT_LOG_TRACE("Slot put successfully (Size: %v, Levels: %v)", size, tags);
        } else {
            YT_LOG_TRACE("Slot put failed (Size: %v, Levels: %v)", size, tags);
        }
    };

    auto dequeueFromQueue = [&] (int queueIndex) mutable {
        TFairShareHierarchicalSlotQueueSlotPtr<TTestTag> slot;
        auto queue = queues[queueIndex];
        i64 requestSize = 0;
        bool dequeued = false;

        if (queue->IsEmpty()) {
            return;
        }

        {
            auto guard = Guard(queueRequests[queueIndex].Lock);
            slot = queue->PeekSlot(queueRequests[queueIndex].SlotIds);

            if (slot == nullptr) {
                return;
            }

            requestSize = queueRequests[queueIndex].SlotIdToRequestSize[slot->GetSlotId()];
            queueRequests[queueIndex].SlotIdToRequestCount[slot->GetSlotId()]--;

            if (queueRequests[queueIndex].SlotIdToRequestCount[slot->GetSlotId()] == 0) {
                queue->DequeueSlot(slot);
                queueRequests[queueIndex].SlotIds.erase(slot->GetSlotId());
                queueRequests[queueIndex].SlotIdToRequestSize.erase(slot->GetSlotId());
                queueRequests[queueIndex].SlotIdToRequestCount.erase(slot->GetSlotId());
                dequeued = true;
            }
        }

        queue->AccountSlot(slot, requestSize);

        TGuid statId;
        {
            auto guard = Guard(SlotToStatLock);
            statId = GetOrCrash(SlotIdToStatId, slot->GetSlotId());
        }

        auto& stat = stats[statId];
        stat.RequestBandwidth += requestSize;
        stat.RequestCount++;

        if (dequeued) {
            slot->ReleaseResources();
        }
    };

    const auto processQueue = [&] (int queueIndex) {
        auto queue = queues[queueIndex];
        while (!queue->IsEmpty()) {
            dequeueFromQueue(queueIndex);
        }
    };

    auto threadPool = NConcurrency::CreateThreadPool(numQueues * numThreadsPerQueue, "StressTestThread");

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < numQueues; ++i) {
        for (int j = 0; j < numThreadsPerQueue; ++j) {
            auto future = BIND([&, index = i, queue = queues[i]] {
                for (int k = 0; k < numRequestsPerThread; ++k) {
                    int action = randomGenerator() % 100;
                    if (action < enqueuePercent) {
                        createRandomRequest(index);
                    } else {
                        dequeueFromQueue(index);
                    }

                    if (randomGenerator() % 200 == 0) {
                        if (auto sensors = exporter->ReadJson({})) {
                            YT_LOG_DEBUG("Sensors: %v", *sensors);
                        }
                        TStringStream output;
                        NYson::TYsonWriter writer(&output, NYson::EYsonFormat::Pretty, NYson::EYsonType::MapFragment);
                        hierarchicalScheduler->BuildOrchid(&writer);
                        writer.Flush();
                        YT_LOG_DEBUG(
                            "Orchid: %v",
                            NYson::TYsonString(output.Str(), NYson::EYsonType::MapFragment));
                    }
                }
            }).AsyncVia(threadPool->GetInvoker()).Run();
            futures.push_back(future);
        }
    }

    NConcurrency::WaitFor(AllSucceeded(futures)).ThrowOnError();

    for (int i = 0; i < numQueues; ++i) {
        processQueue(i);
    }

    auto rootBucket = hierarchicalScheduler->GetBucket({});
    VerifyBucketTree({}, rootBucket, stats);
}

INSTANTIATE_TEST_SUITE_P(
    TFairShareHierarchicalSlotQueueStressTest,
    TFairShareHierarchicalSlotQueueStressTest,
    ::testing::Values(
        TStressTestConfig{},
        TStressTestConfig{
            .TagHierarchyCounts = {3, 3}
        },
        TStressTestConfig{
            .SplitRequests = 4,
        },
        TStressTestConfig{
            .EnqueuePercent = 70,
        },
        TStressTestConfig{
            .EnqueuePercent = 95,
        },
        TStressTestConfig{
            .NumQueues = 8,
            .NumThreadsPerQueue = 8,
        },
        TStressTestConfig{
            .NumQueues = 8,
            .NumThreadsPerQueue = 8,
            .TagHierarchyCounts = {3, 3},
        },
        TStressTestConfig{
            .NumQueues = 8,
            .NumThreadsPerQueue = 8,
            .SplitRequests = 4,
        },
        TStressTestConfig{
            .NumQueues = 8,
            .NumThreadsPerQueue = 8,
            .EnqueuePercent = 70,
        },
        TStressTestConfig{
            .NumQueues = 8,
            .NumThreadsPerQueue = 8,
            .EnqueuePercent = 95,
        }
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
