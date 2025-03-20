#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// This class describes one of the resources needed to process a slot in a hierarchical fair share queue.
// Resources are allocated at the time of slot creation, resource deallocation occurs at any point in time,
// it depends on the needs. If a slot has been displaced in favor of another higher priority resource,
// the resource is deallocated immediately. Such a resource can be, for example, CPU, RAM, etc.
struct IFairShareHierarchicalSlotQueueResource
    : public TRefCounted
{
    // Returns true if the resource has been successfully allocated. It is important to note that,
    // for example, it is impossible to allocate a CPU as a resource, but you can, for example,
    // report a shortage if there was not enough CPU at the time of allocation.
    virtual bool IsAcquired() const = 0;

    // Returns the number of resources needed to create a slot.
    virtual i64 GetNeedResourcesCount() const = 0;

    // Returns the number of resources available in total.
    virtual i64 GetFreeResourcesCount() const = 0;

    // Returns the total size of this resource. It is necessary to
    // check that the slot creation request does not exceed the total limit.
    virtual i64 GetTotalResourcesCount() const = 0;

    // Allocates a resource. Returns an error describing a
    // lack of resource if the allocation failed.
    virtual TError AcquireResource() = 0;

    // Release an allocated resource.
    virtual void ReleaseResource() = 0;
};

DECLARE_REFCOUNTED_STRUCT(IFairShareHierarchicalSlotQueueResource)
DEFINE_REFCOUNTED_TYPE(IFairShareHierarchicalSlotQueueResource)

////////////////////////////////////////////////////////////////////////////////

// To maintain a hierarchical fair share queue, you need to have an entity
// that describes the hierarchy level. The hierarchy level is the tag and the set weight.
template <typename TTag>
class TFairShareHierarchyLevel
{
public:
    TFairShareHierarchyLevel(TTag tag, double weight);

    const TTag& GetTag() const;

    double GetWeight() const;

private:
    const TTag Tag_;
    const double Weight_;
};

////////////////////////////////////////////////////////////////////////////////

struct TFairShareLogKey
{
    TGuid RequestId;
    TInstant CreatedAt;

    std::strong_ordering operator<=>(const TFairShareLogKey& other) const;

    operator size_t() const;
};

////////////////////////////////////////////////////////////////////////////////

// A slot in the queue represents the consumer at a given time (usually it can be a read or write request).
// The slot is described by several important parameters. These are the size, the path of the tags with weights,
// and the resources needed to work. The slot is operated as follows:
//     1) A slot is being created. If there are enough resources, the slot is created immediately, otherwise the slot can
//        either displace a lower-priority slot, or it can be interrupted if the slot's priority is less than the priorities
//        of all slots in the queue. If the slot was created successfully, the user receives a pointer to the slot at the output.
//     2) The hierarchical fair share queue processes the queue in order of priority (priorities will be discussed later).
//     3) During processing, the handler gets a slot that needs to be processed and performs a certain operation.
//     4) The processor writes consumption information to the slot. Consumption can be divided into several stages.
//        For example, it can be useful for processing requests in parts.
//     5) When the request is completed, the slot is deleted.
template <typename TTag>
class TFairShareHierarchicalSlotQueueSlot
    : public TRefCounted
{
public:
    DEFINE_SIGNAL(void(TError), Cancelled);

public:
    TFairShareHierarchicalSlotQueueSlot(
        i64 size,
        std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources,
        std::vector<TFairShareHierarchyLevel<TTag>> levels);

    void Cancel(TError error);

    const std::vector<IFairShareHierarchicalSlotQueueResourcePtr>& GetResources() const;
    const std::vector<TFairShareHierarchyLevel<TTag>>& GetLevels() const;

    TFairShareSlotId GetSlotId() const;
    TInstant GetEnqueueTime() const;
    i64 GetSize() const;

    bool NeedExceedsLimit() const;

    TError AcquireResources();
    void ReleaseResources();

private:
    const TFairShareSlotId SlotId_;
    const i64 Size_;
    const std::vector<TFairShareHierarchyLevel<TTag>> Levels_;
    const TInstant EnqueueTime_;

    std::vector<IFairShareHierarchicalSlotQueueResourcePtr> Resources_;
};

////////////////////////////////////////////////////////////////////////////////

// This class represents a log in a hierarchical fair share scheduler. It describes the type of consumption,
// the time of consumption, the path in the hierarchy, and the amount of consumption. Based on the historical
// log data, a window is formed that describes the consumption so far of each path. There are two main types
// of consumption: slot consumption and request consumption.
template <typename TTag>
class TFairShareHierarchicalSchedulerLog
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TGuid, RequestId);
    DEFINE_BYVAL_RO_PROPERTY(i64, Size);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Slot);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, CreatedAt);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TFairShareHierarchyLevel<TTag>>, Levels);

public:
    TFairShareHierarchicalSchedulerLog(
        TGuid requestId,
        i64 size,
        bool isSlot,
        std::vector<TFairShareHierarchyLevel<TTag>> levels,
        TInstant createdAt);
};

////////////////////////////////////////////////////////////////////////////////

// This function performs a comparison based on slot insertion time. If the slots are equivalent in
// weight divided by consumption, then there is a move down the hierarchy. This function describes the
// comparison at the lowest level. Most often, this means that the hierarchy matches. This can happen,
// for example, on requests from a single consumer. Then the requests will be processed in fifo order.
template <typename TTag>
bool CompareByEnqueueTime(const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& lhs, const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& rhs);

////////////////////////////////////////////////////////////////////////////////

// This class is designed to manage the consumption tree. The nodes of the tree are buckets. Each bucket
// is annotated with a specific tag. The tree resembles a radix tree. The history of each bucket is stored
// in a specified window, the window is configured via a dynamic config.
//
// The counter values are equal to the sum of the counter values of the child elements.
// Since weights can be marked up with paths of different lengths, the counters will also take into account
// the elements marked up to the required level. Logs without tags end up in the root bucket.
//
// For example Log(tags - size):
//
//                                    ***************************
//                                    *         root            *
//                                    *  slot_window = 25 MB    *
//                                    *  request_window = 12 MB  *
//                                    *  slot_count = 13        *
//                                    *  request_count = 9      *
//                                    ***************************
//                                   *                           *
//                                  *    1. Slot ([], 2 MB)        *
//                                 *     2. Slot ([], 2 MB)          *
//     ***************************       3. Request ([], 3 MB)        ***************************
//     *         pool1           *                                    *         pool2           *
//     *  slot_window = 6 MB     *                                    *  slot_window = 15 MB    *
//     *  request_window = 3 MB  *                                    *  request_window = 6 MB  *
//     *  slot_count = 4         *                                    *  slot_count = 7         *
//     *  request_count = 3      *                                    *  request_count = 5      *
//     ***************************                                    ***************************
//                 *                                                   *           *
//                 *    1. Slot([pool1], 1 MB)                        *             *      1. Slot([pool2], 2 MB)
//                 *    2. Request([pool1], 1 MB)                    *                *    2. Request([pool2], 1 MB)
//                 *                                                *                  *
//                 *                                               *                    *
//     ***************************           ***************************             ***************************
//     *       operation1        *           *       operation2        *             *       operation3        *
//     *  slot_window = 5 MB     *           *  slot_window = 5 MB     *             *  slot_window = 8 MB     *
//     *  request_window = 2 MB  *           *  request_window = 2 MB  *             *  request_window = 3 MB  *
//     *  slot_count = 3         *           *  slot_count = 2         *             *  slot_count = 4         *
//     *  request_count = 2      *           *  request_count = 1      *             *  request_count = 3      *
//     ***************************           ***************************             ***************************
//
//     1. Slot([pool1-operation1] 1 MB)      1. Slot([pool1-operation2] 1 MB)        1. Slot([pool1-operation3] 2 MB)
//     2. Slot([pool1-operation1] 2 MB)      2. Slot([pool1-operation2] 4 MB)        2. Slot([pool1-operation3] 2 MB)
//     3. Slot([pool1-operation1] 2 MB)                                              3. Slot([pool1-operation3] 2 MB)
//                                                                                   4. Slot([pool2-operation3] 2 MB)
//
//     1. Request([pool1-operation1] 1 MB)   1. Request([pool1-operation2] 2 MB)     1. Request([pool1-operation3] 1 MB)
//     2. Request([pool1-operation1] 1 MB)                                           2. Request([pool1-operation3] 1 MB)
//                                                                                   3. Request([pool2-operation3] 1 MB)
//
// The main task of the planner is to weigh two elements, to determine which of them is more important. To do this,
// you need to go down the tree using the element tags. Each element on each level finds a bucket that corresponds to it.
// At each level of the tree, a comparison is performed based on bandwidth 2 * weight 1 < bandwidth 1 * weight 2.
//
// That is, the higher the bandwidth consumed, the smaller the item, and the higher the weight of the item, the larger the item.
// The log elements themselves are stored in two sorted sets. For slots and queries, respectively. They are sorted in the order
// in which the log is received. This is necessary to quickly clear the log of outdated elements.
//
// Tree operations are thread-safe.
//
// There are three main methods of working with tree:
// 1) EnqueueLog
// 2) DequeueLog
// 3) TrimLog
//
// The insertion and deletion operations are symmetric. There is a descent down the tree with an increase in links on the
// traversed buckets.  Buckets are created when inserted, if necessary. When deleting, we guarantee that we will find the bucket
// of the item. As we go down the tree, we update the necessary counters. Buckets that have no links left and are empty are deleted.
//
// The log is cleared in a simple way. In the loop, you need to check if the item is outdated, if so, delete it and repeat the operation.
template <typename TTag>
class TFairShareHierarchicalScheduler
    : public TRefCounted
{
public:
    struct TFairShareHierarchicalSlotQueueBucketNode;
    using TFairShareHierarchicalSlotQueueBucketNodePtr = TIntrusivePtr<TFairShareHierarchicalSlotQueueBucketNode>;

    // The bucket class is responsible for counting different counters to ensure fair share and to
    // be able to perform comparison operations. There is always a root bucket that describes the total consumption.
    struct TFairShareHierarchicalSlotQueueBucketNode
        : public TRefCounted
    {
        TFairShareHierarchicalSlotQueueBucketNode() = default;

        // This number describes how much consumption has been requested and used by the slots.
        // The displacement of one slot in favor of another leads to the fact that no
        // consumption is credited to the displaced element.
        std::atomic<i64> SlotWindowSize = 0;

        // This number describes the consumption of requests taken into work.
        std::atomic<i64> RequestWindowSize = 0;

        // These numbers describe the number of items in the log. A bucket cannot be
        // deleted as long as these numbers are non-zero.
        std::atomic<i64> SlotWindowLogCount = 0;
        std::atomic<i64> RequestWindowLogCount = 0;

        // These numbers describe the total weight in the window. This is necessary for diagnosis.
        // Fair share should aim for an average weight.
        std::atomic<double> SummarySlotWeight = 0.0;
        std::atomic<double> SummaryRequestWeight = 0.0;

        // The counters below may not be consistent, as consistency requires additional synchronization.
        // On a large stream, this inconsistency may not be noticeable.
        // This is a local fair share and is calculated as the ratio of consumed to consumed by the parent.
        std::atomic<double> SlotFairShare = 1.0;
        std::atomic<double> RequestFairShare = 1.0;

        // This is a global fair share that describes consumption relative to the root bucket.
        std::atomic<double> SlotTotalFairShare = 1.0;
        std::atomic<double> RequestTotalFairShare = 1.0;

        // To correctly traverse the tree without a global lock, you need per bucket locks.
        // In addition, it is necessary to have a bucket reference counter in order not to
        // delete the bucket at the time of inserting a new log.
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, BucketLock);
        THashMap<TTag, TFairShareHierarchicalSlotQueueBucketNodePtr> Buckets;
        std::atomic<i64> RefCount = 0;

        // This method builds a bucket node in the orchid, which is necessary for diagnosis.
        // {
        // "root" = {
        //     "slot_fair_share" = 1.;
        //     "request_fair_share" = 1.;
        //     "slot_total_fair_share" = 1.;
        //     "request_total_fair_share" = 1.;
        //     "slot_weight" = 1.;
        //     "request_weight" = 1.;
        //     "slot_window_size" = 29549656418;
        //     "request_window_size" = 32182763805;
        //     "slot_window_log_count" = 22668;
        //     "request_window_log_count" = 24669;
        //     "buckets" = {
        //         "user1" = {
        //             "slot_fair_share" = 0.29994889618592224;
        //             "request_fair_share" = 0.2999651517240905;
        //             "slot_total_fair_share" = 0.29994889618592224;
        //             "request_total_fair_share" = 0.2999651517240905;
        //             "slot_weight" = 0.2999999999999693;
        //             "request_weight" = 0.29999999999998644;
        //             "slot_window_size" = 8862733281;
        //             "request_window_size" = 9654114417;
        //             "slot_window_log_count" = 6871;
        //             "request_window_log_count" = 7474;
        //             "buckets" = {};
        //         };
        //         ...
        //    };
        // };
        void BuildOrchid(NYTree::TFluentAny fluent);
    };

    TFairShareHierarchicalScheduler(
        TFairShareHierarchicalSchedulerDynamicConfigPtr config,
        NProfiling::TProfiler profiler);

    void EnqueueLog(const TFairShareHierarchicalSchedulerLogPtr<TTag>& log);
    void DequeueLog(const TFairShareLogKey& log);
    void TrimLog();

    void Reconfigure(const TFairShareHierarchicalSchedulerDynamicConfigPtr& config);

    TFairShareHierarchicalSlotQueueBucketNodePtr GetBucket(const std::vector<TTag>& tags) const;

    // The main method of the class. It performs a comparison of two
    // elements based on the constructed tree.
    bool CompareSlots(
        const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& lhs,
        const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& rhs,
        bool isSlot) const;

    // Methods for building a tree in an orchid.
    NYTree::IYPathServicePtr GetOrchidService();
    void BuildOrchid(NYson::IYsonConsumer* consumer);

private:
    TAtomicIntrusivePtr<TFairShareHierarchicalSchedulerDynamicConfig> Config_;
    const NProfiling::TProfiler Profiler_;

    const TFairShareHierarchicalSlotQueueBucketNodePtr RootBucket_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SlotHistoryLock_);
    std::map<TFairShareLogKey, TFairShareHierarchicalSchedulerLogPtr<TTag>> SlotHistory_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, RequestHistoryLock_);
    std::map<TFairShareLogKey, TFairShareHierarchicalSchedulerLogPtr<TTag>> RequestHistory_;

    std::atomic<i64> BucketCount_ = 0;

    NProfiling::TEventTimer EnqueueLogWallTimer_;
    NProfiling::TEventTimer DequeueLogWallTimer_;
    NProfiling::TEventTimer TrimLogWallTimer_;
    NProfiling::TEventTimer CompareWallTimer_;

    void DoDequeue(const TFairShareHierarchicalSchedulerLogPtr<TTag>& log);

    // Updates the bucket counters.
    void UpdateCounters(
        bool isEnqueue,
        bool isSlot,
        i64 size,
        double weight,
        const TFairShareHierarchicalSlotQueueBucketNodePtr& currentParent,
        const TFairShareHierarchicalSlotQueueBucketNodePtr& root,
        const TFairShareHierarchicalSlotQueueBucketNodePtr& current) const;

    // Updates counters to a set of buckets affected by insertion or deletion.
    // Deletes a bucket if it does not contain any elements and no one links to it.
    void UpdateBuckets(
        const std::vector<TFairShareHierarchicalSlotQueueBucketNodePtr>& touchedParents,
        const std::vector<TFairShareHierarchicalSlotQueueBucketNodePtr>& touchedBuckets,
        const std::vector<TTag>& touchedTags,
        const std::vector<double>& weights,
        const TFairShareHierarchicalSlotQueueBucketNodePtr& root,
        i64 size,
        bool isEnqueue,
        bool isSlot);

    // Creates a bucket if it does not exist yet, it is used only inside the EnqueueLog.
    TFairShareHierarchicalScheduler<TTag>::TFairShareHierarchicalSlotQueueBucketNodePtr GetOrCreateChild(
        const TFairShareHierarchicalScheduler<TTag>::TFairShareHierarchicalSlotQueueBucketNodePtr& parent,
        const TTag& tag);
};

////////////////////////////////////////////////////////////////////////////////

// The class is a queue. At any given time, the queue is not literally a priority queue.
// The main task of the queue is to store slots. Namely, to add slots and displace lower priority
// slots when resource consumption is exceeded. To free up slots at the user's request.
// The queue should also provide a slot for working with it in the required fair share order.
template <typename TTag>
class TFairShareHierarchicalSlotQueue
    : public TRefCounted
{
public:
    TFairShareHierarchicalSlotQueue(
        TFairShareHierarchicalSchedulerPtr<TTag> hierarchicalScheduler,
        NProfiling::TProfiler profiler);

    // The slot is added in several main stages. The first stage is the allocation of resources
    // for the slot, the set of resources is arbitrary. If the resources have been allocated,
    // then the insertion takes place without displacement. If it failed, then the minimum item
    // in the queue is currently in use. If it is smaller than the inserted element, the minimum
    // element is deleted and the insertion attempt is repeated. If the inserted element is smaller
    // than all the elements in the queue, the insertion does not occur.
    // In this case, the slots are compared based on slot consumption.
    // In this case, the slots are compared based on slot consumption. This is necessary to ensure
    // that the queue is formed fairly in case of a shortage of resources.
    TErrorOr<TFairShareHierarchicalSlotQueueSlotPtr<TTag>> EnqueueSlot(
        i64 size,
        std::vector<IFairShareHierarchicalSlotQueueResourcePtr> resources,
        std::vector<TFairShareHierarchyLevel<TTag>> levels);

    // The slot is deleted at the user's request. This should usually mean that the
    // request has completed and stopped using resources.
    void DequeueSlot(const TFairShareHierarchicalSlotQueueSlotPtr<TTag>& slot);

    // This method returns the slot that has the highest priority and needs to be processed first.
    // In this case, the slots are compared according to the amount consumed by the requests.
    TFairShareHierarchicalSlotQueueSlotPtr<TTag> PeekSlot(const THashSet<TFairShareSlotId>& slotFilter);

    // This method allows the user of the class to mark the consumption of requests in the slot.
    void AccountSlot(TFairShareHierarchicalSlotQueueSlotPtr<TTag> slot, i64 requestSize);

    // Checks if the queue is empty.
    bool IsEmpty() const;

private:
    const TFairShareHierarchicalSchedulerPtr<TTag> HierarchicalScheduler_;
    const NProfiling::TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, QueueLock_);
    THashMap<TFairShareLogKey, TFairShareHierarchicalSlotQueueSlotPtr<TTag>> Queue_ = {};
    std::atomic<i64> SlotCount_ = 0;
    std::atomic<i64> SlotSize_ = 0;

    NProfiling::TCounter EnqueueSlotSizeCounter_;
    NProfiling::TCounter AccountSlotSizeCounter_;
    NProfiling::TCounter PreemptSlotSizeCounter_;

    NProfiling::TCounter EnqueueSlotCountCounter_;
    NProfiling::TCounter AccountSlotCountCounter_;
    NProfiling::TCounter PreemptSlotCountCounter_;

    NProfiling::TEventTimer ExecTimer_;

    NProfiling::TEventTimer EnqueueSlotWallTimer_;
    NProfiling::TEventTimer DequeueSlotWallTimer_;
    NProfiling::TEventTimer PeekSlotWallTimer_;
};

////////////////////////////////////////////////////////////////////////////////

// Creates a new fair-share hierarchical scheduler.
template <typename TTag>
TFairShareHierarchicalSchedulerPtr<TTag> CreateFairShareHierarchicalScheduler(
    TFairShareHierarchicalSchedulerDynamicConfigPtr config,
    NProfiling::TProfiler profiler = {});

// Creates a new fair-share hierarchical queue.
template <typename TTag>
TFairShareHierarchicalSlotQueuePtr<TTag> CreateFairShareHierarchicalSlotQueue(
    TFairShareHierarchicalSchedulerPtr<TTag> hierarchicalScheduler,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FAIR_SHARE_HIERARCHICAL_QUEUE_INL_H_
#include "fair_share_hierarchical_queue-inl.h"
#undef FAIR_SHARE_HIERARCHICAL_QUEUE_INL_H_
