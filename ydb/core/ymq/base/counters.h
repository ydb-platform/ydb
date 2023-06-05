#pragma once

#include <ydb/core/protos/config.pb.h>

#include <ydb/core/ymq/base/action.h>
#include <ydb/core/ymq/base/cloud_enums.h>
#include <ydb/core/ymq/base/query_id.h>
#include <ydb/core/ymq/base/queue_path.h>

#include <ydb/library/yql/minikql/aligned_page_pool.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/spinlock.h>
#include <util/generic/hash.h>

#include <atomic>
#include <utility>

namespace NKikimr::NSQS {

struct TUserCounters;
struct TFolderCounters;
struct TQueueCounters;
struct THttpCounters;
struct TCloudAuthCounters;

#define INC_COUNTER(countersPack, counter)       \
    if (countersPack) {                          \
        ++*countersPack->counter;                \
    }

#define INC_COUNTER_COUPLE(countersPack, sqsCounter, ymqCounter)       \
    if (countersPack) {                                                \
        ++*countersPack->sqsCounter;                                   \
        ++*countersPack->ymqCounter;                                   \
    }

#define DEC_COUNTER(countersPack, counter)       \
    if (countersPack) {                          \
        --*countersPack->counter;                \
    }

#define ADD_COUNTER(countersPack, counter, count)   \
    if (countersPack) {                             \
        *countersPack->counter += (count);          \
    }

#define ADD_COUNTER_COUPLE(countersPack, sqsCounter, ymqCounter, count)   \
    if (countersPack) {                                                   \
        *countersPack->sqsCounter += (count);                             \
        *countersPack->ymqCounter += (count);                             \
    }

#define SET_COUNTER_COUPLE(countersPack, sqsCounter, ymqCounter, count)   \
    if (countersPack) {                                                   \
        *countersPack->sqsCounter = (count);                              \
        *countersPack->ymqCounter = (count);                              \
    }

#define COLLECT_HISTOGRAM_COUNTER(countersPack, counter, count) \
    if (countersPack) {                                         \
        countersPack->counter->Collect(count);                  \
    }

#define COLLECT_HISTOGRAM_COUNTER_COUPLE(countersCouple, counter, count)    \
    if (countersCouple.SqsCounters) {                                       \
        countersCouple.SqsCounters->counter->Collect(count);                \
    }                                                                       \
    if (countersCouple.YmqCounters) {                                       \
        countersCouple.YmqCounters->counter->Collect(count);                \
    }

// Enums for code readability and for static types guarantee that all parameters passed correctly.
enum class ELaziness {
    OnStart,
    OnDemand,
};

enum class ELifetime {
    Persistent,
    Expiring,
};

enum class EValueType {
    Absolute,
    Derivative,
};

extern const TString TOTAL_COUNTER_LABEL;
//constexpr static std::array<int, 10> RESPONSE_CODES = {200, 400, 403, 404, 500, 503, 504};

template<typename TCounterPtrType>
struct TCountersCouple {
    TCounterPtrType SqsCounters = nullptr;
    TCounterPtrType YmqCounters = nullptr;
    bool Defined() {
        return SqsCounters != nullptr;
    }
};

using TIntrusivePtrCntrCouple = TCountersCouple<TIntrusivePtr<::NMonitoring::TDynamicCounters>>;

TIntrusivePtr<::NMonitoring::TDynamicCounters> GetSqsServiceCounters(
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& countersRoot, const TString& subgroup);
TIntrusivePtr<::NMonitoring::TDynamicCounters> GetYmqPublicCounters(
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& countersRoot);

TIntrusivePtrCntrCouple GetFolderCounters(const TIntrusivePtrCntrCouple& userCounters, const TString& folderId);
void RemoveFolderCounters(const TIntrusivePtrCntrCouple& userCounters, const TString& folderId);
std::pair<TIntrusivePtrCntrCouple, TIntrusivePtrCntrCouple> GetUserAndQueueCounters(
        const TIntrusivePtrCntrCouple& sqsRootCounters, const TQueuePath& queuePath);
TIntrusivePtr<::NMonitoring::TDynamicCounters> GetAggregatedCountersFromSqsCoreCounters(
        const TIntrusivePtrCntrCouple& sqsCoreCounters, const NKikimrConfig::TSqsConfig& cfg);
TIntrusivePtr<::NMonitoring::TDynamicCounters> GetAggregatedCountersFromUserCounters(
        const TIntrusivePtrCntrCouple& sqsCoreCounters, const NKikimrConfig::TSqsConfig& cfg);

extern const TString DEFAULT_COUNTER_NAME;
extern const TString DEFAULT_YMQ_COUNTER_NAME;
//extern const TString ACTION_CNTR_PREFIX;

namespace NDetails {

template <class TTargetCounterType, class TDerived>
struct TLazyCachedCounterBase {
public:
    // Facade for counters aggregation.
    class TAggregatingCounterFacade {
        friend struct TLazyCachedCounterBase;

        TAggregatingCounterFacade(TLazyCachedCounterBase* parent)
            : Parent(parent)
        {
        }

        TAggregatingCounterFacade(const TAggregatingCounterFacade&) = delete;
        TAggregatingCounterFacade(TAggregatingCounterFacade&&) = delete;

    public:
        ~TAggregatingCounterFacade() = default;

#define FOR_EACH_AGGREGATED_COUNTER(action)                             \
        auto* lazyCounter = Parent;                                     \
        while (lazyCounter) {                                           \
            auto& counter = *lazyCounter->EnsureCreated();              \
            action;                                                     \
            lazyCounter = lazyCounter->AggregatedParent;                \
        }

        TAggregatingCounterFacade& operator++() {
            FOR_EACH_AGGREGATED_COUNTER(++counter);
            return *this;
        }

        TAggregatingCounterFacade& operator--() {
            FOR_EACH_AGGREGATED_COUNTER(--counter);
            return *this;
        }

        TAggregatingCounterFacade& operator+=(i64 value) {
            FOR_EACH_AGGREGATED_COUNTER(counter += value);
            return *this;
        }

        TAggregatingCounterFacade& operator=(i64 value) {
            auto* lazyCounter = Parent;
            i64 diff = 0;
            if (lazyCounter) {
                auto& counter = *lazyCounter->EnsureCreated();
                auto prevValue = AtomicSwap(&counter.GetAtomic(), value);
                diff = value - prevValue;
                lazyCounter = lazyCounter->AggregatedParent;
            }

            // Aggregation
            while (lazyCounter) {
                auto& counter = *lazyCounter->EnsureCreated();
                counter += diff;
                lazyCounter = lazyCounter->AggregatedParent;
            }
            return *this;
        }

        void Collect(i64 value) {
            FOR_EACH_AGGREGATED_COUNTER(counter.Collect(value));
        }

        void Add(i64 value) {
            FOR_EACH_AGGREGATED_COUNTER(counter += value);
        }

        void Inc() {
            FOR_EACH_AGGREGATED_COUNTER(++counter);
        }

#undef FOR_EACH_AGGREGATED_COUNTER

        TAggregatingCounterFacade* operator->() {
            return this;
        }

    private:
        TLazyCachedCounterBase* const Parent = nullptr;
    };

    TLazyCachedCounterBase()
        : Counter(nullptr)
    {
    }

    TLazyCachedCounterBase(const TLazyCachedCounterBase& other) {
        TTargetCounterType* counter = other.Counter;
        std::atomic_init(&Counter, counter);
        if (counter) {
            counter->Ref();
        } else {
            Impl = nullptr;
            if (other.Impl) {
                Impl = new TImpl(*other.Impl);
            }
        }
    }

    ~TLazyCachedCounterBase() {
        TTargetCounterType* const counter = Counter;
        if (counter) {
            counter->UnRef();
        }
    }

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters, ELifetime lifetime, const TString& value, ELaziness laziness) {
        Init(rootCounters, lifetime, DEFAULT_COUNTER_NAME, value, laziness);
    }

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters, ELifetime lifetime, const TString& name, const TString& value, ELaziness laziness) {
        Impl = new TImpl();
        Impl->RootCounters = rootCounters;
        Impl->Name = name;
        Impl->Value = value;
        Impl->Lifetime = lifetime;
        if (laziness == ELaziness::OnStart) {
            EnsureCreated();
        }
    }

    bool operator!() const {
        TTargetCounterType* const counter = Counter;
        return counter == nullptr;
    }

    TAggregatingCounterFacade operator*() {
        return this;
    }

    TAggregatingCounterFacade operator->() {
        return this;
    }

    void SetAggregatedParent(TLazyCachedCounterBase* parent) {
        AggregatedParent = parent;
        Y_ASSERT(!HasCycle());
    }

protected:
    TTargetCounterType* EnsureCreated() {
        TTargetCounterType* const counter = Counter;
        if (counter) {
            return counter;
        }
        TIntrusivePtr<TTargetCounterType> newCounter = static_cast<TDerived*>(this)->Create();
        if (!newCounter) {
            newCounter = TDerived::Default();
            Y_ASSERT(newCounter);
        }
        TTargetCounterType* expected = nullptr;
        if (Counter.compare_exchange_strong(expected, newCounter.Get())) {
            newCounter->Ref();
            return newCounter.Get();
        }
        Y_ASSERT(Counter.load() != nullptr);
        return Counter;
    }

    bool HasCycle() const {
        TLazyCachedCounterBase* item = AggregatedParent;
        while (item) {
            if (item == this) {
                return true;
            }
            item = item->AggregatedParent;
        }
        return false;
    }

protected:
    TIntrusivePtr<NMonitoring::TCounterForPtr> CreateImpl(bool derivative) {
        if (Impl && Impl->RootCounters && Impl->Name && Impl->Value) {
            return Impl->Create(derivative);
        } else {
            return nullptr;
        }
    }   

    TIntrusivePtr<NMonitoring::THistogramCounter> CreateHistogramImpl(const NMonitoring::TBucketBounds* buckets) {
        if (Impl && Impl->RootCounters && Impl->Name && Impl->Value && buckets) {
            return Impl->CreateHistogram(buckets);
        } else {
            return nullptr;
        }
    }      

private:
    class TImpl : public TAtomicRefCount<TImpl> {
    public:
        TIntrusivePtr<::NMonitoring::TDynamicCounters> RootCounters;
        TString Name;
        TString Value;
        ELifetime Lifetime = ELifetime::Persistent;

        TIntrusivePtr<NMonitoring::TCounterForPtr> Create(bool derivative) {
            return Lifetime == ELifetime::Expiring ? RootCounters->GetExpiringNamedCounter(Name, Value, derivative) : RootCounters->GetNamedCounter(Name, Value, derivative);
        }

        TIntrusivePtr<NMonitoring::THistogramCounter> CreateHistogram(const NMonitoring::TBucketBounds* buckets) {
            return Lifetime == ELifetime::Expiring ? RootCounters->GetExpiringNamedHistogram(Name, Value, NMonitoring::ExplicitHistogram(*buckets)) : RootCounters->GetNamedHistogram(Name, Value, NMonitoring::ExplicitHistogram(*buckets));
        }  
    };

protected:
    TIntrusivePtr<TImpl> Impl;
    TLazyCachedCounterBase* AggregatedParent = nullptr;
    std::atomic<TTargetCounterType*> Counter;
};

} // namespace NDetails

struct TLazyCachedCounter : public NDetails::TLazyCachedCounterBase<NMonitoring::TCounterForPtr, TLazyCachedCounter> {
    TLazyCachedCounter() = default;
    TLazyCachedCounter(const TLazyCachedCounter&) = default;

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters, ELifetime lifetime, EValueType valueType, const TString& value, ELaziness laziness) {
        Init(rootCounters, lifetime, valueType, DEFAULT_COUNTER_NAME, value, laziness);
    }

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters, ELifetime lifetime, EValueType valueType, const TString& name, const TString& value, ELaziness laziness) {
        TLazyCachedCounterBase::Init(rootCounters, lifetime, name, value, ELaziness::OnDemand);
        ValueType = valueType;
        if (laziness == ELaziness::OnStart) {
            EnsureCreated();
        }
    }

    TIntrusivePtr<NMonitoring::TCounterForPtr> Create() {
        const bool derivative = ValueType == EValueType::Derivative;
        return CreateImpl(derivative);
    }

    static TIntrusivePtr<NMonitoring::TCounterForPtr> Default() {
        static TIntrusivePtr<NMonitoring::TCounterForPtr> counter = new NMonitoring::TCounterForPtr();
        return counter;
    }

private:
    EValueType ValueType = EValueType::Absolute;
};

struct TLazyCachedHistogram : public NDetails::TLazyCachedCounterBase<NMonitoring::THistogramCounter, TLazyCachedHistogram> {
    TLazyCachedHistogram() = default;
    TLazyCachedHistogram(const TLazyCachedHistogram&) = default;

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters, ELifetime lifetime, const NMonitoring::TBucketBounds& buckets, const TString& value, ELaziness laziness) {
        Init(rootCounters, lifetime, buckets, DEFAULT_COUNTER_NAME, value, laziness);
    }

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters, ELifetime lifetime, const NMonitoring::TBucketBounds& buckets, const TString& name, const TString& value, ELaziness laziness) {
        Buckets = &buckets;
        TLazyCachedCounterBase::Init(rootCounters, lifetime, name, value, ELaziness::OnDemand);
        if (laziness == ELaziness::OnStart) {
            EnsureCreated();
        }
    }

    TIntrusivePtr<NMonitoring::THistogramCounter> Create() {
        return CreateHistogramImpl(Buckets);
    }

    static TIntrusivePtr<NMonitoring::THistogramCounter> Default() {
        static TIntrusivePtr<NMonitoring::THistogramCounter> counter = new NMonitoring::THistogramCounter(NMonitoring::ExplicitHistogram({1}));
        return counter;
    }

private:
    const NMonitoring::TBucketBounds* Buckets = nullptr;
};

// Counters for actions (like SendMessage, CreateQueue or GetQueueUrl).
struct TActionCounters {
public:
    TLazyCachedCounter Success;
    TLazyCachedCounter Errors; // User metric for cloud console (SendMessage/ReceiveMessage/DeleteMessage).
    TLazyCachedCounter Infly;

    TLazyCachedHistogram Duration; // Histogram with buckets for durations (== 18 counters). // User metric for cloud console (SendMessage/DeleteMessage).

    // only for receive message action
    TLazyCachedHistogram WorkingDuration; // Special duration except wait time for ReceiveMessage action (== 18 counters). // User metric for cloud console (ReceiveMessage).

public:
    void Init(const NKikimrConfig::TSqsConfig& cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters,
                      EAction action, ELifetime lifetime = ELifetime::Persistent);

    virtual void SetAggregatedParent(TActionCounters* parent);
    virtual ~TActionCounters() {}
};

struct TYmqActionCounters : public TActionCounters {
    void Init(const NKikimrConfig::TSqsConfig& cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters,
              EAction action, const TString& labelName, const TString& namePrefix,
              ELifetime lifetime = ELifetime::Persistent);
public:
    virtual ~TYmqActionCounters() {}
    void SetAggregatedParent(TActionCounters*) override {
        return;
    }

private:
    ::NMonitoring::TDynamicCounterPtr SubGroup;
};

// Counters for typed queries (WRITE_MESSAGE_ID, PURGE_QUEUE_ID and etc).
struct TQueryTypeCounters {
    TLazyCachedCounter TransactionsCount;
    TLazyCachedCounter TransactionsFailed;
    TLazyCachedHistogram TransactionDuration; // Histogram with buckets for durations (== 18 counters).

    void SetAggregatedParent(TQueryTypeCounters* parent);
};

// Counters for transactions processing.
// These counters are present in queue counters and in user counters.
struct TTransactionCounters : public TAtomicRefCount<TTransactionCounters> {
    // Query types are declared in ydb/core/ymq/base/query_id.h.
    TQueryTypeCounters QueryTypeCounters[EQueryId::QUERY_VECTOR_SIZE];

    std::shared_ptr<TAlignedPagePoolCounters> AllocPoolCounters; // counters for kikimr core.

    TLazyCachedCounter CompileQueryCount; // Compiles count.
    TLazyCachedCounter TransactionsCount; // Transactions processed.
    TLazyCachedCounter TransactionsInfly; // Current transactions count inflight.
    TLazyCachedCounter TransactionRetryTimeouts; // Count of times when we got temporary error from transaction, but can't retry, because there is no time left.
    TLazyCachedCounter TransactionRetries; // Transactions retries due to temporary errors.
    TLazyCachedCounter TransactionsFailed; // Transactions that failed.

    TIntrusivePtr<TTransactionCounters> AggregatedParent; // Less detailed transaction counters aggregated by queue/user.

    void SetAggregatedParent(const TIntrusivePtr<TTransactionCounters>& parent);

public:
    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters,
        std::shared_ptr<TAlignedPagePoolCounters> poolCounters, bool forQueue);
};

// Amazon status codes.
struct TAPIStatusesCounters {
public:
    void AddError(const TString& errorCode, size_t count = 1);
    void AddOk(size_t count = 1);

    void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& root);

    void SetAggregatedParent(TAPIStatusesCounters* parent);

private:
    THashMap<TString, TLazyCachedCounter> ErrorToCounter; // Map with different status codes. See ydb/core/ymq/base/error.cpp.
    TLazyCachedCounter OkCounter; // Special status for successful requests.
    TLazyCachedCounter UnknownCounter; // Special status for statuses that are not in map.
};

// User counters in SQS core subsystem.
struct TUserCounters : public TAtomicRefCount<TUserCounters> {
    // Action types are declared in ydb/core/ymq/base/action.h.
    TActionCounters SqsActionCounters[EAction::ActionsArraySize]; // 11 actions. See IsActionForUser() function in ydb/core/ymq/base/action.cpp.
    TYmqActionCounters YmqActionCounters[EAction::ActionsArraySize]; // see IsActionForUserYMQ() function.

    TLazyCachedCounter RequestTimeouts; // Requests that weren't processed in 10 minutes. They are almost sure hanged.

    TLazyCachedCounter UnauthenticatedAccess; // For users in exception list and total (aggregated) - unauthenticated accesses count.

    struct TDetailedCounters {
        TIntrusivePtr<TTransactionCounters> TransactionCounters;

        TAPIStatusesCounters APIStatuses;

        TLazyCachedHistogram GetConfiguration_Duration; // Part of request initialization. Histogram with buckets for durations (== 18 counters).
        TLazyCachedHistogram GetQuota_Duration; // Histogram with buckets for quota durations (== 16 counters).

        TLazyCachedCounter CreateAccountOnTheFly_Success; // Account created on the fly (Yandex Cloud mode).
        TLazyCachedCounter CreateAccountOnTheFly_Errors; // Account that were failed to create on the fly (Yandex Cloud mode).

        void Init(const TIntrusivePtrCntrCouple& userCounters,
                  const std::shared_ptr<TAlignedPagePoolCounters>& allocPoolCounters,
                  const NKikimrConfig::TSqsConfig& cfg);
        void SetAggregatedParent(TDetailedCounters* parent);
    };

    TDetailedCounters* GetDetailedCounters() {
        if (NeedToShowDetailedCounters()) {
            return &DetailedCounters;
        } else if (AggregatedParent) {
            return AggregatedParent->GetDetailedCounters();
        } else {
            return nullptr;
        }
    }

    // Raw counters interface
    // Don't use counters by name!
    TIntrusivePtrCntrCouple SqsCoreCounters; // Sqs core subsystem
    TIntrusivePtrCntrCouple UserCounters; // User tree in core subsystem

    TUserCounters(
            const NKikimrConfig::TSqsConfig& cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& sqsCoreCounters,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& ymqRootCounters,
            const std::shared_ptr<TAlignedPagePoolCounters>& allocPoolCounters, const TString& userName,
            const TIntrusivePtr<TUserCounters>& aggregatedParent,
            bool isAggregatedCounters = false
    )
        : SqsCoreCounters{sqsCoreCounters, ymqRootCounters}
        , Cfg(&cfg)
        , UserName(userName)
        , AggregatedParent(aggregatedParent)
        , IsAggregatedCounters(isAggregatedCounters)
    {
        InitCounters(userName, allocPoolCounters);
    }

    TIntrusivePtr<TQueueCounters> CreateQueueCounters(const TString& queueName, const TString& folderId, bool insertCounters);
    TIntrusivePtr<TFolderCounters> CreateFolderCounters(const TString& folderId, bool insertCounters);

    void RemoveCounters();

    void ShowDetailedCounters(TInstant deadline) {
        Y_ASSERT(ShowDetailedCountersDeadline);
        *ShowDetailedCountersDeadline = deadline.GetValue();
    }

    bool NeedToShowDetailedCounters() const {
        Y_ASSERT(ShowDetailedCountersDeadline);
        return TInstant::Now().GetValue() < *ShowDetailedCountersDeadline;
    }

    void DisableCounters(bool disable);

    void ExportTransactionCounters(bool needExport) {
        NeedExportTransactionCounters = needExport;
    }

    TIntrusivePtr<TTransactionCounters> GetTransactionCounters() const {
        if (NeedExportTransactionCounters || NeedToShowDetailedCounters()) {
            return DetailedCounters.TransactionCounters;
        } else if (AggregatedParent) {
            return AggregatedParent->GetTransactionCounters();
        } else {
            return nullptr;
        }
    }

private:
    void InitCounters(const TString& userName, const std::shared_ptr<TAlignedPagePoolCounters>& allocPoolCounters);
    TIntrusivePtr<TQueueCounters> CreateQueueCountersImpl(const TString& queueName, const TString& folderId, bool insertCounters, bool aggregated = false);

    friend struct TQueueCounters;

private:
    const NKikimrConfig::TSqsConfig* Cfg = nullptr;
    const TString UserName;
    std::shared_ptr<std::atomic<ui64>> ShowDetailedCountersDeadline = std::make_shared<std::atomic<ui64>>(0ul); // TInstant value
    std::atomic<bool> NeedExportTransactionCounters = false;
    TDetailedCounters DetailedCounters;
    TIntrusivePtr<TUserCounters> AggregatedParent;
    TIntrusivePtr<TQueueCounters> AggregatedQueueCounters;
    bool IsAggregatedCounters;
};

struct TFolderCounters : public TAtomicRefCount<TFolderCounters> {
    TLazyCachedCounter total_count; // Total queues in folder.

    TFolderCounters(const TUserCounters* userCounters, const TString& folderId, bool insertCounters);

    void InitCounters();
    void InsertCounters();

private:
    TIntrusivePtrCntrCouple UserCounters; // User tree in core subsystem
    TIntrusivePtrCntrCouple FolderCounters; // Folder tree in core subsystem

    const TString FolderId;
    bool Inited = false;

};

// Queue counters in SQS core subsystem.
struct TQueueCounters : public TAtomicRefCount<TQueueCounters> {
    // Action types are declared in ydb/core/ymq/base/action.h.
    TActionCounters SqsActionCounters[EAction::ActionsArraySize]; // 12 actions. See IsActionForQueue() function in ydb/core/ymq/base/action.cpp.
    TYmqActionCounters YmqActionCounters[EAction::ActionsArraySize]; // See IsActionForQueueYMQ() function in ydb/core/ymq/sqs/base/action.cpp.

    TLazyCachedCounter RequestTimeouts; // Requests that weren't processed in 10 minutes. They are almost sure hanged.
    TLazyCachedCounter request_timeouts_count_per_second; // Requests that weren't processed in 10 minutes. They are almost sure hanged.
    TLazyCachedCounter RequestsThrottled; // Request that ended with ThrottlingException
    TLazyCachedCounter QueueMasterStartProblems; // TODO: remove after migration
    TLazyCachedCounter QueueLeaderStartProblems; // Critical problems during leader start.

    TLazyCachedCounter MessagesPurged;
    TLazyCachedCounter purged_count_per_second;

    TLazyCachedHistogram MessageReceiveAttempts; // User attempts for receive messages. Histogram with buckets for receive attempts (== 4 counters). // User metric for cloud console.
    TLazyCachedHistogram receive_attempts_count_rate;
    TLazyCachedHistogram ClientMessageProcessing_Duration; // Time between receive and delete for deleted message. Histogram with buckets for client processing (== 21 counters). // User metric for cloud console.
    TLazyCachedHistogram client_processing_duration_milliseconds;
    TLazyCachedHistogram MessageReside_Duration; // Time between send and receive for received messages. Histogram with buckets for client processing (== 21 counters). // User metric for cloud console.
    TLazyCachedHistogram reside_duration_milliseconds;

    TLazyCachedCounter DeleteMessage_Count; // Messages count that were deleted. // User metric for cloud console.
    TLazyCachedCounter deleted_count_per_second;

    TLazyCachedCounter ReceiveMessage_EmptyCount; // Receive message requests count that returned empty results.
    TLazyCachedCounter empty_receive_attempts_count_per_second;
    TLazyCachedCounter ReceiveMessage_Count; // Messages count that were received. // User metric for cloud console.
    TLazyCachedCounter received_count_per_second;
    TLazyCachedCounter ReceiveMessage_BytesRead; // Bytes of message bodies that were received. // User metric for cloud console.
    TLazyCachedCounter received_bytes_per_second;

    TLazyCachedCounter MessagesMovedToDLQ; // Count of messages that were moved to DLQ.

    TLazyCachedCounter SendMessage_DeduplicationCount; // Count of messages that were deduplicated (for fifo only).
    TLazyCachedCounter deduplicated_count_per_second;
    TLazyCachedCounter SendMessage_Count; // Count of messages that were sent. // User metric for cloud console.
    TLazyCachedCounter sent_count_per_second;
    TLazyCachedCounter SendMessage_BytesWritten; // Bytes of message bodies that were sent.
    TLazyCachedCounter sent_bytes_per_second;

    TLazyCachedCounter MessagesCount; // Messages count in queue. // User metric for cloud console.
    TLazyCachedCounter stored_count;
    TLazyCachedCounter InflyMessagesCount; // Messages count in queue that are inflight. // User metric for cloud console.
    TLazyCachedCounter inflight_count;
    TLazyCachedCounter OldestMessageAgeSeconds; // Age of the oldest message in queue. // User metric for cloud console.
    TLazyCachedCounter oldest_age_milliseconds;

    struct TDetailedCounters {
        TIntrusivePtr<TTransactionCounters> TransactionCounters;

        TLazyCachedHistogram GetConfiguration_Duration; // Part of request initialization. Histogram with buckets for durations (== 18 counters).

        TLazyCachedCounter ReceiveMessage_KeysInvalidated; // Count of attempts to receive a message, but race occured.

        TLazyCachedHistogram ReceiveMessageImmediate_Duration; // Time for receive message request that was processed with only one attempt (without wait or try many shards). Histogram with buckets for durations (== 18 counters).

        void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& queueCounters,
            const std::shared_ptr<TAlignedPagePoolCounters>& allocPoolCounters, bool forLeaderNode);
        void SetAggregatedParent(TDetailedCounters* parent);
    };

    TDetailedCounters* GetDetailedCounters() {
        if (NeedToShowDetailedCounters()) {
            return &DetailedCounters;
        } else if (AggregatedParent) {
            return AggregatedParent->GetDetailedCounters();
        } else {
            return nullptr;
        }
    }

    // Raw counters interface.RootCounters
    // Don't use counters by name!
    TIntrusivePtrCntrCouple RootCounters; // Sqs core subsystem.
    TIntrusivePtrCntrCouple UserCounters; // User tree in core subsystem
    TIntrusivePtrCntrCouple FolderCounters; // Folder subtree in user tree (only for Yandex Cloud).
    TIntrusivePtrCntrCouple QueueCounters; // Queue subtree in user (or folder) tree.

    // Creates counters for not leader node.
    TQueueCounters(const NKikimrConfig::TSqsConfig& cfg,
                   const TIntrusivePtrCntrCouple& sqsCoreCounters,
                   const TUserCounters* userCounters,
                   const TString& queueName,
                   const TString& folderId,
                   bool insertCounters,
                   bool aggregated);

    TQueueCounters(const TQueueCounters&) = default;

    void InsertCounters();
    void RemoveCounters();

    TIntrusivePtr<TQueueCounters> GetCountersForLeaderNode();
    TIntrusivePtr<TQueueCounters> GetCountersForNotLeaderNode();

    void ShowDetailedCounters(TInstant deadline) {
        Y_ASSERT(ShowDetailedCountersDeadline);
        *ShowDetailedCountersDeadline = deadline.GetValue();
    }

    bool NeedToShowDetailedCounters() const {
        Y_ASSERT(ShowDetailedCountersDeadline);
        Y_ASSERT(UserShowDetailedCountersDeadline);
        const TInstant now = TInstant::Now();
        return now.GetValue() < *ShowDetailedCountersDeadline || now.GetValue() < *UserShowDetailedCountersDeadline;
    }

    TIntrusivePtr<TTransactionCounters> GetTransactionCounters() const {
        if (NeedToShowDetailedCounters()) {
            return DetailedCounters.TransactionCounters;
        } else if (AggregatedParent) {
            return AggregatedParent->GetTransactionCounters();
        } else {
            return nullptr;
        }
    }

    void SetAggregatedParent(const TIntrusivePtr<TQueueCounters>& parent);

private:
    void InitCounters(bool forLeaderNode = false);

private:
    const NKikimrConfig::TSqsConfig* Cfg = nullptr;
    const TString QueueName;
    bool AggregatedCounters = false;
    TIntrusivePtr<TQueueCounters> NotLeaderNodeCounters;
    std::shared_ptr<std::atomic<ui64>> ShowDetailedCountersDeadline = std::make_shared<std::atomic<ui64>>(0ul); // TInstant value
    std::shared_ptr<std::atomic<ui64>> UserShowDetailedCountersDeadline;
    TDetailedCounters DetailedCounters;
    std::shared_ptr<TAlignedPagePoolCounters> AllocPoolCounters; // Transaction counters for kikimr core.
    TIntrusivePtr<TQueueCounters> AggregatedParent;
};

//
// Http subsystem counters
//

// Http counters for actions (like SendMessage, CreateQueue or GetQueueUrl).
struct THttpActionCounters {
    TLazyCachedCounter Requests; // Requests count of given type.

    void Init(const NKikimrConfig::TSqsConfig& cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters, EAction action);
    void SetAggregatedParent(THttpActionCounters* parent);

private:
    const NKikimrConfig::TSqsConfig* Cfg = nullptr;
};

struct THttpUserCounters : public TAtomicRefCount<THttpUserCounters> {
    // Action types are declared in ydb/core/ymq/base/action.h.
    THttpActionCounters ActionCounters[EAction::ActionsArraySize]; // 23 actions.

    TLazyCachedCounter RequestExceptions; // Exceptions count during http request processing.

    // Raw counters interface
    // Don't use counters by name!
    TIntrusivePtr<::NMonitoring::TDynamicCounters> SqsHttpCounters; // Sqs http subsystem
    TIntrusivePtr<::NMonitoring::TDynamicCounters> UserCounters; // User tree in core subsystem

    THttpUserCounters(const NKikimrConfig::TSqsConfig& cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& sqsHttpCounters, const TString& userName)
        : SqsHttpCounters(sqsHttpCounters)
        , Cfg(&cfg)
    {
        InitCounters(userName);
    }

    void SetAggregatedParent(const TIntrusivePtr<THttpUserCounters>& parent);

private:
    void InitCounters(const TString& userName);

private:
    const NKikimrConfig::TSqsConfig* Cfg = nullptr;
    TIntrusivePtr<THttpUserCounters> AggregatedParent;
};

struct THttpCounters : public TAtomicRefCount<THttpCounters> {
    TLazyCachedCounter RequestExceptions; // Exceptions count during http request processing.
    TLazyCachedCounter InternalExceptions; // Internal exceptions count.
    TLazyCachedCounter ConnectionsCount; // Connections count that kikimr http server sees.

    // Raw counters interface
    // Don't use counters by name!
    TIntrusivePtr<::NMonitoring::TDynamicCounters> SqsHttpCounters; // Sqs http subsystem

    THttpCounters(const NKikimrConfig::TSqsConfig& cfg, TIntrusivePtr<::NMonitoring::TDynamicCounters> sqsHttpCounters)
        : SqsHttpCounters(std::move(sqsHttpCounters))
        , Cfg(&cfg)
    {
        InitCounters();
    }

    TIntrusivePtr<THttpUserCounters> GetUserCounters(const TString& userName);

private:
    void InitCounters();
    TIntrusivePtr<THttpUserCounters> GetUserCountersImpl(const TString& userName, const TIntrusivePtr<THttpUserCounters>& aggregatedUserCounters);

private:
    const NKikimrConfig::TSqsConfig* Cfg = nullptr;
    TAdaptiveLock Lock;
    THashMap<TString, TIntrusivePtr<THttpUserCounters>> UserCounters;
    TIntrusivePtr<THttpUserCounters> AggregatedUserCounters;
};

// Cloud specific counters.
struct TCloudAuthCounters {
    // Durations for different security actions types.
    TLazyCachedHistogram AuthenticateDuration; // Histogram with buckets for durations (== 18 counters).
    TLazyCachedHistogram AuthorizeDuration; // Histogram with buckets for durations (== 18 counters).
    TLazyCachedHistogram GetFolderIdDuration; // Histogram with buckets for durations (== 18 counters).

    explicit TCloudAuthCounters(const NKikimrConfig::TSqsConfig& cfg, TIntrusivePtr<::NMonitoring::TDynamicCounters> cloudAuthCountersRoot)
        : Cfg(&cfg)
    {
        InitCounters(std::move(cloudAuthCountersRoot));
    }

    void IncCounter(const NCloudAuth::EActionType actionType, const NCloudAuth::ECredentialType credentialType, int grpcStatus);
    void IncAuthorizeCounter(const NCloudAuth::ECredentialType credentialType, bool error);

    static constexpr int GRPC_STATUSES_COUNT = 18;

private:
    void InitCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> cloudAuthCounters);

private:
    const NKikimrConfig::TSqsConfig* Cfg = nullptr;
    TLazyCachedCounter CloudAuthCounters[NCloudAuth::EActionType::ActionTypesCount] // 3 types.
                                        [NCloudAuth::ECredentialType::CredentialTypesCount] // 2 types.
                                        [GRPC_STATUSES_COUNT]; // 18 types.
    TLazyCachedCounter AuthorizeSuccess[NCloudAuth::ECredentialType::CredentialTypesCount];
    TLazyCachedCounter AuthorizeError[NCloudAuth::ECredentialType::CredentialTypesCount];
};

// Metering counters in SQS core subsystem.
struct TMeteringCounters : public TAtomicRefCount<TMeteringCounters> {
    THashMap<TString, TLazyCachedCounter> ClassifierRequestsResults;
    THashMap<TString, TLazyCachedCounter> IdleClassifierRequestsResults;

    TMeteringCounters(const NKikimrConfig::TSqsConfig& config, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& sqsMeteringCounters, const TVector<TString>& classifierLabels)
        : SqsMeteringCounters(sqsMeteringCounters)
        , Config(config)
    {
        InitCounters(classifierLabels);
    }

private:
    void InitCounters(const TVector<TString>& classifierLabels);

private:
    TIntrusivePtr<::NMonitoring::TDynamicCounters> SqsMeteringCounters;
    const NKikimrConfig::TSqsConfig& Config;
};

// Common service monitoring counters
struct TMonitoringCounters : public TAtomicRefCount<TMonitoringCounters> {
    TLazyCachedCounter CleanupRemovedQueuesLagSec;
    TLazyCachedCounter CleanupRemovedQueuesLagCount;
    
    TLazyCachedCounter CleanupRemovedQueuesDone;
    TLazyCachedCounter CleanupRemovedQueuesRows;
    TLazyCachedCounter CleanupRemovedQueuesErrors;

    TLazyCachedCounter LocalLeaderStartInflight;
    TLazyCachedCounter LocalLeaderStartQueue;
    TLazyCachedHistogram LocalLeaderStartAwaitMs;

    TMonitoringCounters(const NKikimrConfig::TSqsConfig& config, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& monitoringCounters)
        : MonitoringCounters(monitoringCounters)
        , Config(config)
    {
        InitCounters();
    }

private:
    void InitCounters();

private:
    TIntrusivePtr<::NMonitoring::TDynamicCounters> MonitoringCounters;
    const NKikimrConfig::TSqsConfig& Config;
};

} // namespace NKikimr::NSQS
