#include "counters.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/http_proxy/error/error.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/system/defaults.h>

#include <vector>
#include <type_traits>

namespace NKikimr::NSQS {

extern const TString DEFAULT_COUNTER_NAME = "sensor";
extern const TString DEFAULT_YMQ_COUNTER_NAME = "name";
extern const TString ACTION_CNTR_PREFIX = "api.http.";
extern const TString QUEUE_CNTR_PREFIX = "queue.messages.";
extern const TString METHOD_LABLE = "method";
static const TString USER_LABEL = "user";
static const TString CLOUD_LABEL = "cloud";
static const TString FOLDER_LABEL = "folder";
static const TString QUEUE_LABEL = "queue";
extern const TString TOTAL_COUNTER_LABEL = "total";
static const TString QUERY_TYPE = "query_type";
static const TString STATUS_CODE = "status_code";

static const NMonitoring::TBucketBounds FastActionsDurationBucketsMs = {
    5,
    10,
    25,
    50,
    75,
    100,
    150,
    300,
    500,
    1000,
    5000,
};

static const NMonitoring::TBucketBounds YmqFastActionsDurationBucketsMs = {
    5,
    10,
    25,
    50,
    75,
    100,
    150,
    300,
    500,
    1000,
    5000,
};


static const NMonitoring::TBucketBounds SlowActionsDurationBucketsMs = {
    100,
    150,
    300,
    500,
    1000,
    5000,
};

static const NMonitoring::TBucketBounds YmqSlowActionsDurationBucketsMs = {
    100,
    150,
    300,
    500,
    1000,
    5000,
    10'000,
    20'000,
    50'000
};

static const NMonitoring::TBucketBounds DurationBucketsMs = {
    5,
    10,
    25,
    50,
    75,
    100,
    125,
    150,
    250,
    500,
    750,
    1'000,
    2'500,
    5'000,
    10'000,
    30'000,
    50'000,
};

static const NMonitoring::TBucketBounds ClientDurationBucketsMs = {
    100,
    250,
    500,
    750,
    1'000,
    2'500,
    5'000,
    7'500,
    10'000,
    25'000,
    50'000,
    100'000,
    250'000,
    500'000,
    1'000'000,
    2'500'000,
    5'000'000,
    10'000'000,
    25'000'000,
    50'000'000,
};
static const NMonitoring::TBucketBounds YmqClientDurationBucketsMs = {
    100,
    200,
    500,
    1'000,
    2'000,
    5'000,
    10'000,
    20'000,
    60'000,
    120'000,
    300'000,
    600'000,
    3'600'000,
    7'200'000,
};

static const NMonitoring::TBucketBounds GetQuotaDurationBucketsMs = {
    1,
    2,
    5,
    10,
    25,
    50,
    75,
    100,
    125,
    150,
    250,
    500,
    750,
    1'000,
    2'500,
};

static const NMonitoring::TBucketBounds MessageReceiveAttemptsBuckets = {
    1,
    2,
    5,
};

template <class T>
struct TSizeOfMemberType;

template <class T, class TPack>
struct TSizeOfMemberType<T TPack::*> {
    static constexpr size_t Value = sizeof(T);
};

template <class T>
constexpr size_t SizeOfMember = TSizeOfMemberType<T>::Value;

template <class... T>
struct TMemberCountersDescriptor {
    TMemberCountersDescriptor(T... memberCounterPointers)
        : MemberCounterPointers(memberCounterPointers...)
    {
    }

    template <class TCountersPack, size_t i = 0>
    void SetAggregatedParent(TCountersPack* pack, TCountersPack* parent) const {
        auto pointerToMemberCounter = std::get<i>(MemberCounterPointers);
        (pack->*pointerToMemberCounter).SetAggregatedParent(parent ? &(parent->*pointerToMemberCounter) : nullptr);
        if constexpr (i + 1 < MembersCount) {
            SetAggregatedParent<TCountersPack, i + 1>(pack, parent);
        }
    }

    template <size_t i = 0>
    constexpr size_t SizeOfCounters() const {
        constexpr size_t currentElemSize = SizeOfMember<std::tuple_element_t<i, decltype(MemberCounterPointers)>>;
        if constexpr (i + 1 < MembersCount) {
            return currentElemSize + SizeOfCounters<i + 1>();
        } else {
            return currentElemSize;
        }
    }

    std::tuple<T...> MemberCounterPointers;
    static constexpr size_t MembersCount = sizeof...(T);
};

static constexpr size_t AbsDiff(size_t a, size_t b) {
    return a < b ? b - a : a - b;
}

static constexpr bool AbsDiffLessThanCounter(size_t a, size_t b) {
    return AbsDiff(a, b) < sizeof(TLazyCachedCounter);
}

static const auto ActionCountersDescriptor =
    TMemberCountersDescriptor(&TActionCounters::Success,
                              &TActionCounters::Errors,
                              &TActionCounters::Infly,
                              &TActionCounters::Duration,
                              &TActionCounters::WorkingDuration);

static_assert(AbsDiffLessThanCounter(ActionCountersDescriptor.SizeOfCounters(), sizeof(TActionCounters)));

static const auto QueryTypeCountersDescriptor =
    TMemberCountersDescriptor(&TQueryTypeCounters::TransactionsCount,
                              &TQueryTypeCounters::TransactionsFailed,
                              &TQueryTypeCounters::TransactionDuration);

static_assert(AbsDiffLessThanCounter(QueryTypeCountersDescriptor.SizeOfCounters(), sizeof(TQueryTypeCounters)));

static const auto TransactionCountersDescriptor =
    TMemberCountersDescriptor(&TTransactionCounters::CompileQueryCount,
                              &TTransactionCounters::TransactionsCount,
                              &TTransactionCounters::TransactionsInfly,
                              &TTransactionCounters::TransactionRetryTimeouts,
                              &TTransactionCounters::TransactionRetries,
                              &TTransactionCounters::TransactionsFailed);

static_assert(AbsDiffLessThanCounter(TransactionCountersDescriptor.SizeOfCounters() +
                                     SizeOfMember<decltype(&TTransactionCounters::AllocPoolCounters)> +
                                     SizeOfMember<decltype(&TTransactionCounters::AggregatedParent)> +
                                     SizeOfMember<decltype(&TTransactionCounters::QueryTypeCounters)>,
                                     sizeof(TTransactionCounters)));

static const auto UserDetailedCountersDescriptor =
    TMemberCountersDescriptor(&TUserCounters::TDetailedCounters::APIStatuses,
                              &TUserCounters::TDetailedCounters::GetConfiguration_Duration,
                              &TUserCounters::TDetailedCounters::GetQuota_Duration,
                              &TUserCounters::TDetailedCounters::CreateAccountOnTheFly_Success,
                              &TUserCounters::TDetailedCounters::CreateAccountOnTheFly_Errors);

static_assert(AbsDiffLessThanCounter(UserDetailedCountersDescriptor.SizeOfCounters() +
                                     SizeOfMember<decltype(&TUserCounters::TDetailedCounters::TransactionCounters)>,
                                     sizeof(TUserCounters::TDetailedCounters)));

static const auto UserCountersDescriptor =
    TMemberCountersDescriptor(&TUserCounters::RequestTimeouts,
                              &TUserCounters::UnauthenticatedAccess);

static const auto QueueDetailedCountersDescriptor =
    TMemberCountersDescriptor(&TQueueCounters::TDetailedCounters::GetConfiguration_Duration,
                              &TQueueCounters::TDetailedCounters::ReceiveMessage_KeysInvalidated,
                              &TQueueCounters::TDetailedCounters::ReceiveMessageImmediate_Duration);

static_assert(AbsDiffLessThanCounter(QueueDetailedCountersDescriptor.SizeOfCounters() +
                                     SizeOfMember<decltype(&TQueueCounters::TDetailedCounters::TransactionCounters)>,
                                     sizeof(TQueueCounters::TDetailedCounters)));

static const auto QueueCountersDescriptor =
    TMemberCountersDescriptor(&TQueueCounters::RequestTimeouts,
                              &TQueueCounters::RequestsThrottled,
                              &TQueueCounters::QueueMasterStartProblems,
                              &TQueueCounters::QueueLeaderStartProblems,
                              &TQueueCounters::MessagesPurged,
                              &TQueueCounters::MessageReceiveAttempts,
                              &TQueueCounters::ClientMessageProcessing_Duration,
                              &TQueueCounters::MessageReside_Duration,
                              &TQueueCounters::DeleteMessage_Count,
                              &TQueueCounters::ReceiveMessage_EmptyCount,
                              &TQueueCounters::ReceiveMessage_Count,
                              &TQueueCounters::ReceiveMessage_BytesRead,
                              &TQueueCounters::MessagesMovedToDLQ,
                              &TQueueCounters::SendMessage_DeduplicationCount,
                              &TQueueCounters::SendMessage_Count,
                              &TQueueCounters::SendMessage_BytesWritten,
                              //&TQueueCounters::OldestMessageAgeSeconds, // not aggregated
                              &TQueueCounters::MessagesCount,
                              &TQueueCounters::InflyMessagesCount);

static const auto HttpActionCountersDescriptor =
    TMemberCountersDescriptor(&THttpActionCounters::Requests);

static_assert(AbsDiffLessThanCounter(HttpActionCountersDescriptor.SizeOfCounters(), sizeof(THttpActionCounters)));

static const auto HttpUserCountersDescriptor =
    TMemberCountersDescriptor(&THttpUserCounters::RequestExceptions);

static_assert(AbsDiffLessThanCounter(HttpUserCountersDescriptor.SizeOfCounters() +
                                     SizeOfMember<decltype(&THttpUserCounters::ActionCounters)> +
                                     SizeOfMember<decltype(&THttpUserCounters::SqsHttpCounters)> +
                                     SizeOfMember<decltype(&THttpUserCounters::UserCounters)> +
                                     sizeof(NKikimrConfig::TSqsConfig*) +
                                     sizeof(TIntrusivePtr<THttpUserCounters>), sizeof(THttpUserCounters)));

TIntrusivePtr<::NMonitoring::TDynamicCounters> GetSqsServiceCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& countersRoot, const TString& subgroup) {
    return GetServiceCounters(countersRoot, "sqs")->GetSubgroup("subsystem", subgroup);
}
TIntrusivePtr<::NMonitoring::TDynamicCounters> GetYmqPublicCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& countersRoot) {
    // Remove subgroup and don't have subsystem (is this correct - ?)
    return GetServiceCounters(countersRoot, "ymq_public");
}

static TIntrusivePtrCntrCouple GetUserCounters(
        const TIntrusivePtrCntrCouple & sqsCoreCounters, const TString& userName
) {
    return {
        sqsCoreCounters.SqsCounters->GetSubgroup(USER_LABEL, userName),
        sqsCoreCounters.YmqCounters ? sqsCoreCounters.YmqCounters->GetSubgroup(CLOUD_LABEL, userName) : nullptr
    };
}

TIntrusivePtrCntrCouple GetFolderCounters(const TIntrusivePtrCntrCouple& userCounters, const TString& folderId) {
    return {
        userCounters.SqsCounters->GetSubgroup(FOLDER_LABEL, folderId),
        userCounters.YmqCounters ? userCounters.YmqCounters->GetSubgroup(FOLDER_LABEL, folderId) : nullptr
    };
}

void RemoveFolderCounters(const TIntrusivePtrCntrCouple& userCounters, const TString& folderId) {
    if (userCounters.YmqCounters) {
        userCounters.YmqCounters->RemoveSubgroup(FOLDER_LABEL, folderId);
    }
}

static TIntrusivePtrCntrCouple GetQueueCounters(const TIntrusivePtrCntrCouple & userOrFolderCounters, const TString& queueName) {
    return {
        userOrFolderCounters.SqsCounters->GetSubgroup(QUEUE_LABEL, queueName),
        userOrFolderCounters.YmqCounters ? userOrFolderCounters.YmqCounters->GetSubgroup(QUEUE_LABEL, queueName) : nullptr
    };
}

std::pair<TIntrusivePtrCntrCouple, TIntrusivePtrCntrCouple> GetUserAndQueueCounters(
        const TIntrusivePtrCntrCouple& sqsCounters, const TQueuePath& queuePath
) {
    TIntrusivePtrCntrCouple userCounters;
    TIntrusivePtrCntrCouple queueCounters;
    if (queuePath.UserName && sqsCounters.SqsCounters) {
        userCounters = GetUserCounters(sqsCounters, queuePath.UserName);
        if (queuePath.QueueName) {
            queueCounters = GetQueueCounters(userCounters, queuePath.QueueName);
        }
    }
    return { std::move(userCounters), std::move(queueCounters) };
}

TIntrusivePtr<::NMonitoring::TDynamicCounters> GetAggregatedCountersFromSqsCoreCounters(
        const TIntrusivePtrCntrCouple& rootCounters, const NKikimrConfig::TSqsConfig& cfg
) {
    return GetAggregatedCountersFromUserCounters(GetUserCounters(rootCounters, TOTAL_COUNTER_LABEL), cfg);
}

TIntrusivePtr<::NMonitoring::TDynamicCounters> GetAggregatedCountersFromUserCounters(
        const TIntrusivePtrCntrCouple& userCounters, const NKikimrConfig::TSqsConfig& cfg
) {
    if (cfg.GetYandexCloudMode()) {
        return GetQueueCounters(GetFolderCounters(userCounters, TOTAL_COUNTER_LABEL), TOTAL_COUNTER_LABEL).SqsCounters;
    } else {
        return GetQueueCounters(userCounters, TOTAL_COUNTER_LABEL).SqsCounters;
    }
}

ELaziness Lazy(const NKikimrConfig::TSqsConfig& cfg) {
    return cfg.GetCreateLazyCounters() ? ELaziness::OnDemand : ELaziness::OnStart;
}

#define INIT_COUNTER_WITH_NAME(rootCounters, variable, name, expiring, valueType, lazy) \
    variable.Init(rootCounters, expiring, valueType, name, lazy)
#define INIT_COUNTER_WITH_NAME_AND_LABEL(rootCounters, variable, labelName, name, expiring, valueType, lazy) \
    variable.Init(rootCounters, expiring, valueType, labelName, name, lazy)

#define INIT_COUNTERS_COUPLE_WITH_NAMES(rootCounters, sqsCounter, ymqCounter, sqsName, ymqName, expiring, valueType, lazy, aggr)     \
    sqsCounter.Init(rootCounters.SqsCounters, expiring, valueType, sqsName, lazy);                                                   \
    if (rootCounters.YmqCounters && !aggr) {                                                                                         \
        ymqCounter.Init(                                                                                                             \
            rootCounters.YmqCounters, ELifetime::Expiring, valueType, DEFAULT_YMQ_COUNTER_NAME, ymqName,                             \
            ELaziness::OnStart                                                                                                       \
        );                                                                                                                           \
    }

#define INIT_COUNTER(rootCounters, variable, expiring, valueType, lazy) \
    INIT_COUNTER_WITH_NAME(rootCounters, variable, Y_STRINGIZE(variable), expiring, valueType, lazy)

#define INIT_COUNTERS_COUPLE(rootCounters, sqsCounter, ymqCounter, expiring, valueType, lazy, aggr) \
    INIT_COUNTERS_COUPLE_WITH_NAMES(rootCounters, sqsCounter, ymqCounter, Y_STRINGIZE(sqsCounter), TString(QUEUE_CNTR_PREFIX) + Y_STRINGIZE(ymqCounter), expiring, valueType, lazy, aggr)

#define INIT_HISTOGRAMS_COUPLE_WITH_NAMES(rootCounters, sqsHistogram, ymqHistogram, sqsName, ymqName, expiring, sqsBuckets, ymqBuckets, lazy, aggr) \
    sqsHistogram.Init(rootCounters.SqsCounters, expiring, sqsBuckets, sqsName, lazy);                                                               \
    if (rootCounters.YmqCounters && !aggr) {                                                                                                        \
        ymqHistogram.Init(rootCounters.YmqCounters, ELifetime::Expiring, ymqBuckets, DEFAULT_YMQ_COUNTER_NAME, ymqName, lazy);                      \
    }

#define INIT_HISTOGRAMS_COUPLE_WITH_BUCKETS(rootCounters, sqsHistogram, ymqHistogram, expiring, sqsBuckets, ymqBuckets, lazy, aggr) \
    INIT_HISTOGRAMS_COUPLE_WITH_NAMES(rootCounters, sqsHistogram, ymqHistogram, Y_STRINGIZE(sqsHistogram), TString(QUEUE_CNTR_PREFIX) + Y_STRINGIZE(ymqHistogram), expiring, sqsBuckets, ymqBuckets, lazy, aggr)

#define INIT_HISTOGRAMS_COUPLE(rootCounters, sqsHistogram, ymqHistogram, expiring, buckets, lazy, aggr) \
    INIT_HISTOGRAMS_COUPLE_WITH_BUCKETS(rootCounters, sqsHistogram, ymqHistogram, expiring, buckets, Y_CAT(Ymq, buckets), lazy, aggr)

#define INIT_HISTOGRAM_COUNTER_WITH_NAME(rootCounters, variable, name, expiring, buckets, lazy) \
    variable.Init(rootCounters, expiring, buckets, name, lazy)
#define INIT_HISTOGRAM_COUNTER(rootCounters, variable, expiring, buckets, lazy) \
    INIT_HISTOGRAM_COUNTER_WITH_NAME(rootCounters, variable, Y_STRINGIZE(variable), expiring, buckets, lazy)

void TActionCounters::Init(const NKikimrConfig::TSqsConfig& cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters, EAction action, ELifetime lifetime) {
    const ELaziness laziness = IsActionForMessage(action) ? Lazy(cfg) : ELaziness::OnDemand;
    const EAction nonBatch = GetNonBatchAction(action);
    INIT_COUNTER_WITH_NAME(rootCounters, Success, TStringBuilder() << nonBatch << "_Success", lifetime, EValueType::Derivative, laziness);
    INIT_COUNTER_WITH_NAME(rootCounters, Errors, TStringBuilder() << nonBatch << "_Errors", lifetime, EValueType::Derivative, IsActionForMessage(action) ? ELaziness::OnStart : laziness);
    INIT_COUNTER_WITH_NAME(rootCounters, Infly, TStringBuilder() << nonBatch << "_Infly", lifetime, EValueType::Absolute, ELaziness::OnDemand);

    Duration.Init(rootCounters, lifetime, IsFastAction(action) ? FastActionsDurationBucketsMs : SlowActionsDurationBucketsMs, TStringBuilder() << nonBatch << "_Duration", laziness);

    if (action == EAction::ReceiveMessage) {
        INIT_HISTOGRAM_COUNTER_WITH_NAME(rootCounters, WorkingDuration, TStringBuilder() << action << "_WorkingDuration", lifetime, DurationBucketsMs, Lazy(cfg));
    }
}

void TYmqActionCounters::Init(
        const NKikimrConfig::TSqsConfig&, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters,
        EAction action, const TString& labelName, const TString& namePrefix, ELifetime lifetime
) {
    const auto& methodName = ActionToCloudConvMethod(action);
    SubGroup = rootCounters->GetSubgroup(labelName, methodName);
    INIT_COUNTER_WITH_NAME_AND_LABEL(
            SubGroup, Success, DEFAULT_YMQ_COUNTER_NAME, TStringBuilder() << namePrefix << "requests_count_per_second",
            lifetime, EValueType::Derivative, ELaziness::OnStart);
    INIT_COUNTER_WITH_NAME_AND_LABEL(
            SubGroup, Errors, DEFAULT_YMQ_COUNTER_NAME, TStringBuilder() << namePrefix << "errors_count_per_second",
            lifetime, EValueType::Derivative, ELaziness::OnStart
    );
    // ! - No inflight counter

    Duration.Init(
            SubGroup, lifetime, IsFastAction(action) ? YmqFastActionsDurationBucketsMs : YmqSlowActionsDurationBucketsMs,
            DEFAULT_YMQ_COUNTER_NAME, TStringBuilder() << namePrefix << "request_duration_milliseconds",
            ELaziness::OnStart
    );
}

void TActionCounters::SetAggregatedParent(TActionCounters* parent) {
    ActionCountersDescriptor.SetAggregatedParent(this, parent);
}

void TQueryTypeCounters::SetAggregatedParent(TQueryTypeCounters* parent) {
    QueryTypeCountersDescriptor.SetAggregatedParent(this, parent);
}

void TTransactionCounters::Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters,
        std::shared_ptr<TAlignedPagePoolCounters> poolCounters, bool forQueue) {
    AllocPoolCounters = std::move(poolCounters);

    const ELifetime lifetime = forQueue ? ELifetime::Expiring : ELifetime::Persistent;

    auto transactionsByType = rootCounters->GetSubgroup(DEFAULT_COUNTER_NAME, "TransactionsByType");
    auto transactionsDurationByType = rootCounters->GetSubgroup(DEFAULT_COUNTER_NAME, "TransactionsDurationsByType");
    auto transactionsFailedByType = rootCounters->GetSubgroup(DEFAULT_COUNTER_NAME, "TransactionsFailedByType");
    for (size_t i = 0; i < EQueryId::QUERY_VECTOR_SIZE; ++i) {
        const auto& typeStr = ToString(EQueryId(i));
        QueryTypeCounters[i].TransactionsCount.Init(transactionsByType, lifetime, EValueType::Derivative, QUERY_TYPE, typeStr, ELaziness::OnDemand);
        QueryTypeCounters[i].TransactionsFailed.Init(transactionsFailedByType, lifetime, EValueType::Derivative, QUERY_TYPE, typeStr, ELaziness::OnDemand);
        QueryTypeCounters[i].TransactionDuration.Init(transactionsDurationByType, lifetime, DurationBucketsMs, QUERY_TYPE, typeStr, ELaziness::OnDemand);
    }

    INIT_COUNTER(rootCounters, CompileQueryCount, lifetime, EValueType::Derivative, ELaziness::OnDemand);
    INIT_COUNTER(rootCounters, TransactionsCount, lifetime, EValueType::Derivative, ELaziness::OnDemand);
    INIT_COUNTER(rootCounters, TransactionsInfly, lifetime, EValueType::Absolute, ELaziness::OnDemand);
    INIT_COUNTER(rootCounters, TransactionRetryTimeouts, lifetime, EValueType::Derivative, ELaziness::OnDemand);
    INIT_COUNTER(rootCounters, TransactionRetries, lifetime, EValueType::Derivative, ELaziness::OnDemand);
    INIT_COUNTER(rootCounters, TransactionsFailed, lifetime, EValueType::Derivative, ELaziness::OnDemand);
}

void TTransactionCounters::SetAggregatedParent(const TIntrusivePtr<TTransactionCounters>& parent) {
    AggregatedParent = parent;
    TransactionCountersDescriptor.SetAggregatedParent(this, parent.Get());
    for (size_t i = 0; i < EQueryId::QUERY_VECTOR_SIZE; ++i) {
        QueryTypeCounters[i].SetAggregatedParent(parent ? &parent->QueryTypeCounters[i] : nullptr);
    }
}

void TAPIStatusesCounters::Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& root) {
    auto statusesByType = root->GetSubgroup(DEFAULT_COUNTER_NAME, "StatusesByType");
    for (const TString& errorCode : TErrorClass::GetAvailableErrorCodes()) {
        ErrorToCounter[errorCode].Init(statusesByType, ELifetime::Persistent, EValueType::Derivative, STATUS_CODE, errorCode, ELaziness::OnDemand);
    }
    OkCounter.Init(statusesByType, ELifetime::Persistent, EValueType::Derivative, STATUS_CODE, "OK", ELaziness::OnDemand);
    UnknownCounter.Init(statusesByType, ELifetime::Persistent, EValueType::Derivative, STATUS_CODE, "Unknown", ELaziness::OnDemand);
}

void TAPIStatusesCounters::AddError(const TString& errorCode, size_t count) {
    const auto counter = ErrorToCounter.find(errorCode);
    if (counter != ErrorToCounter.end()) {
        *counter->second += count;
    } else {
        *UnknownCounter += count;
    }
}

void TAPIStatusesCounters::AddOk(size_t count) {
    *OkCounter += count;
}

void TAPIStatusesCounters::SetAggregatedParent(TAPIStatusesCounters* parent) {
    for (auto& [err, counter] : ErrorToCounter) {
        if (parent) {
            counter.SetAggregatedParent(&parent->ErrorToCounter[err]);
        } else {
            counter.SetAggregatedParent(nullptr);
        }
    }
    OkCounter.SetAggregatedParent(parent ? &parent->OkCounter : nullptr);
    UnknownCounter.SetAggregatedParent(parent ? &parent->UnknownCounter : nullptr);
}

TFolderCounters::TFolderCounters(const TUserCounters* userCounters, const TString& folderId, bool insertCounters)
    : UserCounters(userCounters->UserCounters)
    , FolderId(folderId)
{
    if (insertCounters) {
        FolderCounters = GetFolderCounters(UserCounters, folderId);
    } else {
        FolderCounters = {new ::NMonitoring::TDynamicCounters(), new ::NMonitoring::TDynamicCounters()};
    }
    //InitCounters();
}

void TFolderCounters::InitCounters() {
    if (Inited) {
        return;
    }
    Inited = true;

    InsertCounters();
    if (UserCounters.YmqCounters) {
        total_count.Init(
            FolderCounters.YmqCounters, ELifetime::Expiring, EValueType::Absolute, DEFAULT_YMQ_COUNTER_NAME,
            "queue.total_count",
            ELaziness::OnStart
        );
    }
}


void TFolderCounters::InsertCounters() {

    if (UserCounters.Defined()) {
        if (!UserCounters.SqsCounters->FindSubgroup(FOLDER_LABEL, FolderId)) {
            FolderCounters.SqsCounters->ResetCounters();
            UserCounters.SqsCounters->RegisterSubgroup(FOLDER_LABEL, FolderId, FolderCounters.SqsCounters);
        }
        if (UserCounters.YmqCounters && !UserCounters.YmqCounters->FindSubgroup(FOLDER_LABEL, FolderId)) {
            FolderCounters.YmqCounters->ResetCounters();
            UserCounters.YmqCounters->RegisterSubgroup(FOLDER_LABEL, FolderId, FolderCounters.YmqCounters);
        }
    }
}

TQueueCounters::TQueueCounters(const NKikimrConfig::TSqsConfig& cfg,
                               const TIntrusivePtrCntrCouple& rootCounters,
                               const TUserCounters* userCounters,
                               const TString& queueName,
                               const TString& folderId,
                               bool insertCounters,
                               bool aggregated)
    : RootCounters(rootCounters)
    , UserCounters(userCounters->UserCounters)
    , FolderCounters(folderId ? GetFolderCounters(UserCounters, folderId) : TIntrusivePtrCntrCouple{})
    , Cfg(&cfg)
    , QueueName(queueName)
    , AggregatedCounters(aggregated)
    , UserShowDetailedCountersDeadline(userCounters->ShowDetailedCountersDeadline)
    , AllocPoolCounters(userCounters->DetailedCounters.TransactionCounters->AllocPoolCounters)
{
    if (insertCounters) {
        QueueCounters = GetQueueCounters(FolderCounters.Defined() ? FolderCounters : UserCounters, queueName);
    } else {
        QueueCounters = {new ::NMonitoring::TDynamicCounters(), new ::NMonitoring::TDynamicCounters()};
    }
    InitCounters();
}

void TQueueCounters::InitCounters(bool forLeaderNode) {
    if (!RequestTimeouts) {
        INIT_COUNTERS_COUPLE(
                QueueCounters,
                RequestTimeouts, request_timeouts_count_per_second,
                ELifetime::Persistent, EValueType::Derivative,
                Lazy(*Cfg), AggregatedCounters
        );
    }


    if (forLeaderNode) {
        INIT_COUNTER(QueueCounters.SqsCounters, RequestsThrottled, ELifetime::Expiring, EValueType::Derivative, ELaziness::OnStart);

        INIT_COUNTER(QueueCounters.SqsCounters, QueueMasterStartProblems, ELifetime::Persistent, EValueType::Derivative, ELaziness::OnStart);
        INIT_COUNTER(QueueCounters.SqsCounters, QueueLeaderStartProblems, ELifetime::Persistent, EValueType::Derivative, ELaziness::OnStart);

        INIT_COUNTERS_COUPLE(
                QueueCounters,
                MessagesPurged, purged_count_per_second,
                ELifetime::Expiring, EValueType::Derivative,
                ELaziness::OnStart, AggregatedCounters
        );

        INIT_HISTOGRAMS_COUPLE_WITH_BUCKETS(
                QueueCounters, MessageReceiveAttempts, receive_attempts_count_rate,
                ELifetime::Expiring, MessageReceiveAttemptsBuckets, MessageReceiveAttemptsBuckets,
                Lazy(*Cfg), AggregatedCounters
        );
        INIT_HISTOGRAMS_COUPLE(
                QueueCounters,
                ClientMessageProcessing_Duration, client_processing_duration_milliseconds,
                ELifetime::Expiring, ClientDurationBucketsMs,
                Lazy(*Cfg), AggregatedCounters
        );
        INIT_HISTOGRAMS_COUPLE(
                QueueCounters,
                MessageReside_Duration, reside_duration_milliseconds,
                ELifetime::Expiring, ClientDurationBucketsMs,
                Lazy(*Cfg), AggregatedCounters
        );
        INIT_COUNTERS_COUPLE(
                QueueCounters,
                DeleteMessage_Count, deleted_count_per_second,
                ELifetime::Expiring, EValueType::Derivative,
                Lazy(*Cfg), AggregatedCounters
        );

        INIT_COUNTERS_COUPLE(
                QueueCounters,
                ReceiveMessage_EmptyCount, empty_receive_attempts_count_per_second,
                ELifetime::Expiring, EValueType::Derivative,
                Lazy(*Cfg), AggregatedCounters
        );
        INIT_COUNTERS_COUPLE(
                QueueCounters,
                ReceiveMessage_Count, received_count_per_second,
                ELifetime::Expiring, EValueType::Derivative,
                Lazy(*Cfg), AggregatedCounters
        );
        INIT_COUNTERS_COUPLE(
                QueueCounters,
                ReceiveMessage_BytesRead, received_bytes_per_second,
                ELifetime::Expiring, EValueType::Derivative,
                Lazy(*Cfg), AggregatedCounters
        );

        INIT_COUNTER(QueueCounters.SqsCounters, MessagesMovedToDLQ, ELifetime::Expiring, EValueType::Derivative, ELaziness::OnStart);

        INIT_COUNTERS_COUPLE(
                QueueCounters,
                SendMessage_DeduplicationCount, deduplicated_count_per_second,
                ELifetime::Expiring, EValueType::Derivative,
                Lazy(*Cfg), AggregatedCounters
        );
        INIT_COUNTERS_COUPLE(
                QueueCounters,
                SendMessage_Count, sent_count_per_second,
                ELifetime::Expiring, EValueType::Derivative,
                Lazy(*Cfg), AggregatedCounters
        );
        INIT_COUNTERS_COUPLE(
                QueueCounters,
                SendMessage_BytesWritten, sent_bytes_per_second,
                ELifetime::Expiring, EValueType::Derivative,
                Lazy(*Cfg), AggregatedCounters
        );

        INIT_COUNTERS_COUPLE(
                QueueCounters,
                MessagesCount, stored_count,
                ELifetime::Expiring, EValueType::Absolute,
                Lazy(*Cfg), AggregatedCounters
        );
        INIT_COUNTERS_COUPLE(
                QueueCounters,
                InflyMessagesCount, inflight_count,
                ELifetime::Expiring, EValueType::Absolute,
                Lazy(*Cfg), AggregatedCounters
        );
        if (!AggregatedCounters) { // OldestMessageAgeSeconds will not be aggregated properly.
            INIT_COUNTERS_COUPLE(
                    QueueCounters,
                    OldestMessageAgeSeconds, oldest_age_milliseconds,
                    ELifetime::Expiring, EValueType::Absolute,
                    ELaziness::OnStart, false
            );
        }
    }

    for (EAction action = static_cast<EAction>(EAction::Unknown + 1); action < EAction::ActionsArraySize; action = static_cast<EAction>(action + 1)) {
        if (IsActionForQueue(action)) {
            if (forLeaderNode && IsProxyAction(action) || !forLeaderNode && !IsProxyAction(action)) {
                SqsActionCounters[action].Init(*Cfg, QueueCounters.SqsCounters, action, forLeaderNode ? ELifetime::Expiring : ELifetime::Persistent);
            }
        }
        if (IsActionForQueueYMQ(action) && QueueCounters.YmqCounters && !AggregatedCounters) {
            if (forLeaderNode && IsProxyAction(action) || !forLeaderNode && !IsProxyAction(action)) {
                YmqActionCounters[action].Init(
                        *Cfg, QueueCounters.YmqCounters, action, METHOD_LABLE, ACTION_CNTR_PREFIX,
                        forLeaderNode ? ELifetime::Expiring : ELifetime::Persistent
                );
            }
        }
    }

    DetailedCounters.Init(QueueCounters.SqsCounters, AllocPoolCounters, forLeaderNode);
}

void TQueueCounters::TDetailedCounters::Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& queueCounters,
        const std::shared_ptr<TAlignedPagePoolCounters>& allocPoolCounters, bool forLeaderNode) {
    if (!GetConfiguration_Duration) {
        INIT_HISTOGRAM_COUNTER(queueCounters, GetConfiguration_Duration, ELifetime::Expiring, DurationBucketsMs, ELaziness::OnDemand);
    }

    if (forLeaderNode) {
        TransactionCounters = new TTransactionCounters();
        TransactionCounters->Init(queueCounters, allocPoolCounters, true);

        INIT_COUNTER(queueCounters, ReceiveMessage_KeysInvalidated, ELifetime::Expiring, EValueType::Derivative, ELaziness::OnDemand);

        INIT_HISTOGRAM_COUNTER(queueCounters, ReceiveMessageImmediate_Duration, ELifetime::Expiring, DurationBucketsMs, ELaziness::OnDemand);
    }
}

void TQueueCounters::TDetailedCounters::SetAggregatedParent(TQueueCounters::TDetailedCounters* parent) {
    if (TransactionCounters) {
        TransactionCounters->SetAggregatedParent(parent ? parent->TransactionCounters : nullptr);
    }
    QueueDetailedCountersDescriptor.SetAggregatedParent(this, parent);
}

void TQueueCounters::InsertCounters() {
    auto insert = [&](auto& parent) {
        if (!parent.SqsCounters->FindSubgroup(QUEUE_LABEL, QueueName)) {
            QueueCounters.SqsCounters->ResetCounters();
            parent.SqsCounters->RegisterSubgroup(QUEUE_LABEL, QueueName, QueueCounters.SqsCounters);
        }
        if (parent.YmqCounters && !parent.YmqCounters->FindSubgroup(QUEUE_LABEL, QueueName)) {
            QueueCounters.YmqCounters->ResetCounters();
            parent.YmqCounters->RegisterSubgroup(QUEUE_LABEL, QueueName, QueueCounters.YmqCounters);
        }
    };

    if (FolderCounters.Defined()) {
        insert(FolderCounters);
    } else {
        insert(UserCounters);
    }
}

void TQueueCounters::SetAggregatedParent(const TIntrusivePtr<TQueueCounters>& parent) {
    AggregatedParent = parent;
    QueueCountersDescriptor.SetAggregatedParent(this, parent.Get());
    DetailedCounters.SetAggregatedParent(parent ? &parent->DetailedCounters : nullptr);
    for (size_t i = 0; i < EAction::ActionsArraySize; ++i) {
        SqsActionCounters[i].SetAggregatedParent(parent ? &parent->SqsActionCounters[i] : nullptr);
    }
}

void TQueueCounters::RemoveCounters() {
    auto couple = FolderCounters.Defined() ? FolderCounters : UserCounters;
    couple.SqsCounters->RemoveSubgroup(QUEUE_LABEL, QueueName);
    if (couple.YmqCounters)
        couple.YmqCounters->RemoveSubgroup(QUEUE_LABEL, QueueName);
}

TIntrusivePtr<TQueueCounters> TQueueCounters::GetCountersForLeaderNode() {
    TIntrusivePtr<TQueueCounters> counters = new TQueueCounters(*this);
    counters->NotLeaderNodeCounters = this;
    counters->InitCounters(true);
    if (AggregatedParent) {
        counters->SetAggregatedParent(AggregatedParent);
    }
    return counters;
}

TIntrusivePtr<TQueueCounters> TQueueCounters::GetCountersForNotLeaderNode() {
    return NotLeaderNodeCounters;
}

void TUserCounters::InitCounters(const TString& userName, const std::shared_ptr<TAlignedPagePoolCounters>& allocPoolCounters) {
    UserCounters = GetUserCounters(SqsCoreCounters, userName);

    INIT_COUNTER(UserCounters.SqsCounters, RequestTimeouts, ELifetime::Persistent, EValueType::Derivative, Lazy(*Cfg));

    if (Cfg->GetForceAccessControl() && Cfg->AccountsWithoutMandatoryAuthSize() && (userName == TOTAL_COUNTER_LABEL || IsIn(Cfg->GetAccountsWithoutMandatoryAuth(), userName))) {
        INIT_COUNTER(UserCounters.SqsCounters, UnauthenticatedAccess, ELifetime::Persistent, EValueType::Derivative, ELaziness::OnStart);
    }

    for (EAction action = static_cast<EAction>(EAction::Unknown + 1); action < EAction::ActionsArraySize; action = static_cast<EAction>(action + 1)) {
        if (IsActionForUser(action)) {
            SqsActionCounters[action].Init(*Cfg, UserCounters.SqsCounters, action);
        }
        if (IsActionForUserYMQ(action) && UserCounters.YmqCounters && !IsAggregatedCounters) {
            YmqActionCounters[action].Init(*Cfg, UserCounters.YmqCounters, action, METHOD_LABLE, ACTION_CNTR_PREFIX);
        }
    }

    // ToDo. Errors codes here. Will probably need this in Ymq counters further
    DetailedCounters.Init(UserCounters, allocPoolCounters, *Cfg);

    AggregatedQueueCounters = CreateQueueCountersImpl(TOTAL_COUNTER_LABEL, Cfg->GetYandexCloudMode() ? TOTAL_COUNTER_LABEL : TString(), true, true)->GetCountersForLeaderNode();

    if (AggregatedParent) {
        AggregatedQueueCounters->SetAggregatedParent(AggregatedParent->AggregatedQueueCounters);
        UserCountersDescriptor.SetAggregatedParent(this, AggregatedParent.Get());
        DetailedCounters.SetAggregatedParent(&AggregatedParent->DetailedCounters);
        for (size_t i = 0; i < EAction::ActionsArraySize; ++i) {
            SqsActionCounters[i].SetAggregatedParent(&AggregatedParent->SqsActionCounters[i]);
        }
    }
}

void TUserCounters::TDetailedCounters::Init(const TIntrusivePtrCntrCouple& userCounters,
        const std::shared_ptr<TAlignedPagePoolCounters>& allocPoolCounters, const NKikimrConfig::TSqsConfig& cfg) {
    TransactionCounters = new TTransactionCounters();
    TransactionCounters->Init(GetAggregatedCountersFromUserCounters(userCounters, cfg), allocPoolCounters, false);

    APIStatuses.Init(userCounters.SqsCounters);

    INIT_HISTOGRAM_COUNTER(userCounters.SqsCounters, GetConfiguration_Duration, ELifetime::Persistent, DurationBucketsMs, ELaziness::OnDemand);
    INIT_HISTOGRAM_COUNTER(userCounters.SqsCounters, GetQuota_Duration, ELifetime::Persistent, GetQuotaDurationBucketsMs, ELaziness::OnDemand);

    INIT_COUNTER(userCounters.SqsCounters, CreateAccountOnTheFly_Success, ELifetime::Persistent, EValueType::Derivative, ELaziness::OnDemand);
    INIT_COUNTER(userCounters.SqsCounters, CreateAccountOnTheFly_Errors, ELifetime::Persistent, EValueType::Derivative, ELaziness::OnDemand);
}

void TUserCounters::TDetailedCounters::SetAggregatedParent(TUserCounters::TDetailedCounters* parent) {
    TransactionCounters->SetAggregatedParent(parent ? parent->TransactionCounters : nullptr);
    UserDetailedCountersDescriptor.SetAggregatedParent(this, parent);
}

TIntrusivePtr<TFolderCounters> TUserCounters::CreateFolderCounters(const TString& folderId, bool insertCounters) {
    return new TFolderCounters(this, folderId, insertCounters);
}

TIntrusivePtr<TQueueCounters> TUserCounters::CreateQueueCounters(const TString& queueName, const TString& folderId, bool insertCounters) {
    auto counters = CreateQueueCountersImpl(queueName, folderId, insertCounters, IsAggregatedCounters);
    counters->SetAggregatedParent(AggregatedQueueCounters);
    return counters;
}

TIntrusivePtr<TQueueCounters> TUserCounters::CreateQueueCountersImpl(const TString& queueName, const TString& folderId, bool insertCounters, bool aggregated) {
    return new TQueueCounters(*Cfg, SqsCoreCounters, this, queueName, folderId, insertCounters, aggregated);
}

void TUserCounters::RemoveCounters() {
    SqsCoreCounters.SqsCounters->RemoveSubgroup(USER_LABEL, UserName);
    if (SqsCoreCounters.YmqCounters)
        SqsCoreCounters.YmqCounters->RemoveSubgroup(CLOUD_LABEL, UserName);
}

void TUserCounters::DisableCounters(bool disable) {
    if (disable) {
        SqsCoreCounters.SqsCounters->RemoveSubgroup(USER_LABEL, UserName);
        if (SqsCoreCounters.YmqCounters)
            SqsCoreCounters.YmqCounters->RemoveSubgroup(CLOUD_LABEL, UserName);
    } else {
        if (!SqsCoreCounters.SqsCounters->FindSubgroup(USER_LABEL, UserName)) {
            UserCounters.SqsCounters->ResetCounters();
            SqsCoreCounters.SqsCounters->RegisterSubgroup(USER_LABEL, UserName, UserCounters.SqsCounters);
        }
        if (SqsCoreCounters.YmqCounters && !SqsCoreCounters.YmqCounters->FindSubgroup(CLOUD_LABEL, UserName)) {
            UserCounters.YmqCounters->ResetCounters();
            SqsCoreCounters.YmqCounters->RegisterSubgroup(CLOUD_LABEL, UserName, UserCounters.YmqCounters);
        }
    }
}

TIntrusivePtr<THttpUserCounters> THttpCounters::GetUserCountersImpl(const TString& userName, const TIntrusivePtr<THttpUserCounters>& aggregatedUserCounters) {
    {
        auto guard = Guard(Lock);
        auto countersIt = UserCounters.find(userName);
        if (countersIt != UserCounters.end()) {
            return countersIt->second;
        }
    }

    TIntrusivePtr<THttpUserCounters> userCounters = new THttpUserCounters(*Cfg, SqsHttpCounters, userName);
    if (aggregatedUserCounters) {
        userCounters->SetAggregatedParent(aggregatedUserCounters);
    }

    auto guard = Guard(Lock);
    auto [iter, inserted] = UserCounters.emplace(userName, std::move(userCounters));
    return iter->second;
}

TIntrusivePtr<THttpUserCounters> THttpCounters::GetUserCounters(const TString& userName) {
    return GetUserCountersImpl(userName, AggregatedUserCounters);
}

void THttpCounters::InitCounters() {
    INIT_COUNTER(SqsHttpCounters, RequestExceptions, ELifetime::Persistent, EValueType::Derivative, ELaziness::OnStart);
    INIT_COUNTER(SqsHttpCounters, InternalExceptions, ELifetime::Persistent, EValueType::Derivative, ELaziness::OnStart);
    INIT_COUNTER(SqsHttpCounters, ConnectionsCount, ELifetime::Persistent, EValueType::Absolute, ELaziness::OnStart);

    AggregatedUserCounters = GetUserCountersImpl(TOTAL_COUNTER_LABEL, nullptr);
}

void THttpUserCounters::InitCounters(const TString& userName) {
    UserCounters = SqsHttpCounters->GetSubgroup(USER_LABEL, userName);

    for (EAction action = static_cast<EAction>(EAction::Unknown + 1); action < EAction::ActionsArraySize; action = static_cast<EAction>(action + 1)) {
        ActionCounters[action].Init(*Cfg, UserCounters, action);
    }

    INIT_COUNTER(UserCounters, RequestExceptions, ELifetime::Persistent, EValueType::Derivative, Lazy(*Cfg));
}

void THttpUserCounters::SetAggregatedParent(const TIntrusivePtr<THttpUserCounters>& parent) {
    AggregatedParent = parent;
    HttpUserCountersDescriptor.SetAggregatedParent(this, parent.Get());
    for (size_t i = 0; i < EAction::ActionsArraySize; ++i) {
        ActionCounters[i].SetAggregatedParent(parent ? &parent->ActionCounters[i] : nullptr);
    }
}

void THttpActionCounters::Init(const NKikimrConfig::TSqsConfig& cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& rootCounters, EAction action) {
    Cfg = &cfg;
    Requests.Init(rootCounters, ELifetime::Persistent, EValueType::Derivative, TStringBuilder() << action << "Request", Lazy(*Cfg));
}

void THttpActionCounters::SetAggregatedParent(THttpActionCounters* parent) {
    HttpActionCountersDescriptor.SetAggregatedParent(this, parent);
}

static const TString& StringifyGrpcStatus(int grpcStatus) {
    if (grpcStatus < 0 || grpcStatus > TCloudAuthCounters::GRPC_STATUSES_COUNT - 2) {
        grpcStatus = TCloudAuthCounters::GRPC_STATUSES_COUNT - 1;
    }

    static const TString statusStrings[] = {
        "Ok",
        "Cancelled",
        "Unknown",
        "InvalidArgument",
        "DeadlineExceeded",
        "NotFound",
        "AlreadyExists",
        "PermissionDenied",
        "ResourceExhausted",
        "FailedPrecondition",
        "Aborted",
        "OutOfRange",
        "Unimplemented",
        "Internal",
        "Unavailable",
        "DataLoss",
        "Unauthenticated",
        "Misc",
    };

    static_assert(Y_ARRAY_SIZE(statusStrings) == TCloudAuthCounters::GRPC_STATUSES_COUNT);

    return statusStrings[grpcStatus];
}

void TCloudAuthCounters::IncCounter(const NCloudAuth::EActionType actionType, const NCloudAuth::ECredentialType credentialType, int grpcStatus) {
    if (grpcStatus < 0 || grpcStatus > GRPC_STATUSES_COUNT - 2) {
        grpcStatus = GRPC_STATUSES_COUNT - 1;
    }

    ++*CloudAuthCounters[actionType][credentialType][grpcStatus];
}

void TCloudAuthCounters::IncAuthorizeCounter(const NCloudAuth::ECredentialType credentialType, bool error) {
    if (error) {
        ++*AuthorizeError[credentialType];
    } else {
        ++*AuthorizeSuccess[credentialType];
    }
}

void TCloudAuthCounters::InitCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> cloudAuthCounters) {
    for (size_t actionType = 0; actionType < NCloudAuth::EActionType::ActionTypesCount; ++actionType) {
        const auto actionTypeStr = ToString(static_cast<NCloudAuth::EActionType>(actionType));
        const auto actionCounters = cloudAuthCounters->GetSubgroup("action_type", actionTypeStr);
        for (size_t credentialType = 0; credentialType < NCloudAuth::ECredentialType::CredentialTypesCount; ++credentialType) {
            const auto credentialTypeStr = ToString(static_cast<NCloudAuth::ECredentialType>(credentialType));
            const auto actionAndCredentialCounters = actionCounters->GetSubgroup("credential_type", credentialTypeStr);

            if (actionType == NCloudAuth::EActionType::Authorize) {
                INIT_COUNTER_WITH_NAME(actionAndCredentialCounters, AuthorizeSuccess[credentialType], "Ok", ELifetime::Persistent, EValueType::Derivative, Lazy(*Cfg));
                INIT_COUNTER_WITH_NAME(actionAndCredentialCounters, AuthorizeError[credentialType], "PermissionDenied", ELifetime::Persistent, EValueType::Derivative, Lazy(*Cfg));
            } else {
                for (size_t grpcStatus = 0; grpcStatus < GRPC_STATUSES_COUNT; ++grpcStatus) {
                    INIT_COUNTER_WITH_NAME(actionAndCredentialCounters, CloudAuthCounters[actionType][credentialType][grpcStatus], StringifyGrpcStatus(grpcStatus), ELifetime::Persistent, EValueType::Derivative, Lazy(*Cfg));
                }
            }
        }
    }

    INIT_HISTOGRAM_COUNTER(cloudAuthCounters, AuthenticateDuration, ELifetime::Persistent, DurationBucketsMs, Lazy(*Cfg));
    INIT_HISTOGRAM_COUNTER(cloudAuthCounters, AuthorizeDuration, ELifetime::Persistent, DurationBucketsMs, Lazy(*Cfg));
    INIT_HISTOGRAM_COUNTER(cloudAuthCounters, GetFolderIdDuration, ELifetime::Persistent, DurationBucketsMs, Lazy(*Cfg));
}

void TMeteringCounters::InitCounters(const TVector<TString>& classifierLabels) {
    const TString classifierRequestsLabel("ClassifierRequests_");
    const TString idleClassifierRequestsLabel("IdleClassifierRequests_");
    for (auto label : classifierLabels) {
        INIT_COUNTER_WITH_NAME(SqsMeteringCounters, ClassifierRequestsResults[label], TStringBuilder() << classifierRequestsLabel << label, ELifetime::Persistent, EValueType::Derivative, Lazy(Config));
        INIT_COUNTER_WITH_NAME(SqsMeteringCounters, IdleClassifierRequestsResults[label], TStringBuilder() << idleClassifierRequestsLabel << label, ELifetime::Persistent, EValueType::Derivative, Lazy(Config));
    }
}

void TMonitoringCounters::InitCounters() {
    INIT_COUNTER(MonitoringCounters, CleanupRemovedQueuesLagSec, ELifetime::Persistent, EValueType::Derivative, Lazy(Config));
    INIT_COUNTER(MonitoringCounters, CleanupRemovedQueuesLagCount, ELifetime::Persistent, EValueType::Derivative, Lazy(Config));
    INIT_COUNTER(MonitoringCounters, CleanupRemovedQueuesDone, ELifetime::Persistent, EValueType::Derivative, Lazy(Config));
    INIT_COUNTER(MonitoringCounters, CleanupRemovedQueuesRows, ELifetime::Persistent, EValueType::Derivative, Lazy(Config));
    INIT_COUNTER(MonitoringCounters, CleanupRemovedQueuesErrors, ELifetime::Persistent, EValueType::Derivative, Lazy(Config));


    INIT_COUNTER(MonitoringCounters, LocalLeaderStartInflight, ELifetime::Persistent, EValueType::Derivative, Lazy(Config));
    INIT_COUNTER(MonitoringCounters, LocalLeaderStartQueue, ELifetime::Persistent, EValueType::Derivative, Lazy(Config));

    INIT_HISTOGRAM_COUNTER(MonitoringCounters, LocalLeaderStartAwaitMs, ELifetime::Expiring, DurationBucketsMs, ELaziness::OnDemand);
}

} // namespace NKikimr::NSQS
