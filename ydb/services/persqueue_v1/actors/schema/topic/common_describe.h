#pragma once

#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>

#include <util/string/join.h>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <memory>
#include <optional>

namespace NKikimr::NGRpcProxy::V1::NTopic {

template<class T>
void SetProtoTime(T* proto, const ui64 ms) {
    proto->set_seconds(ms / 1000);
    proto->set_nanos((ms % 1000) * 1'000'000);
}

template<class T>
void UpdateProtoTime(T& proto, const T& time, bool storeMin) {
    bool cmp = proto.seconds() > time.seconds() || (proto.seconds() == time.seconds() && proto.nanos() > time.nanos());
    if (cmp == storeMin) {
        proto.CopyFrom(time);
    }
}

inline void AddWindowsStat(Ydb::Topic::MultipleWindowsStat *stat, ui64 perMin, ui64 perHour, ui64 perDay) {
    stat->set_per_minute(stat->per_minute() + perMin);
    stat->set_per_hour(stat->per_hour() + perHour);
    stat->set_per_day(stat->per_day() + perDay);
}

enum EDescribeEv : ui32 {
    EvDescribeResponse = NPQ::NEvents::InternalEventSpaceBegin(NPQ::NEvents::EServices::SCHEMA) + 32,
};

struct TDescribeSettings {
    TString Path;
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    NPQ::NDescriber::TAccessRights AccessRights;
    bool IncludeStats = false;
    bool IncludeLocation = false;
};

struct TPartitionDescribeInfo {
    Ydb::Topic::PartitionLocation Location;
    Ydb::Topic::DescribeConsumerResult::PartitionInfo Stats;
    NKikimrPQ::TReadSessionsInfoResponse::TPartitionInfo ReadSession;
};

struct TEvDescribeResponse : public NActors::TEventLocal<TEvDescribeResponse, EDescribeEv::EvDescribeResponse> {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    TString ErrorMessage;
    Ydb::PersQueue::ErrorCode::ErrorCode IssueCode = Ydb::PersQueue::ErrorCode::OK;

    NPQ::NDescriber::TTopicInfo TopicInfo;
    Ydb::Scheme::Entry SelfEntry;
    absl::flat_hash_map<ui32, TPartitionDescribeInfo> Partitions;
    TString ConsumerName;
};

struct TDescribeSchemaError {
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::BAD_REQUEST;
    TString Message;
    Ydb::PersQueue::ErrorCode::ErrorCode IssueCode = Ydb::PersQueue::ErrorCode::BAD_REQUEST;
};

struct TDescribeSchemaResult {
    std::optional<TDescribeSchemaError> Error;
    TString ConsumerName;
};

class IDescribeStrategy {
public:
    virtual ~IDescribeStrategy() = default;

    virtual TString GetName() const = 0;
    virtual TDescribeSchemaResult ValidateSchema(const NPQ::NDescriber::TTopicInfo& topicInfo) = 0;
    virtual bool NeedProcessPartition(
        const NKikimrSchemeOp::TPersQueueGroupDescription::TPartition& partition) const = 0;
    virtual std::unique_ptr<TEvPersQueue::TEvGetReadSessionsInfo> CreateReadSessionsInfoRequest() const = 0;
    virtual std::unique_ptr<TEvPersQueue::TEvStatus> CreateStatusRequest() const = 0;
};

class TDescribeOperationActor: public NActors::TActorBootstrapped<TDescribeOperationActor>
                         , protected NPQ::TPipeCacheClient
                         , public NPQ::TConstantLogPrefix {
    static constexpr NKikimrServices::EServiceKikimr Service = NKikimrServices::EServiceKikimr::PQ_SCHEMA;
    static constexpr TDuration RequestTimeout = TDuration::Seconds(30);
    static constexpr size_t StatsMaxRetries = 15;
    static constexpr TDuration StatsRetryInitialDelay = TDuration::MilliSeconds(25);
    static constexpr TDuration StatsRetryMaxDelay = TDuration::MilliSeconds(250);
    static constexpr ui64 BalancerRetryWakeupTag = 100;
    static constexpr ui64 RequestTimeoutWakeupTag = 101;

public:
    TDescribeOperationActor(
        const NActors::TActorId& parent,
        TDescribeSettings&& settings,
        std::unique_ptr<IDescribeStrategy> strategy);

    void Bootstrap();

    TStringBuilder LogBuilder() const;
    TString BuildLogPrefix() const override;

private:
    void PassAway() override;
    void HandlePoison();
    void ReplyWithError(
        Ydb::StatusIds::StatusCode status,
        const TString& messageText,
        Ydb::PersQueue::ErrorCode::ErrorCode issueCode = Ydb::PersQueue::ErrorCode::BAD_REQUEST);
    void ReplyWithSuccess();

    STFUNC(StateDescribe);
    STFUNC(StateWork);

    void Handle(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev);
    void Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev);
    void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev);
    void Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);

    bool ReplyIfPossible();
    void RequestReadBalancer();
    void RequestStats(ui64 tabletId);
    void ScheduleStatsRetry(ui64 tabletId);
    bool HandleStatsRetryWakeup(ui64 tabletId);
    void ScheduleBalancerRetry();
    void HandleBalancerRetryWakeup();
    TDuration RemainingRequestTimeout() const;
    void HandleRequestTimeout();

private:
    const NActors::TActorId Parent;
    TDescribeSettings Settings;
    std::unique_ptr<IDescribeStrategy> Strategy;

    NPQ::NDescriber::TTopicInfo TopicInfo;
    Ydb::Scheme::Entry SelfEntry;
    absl::flat_hash_map<ui32, TPartitionDescribeInfo> Partitions;
    TString ConsumerName;

    ui64 ReadBalancerTabletId = 0;
    absl::flat_hash_set<ui64> TabletsInflight;

    bool LocationsReceived = false;
    bool ReadSessionsReceived = false;
    bool BalancerRetryPending = false;
    bool IsDead = false;
    TBackoff LocationsBackoff = TBackoff(25, TDuration::MilliSeconds(10), TDuration::MilliSeconds(100));
    std::optional<TInstant> RequestStartTime;
    absl::flat_hash_map<ui64, TBackoff> StatsBackoff;
    absl::flat_hash_set<ui64> StatsRetryPending;
    NActors::TActorId DescriberActorId;
};

} // namespace NKikimr::NGRpcProxy::V1::NTopic
