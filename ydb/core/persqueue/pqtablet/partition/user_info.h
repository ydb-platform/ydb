#pragma once

#include "working_time_counter.h"
#include "subscriber.h"
#include <ydb/core/persqueue/public/counters/percentile_counter.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/pqtablet/metering_sink.h>
#include <ydb/core/persqueue/dread_cache_service/caching_service.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/persqueue/topic_parser/counters.h>

#include <library/cpp/sliding_window/sliding_window.h>

#include <util/generic/set.h>

namespace NKikimr {
namespace NPQ {


TString EscapeBadChars(const TString& str);

namespace NDeprecatedUserData {
    // [offset:64bit][generation:32bit][step:32bit][session:other bytes]
    TBuffer Serialize(ui64 offset, ui32 gen, ui32 step, const TString& session);
    void Parse(const TString& data, ui64& offset, ui32& gen, ui32& step, TString& session);
} // NDeprecatedUserInfo

static const ui32 MAX_USER_TS_CACHE_SIZE = 10'000;
static const ui64 MIN_TIMESTAMP_MS = 1'000'000'000'000ll; // around 2002 year

struct TMessageInfo {
    TInstant CreateTimestamp;
    TInstant WriteTimestamp;
};

struct TConsumerSnapshot {
    TInstant Now;

    TMessageInfo LastCommittedMessage;

    i64 ReadOffset;
    TInstant LastReadTimestamp;
    TMessageInfo LastReadMessage;

    TDuration ReadLag;
    TDuration CommitedLag;
    TDuration TotalLag;
};

struct TUserInfoBase {
    TString User;
    ui64 ReadRuleGeneration = 0;

    TString Session = "";
    ui32 Generation = 0;
    ui32 Step = 0;
    i64 Offset = 0;
    bool AnyCommits = false;

    bool Important = false;
    TDuration AvailabilityPeriod;
    TInstant ReadFromTimestamp;

    ui64 PartitionSessionId = 0;
    TActorId PipeClient;

    std::optional<TString> CommittedMetadata = std::nullopt;
};

struct TUserInfo: public TUserInfoBase, public TAtomicRefCount<TUserInfo> {
    TUserInfo(
        const TActorContext& ctx,
        NMonitoring::TDynamicCounterPtr streamCountersSubgroup,
        TConstArrayRef<NMonitoring::TDynamicCounterPtr> partitionCountersSubgroups,
        const TString& user,
        const ui64 readRuleGeneration, const bool important, const TDuration availabilityPeriod,
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const ui32 partition, const TString& session, ui64 partitionSession, ui32 gen, ui32 step, i64 offset,
        const ui64 readOffsetRewindSum, const TString& dcId, TInstant readFromTimestamp,
        const TString& dbPath, bool meterRead, const TActorId& pipeClient, bool anyCommits,
        const std::optional<TString>& committedMetadata = std::nullopt
    );

    void ForgetSubscription(i64 endOffset, const TInstant& now);
    void UpdateReadingState();
    void UpdateReadingTimeAndState(i64 endOffset, TInstant now);
    void ReadDone(const TActorContext& ctx, const TInstant& now, ui64 readSize, ui32 readCount,
                  const TString& clientDC, const TActorId& tablet, bool isExternalRead, i64 endOffset);

    void SetupDetailedMetrics(const TActorContext& ctx, TConstArrayRef<NMonitoring::TDynamicCounterPtr> subgroups);
    void ResetDetailedMetrics();
    void SetupStreamCounters(NMonitoring::TDynamicCounterPtr subgroup);
    void SetupTopicCounters(const TActorContext& ctx, const TString& dcId, const TString& partition);

    void UpdateReadOffset(const i64 offset, TInstant writeTimestamp, TInstant createTimestamp, TInstant now, bool force = false);
    void AddTimestampToCache(const ui64 offset, TInstant writeTimestamp, TInstant createTimestamp, bool isUserRead, TInstant now);
    bool UpdateTimestampFromCache();

    i64 GetReadOffset() const {
        return ReadOffset == -1 ? Offset : (ReadOffset + 1); //+1 because we want to track first not readed offset
    }
    TInstant GetReadTimestamp() const {
        return ReadTimestamp;
    }
    ui64 GetWriteLagMs() const {
        return WriteLagMs.GetValue();
    }

private:
    friend class TUsersInfoStorage;
    void SetImportant(bool important, TDuration availabilityPeriod);

public:

    bool ActualTimestamps = false;
    // WriteTimestamp of the last committed message
    TInstant WriteTimestamp;
    // CreateTimestamp of the last committed message
    TInstant CreateTimestamp;

    // Timstamp of the last read
    TInstant ReadTimestamp;

    i64 ReadOffset = -1;

    // WriteTimestamp of the last read message
    TInstant ReadWriteTimestamp;
    // CreateTimestamp of the last read message
    TInstant ReadCreateTimestamp;
    ui64 ReadOffsetRewindSum = 0;

    bool ReadScheduled = false;

    //cache is used for storing WriteTime;CreateTime for offsets.
    //When client will commit to new position, timestamps for this offset could be in cache - not insane client should read data before commit
    std::deque<std::pair<ui64, std::pair<TInstant, TInstant>>> Cache;

    bool HasReadRule = false;
    std::optional<TTabletLabeledCountersBase> LabeledCounters;
    NPersQueue::TTopicConverterPtr TopicConverter;

    TWorkingTimeCounter Counter;
    NKikimr::NPQ::TMultiCounter BytesRead;
    NKikimr::NPQ::TMultiCounter MsgsRead;
    NKikimr::NPQ::TMultiCounter BytesReadGrpc;
    NKikimr::NPQ::TMultiCounter MsgsReadGrpc;
    TMap<TString, NKikimr::NPQ::TMultiCounter> BytesReadFromDC;

    // Per partition counters
    struct TPerPartitionCounters {
        NMonitoring::TDynamicCounters::TCounterPtr BytesReadPerPartition;
        NMonitoring::TDynamicCounters::TCounterPtr MessagesReadPerPartition;
        NMonitoring::TDynamicCounters::TCounterPtr MessageLagByLastReadPerPartition;
        NMonitoring::TDynamicCounters::TCounterPtr MessageLagByCommittedPerPartition;
        NMonitoring::TDynamicCounters::TCounterPtr WriteTimeLagMsByLastReadPerPartition;
        NMonitoring::TDynamicCounters::TCounterPtr WriteTimeLagMsByCommittedPerPartition;
        NMonitoring::TDynamicCounters::TCounterPtr TimeSinceLastReadMsPerPartition;
        NMonitoring::TDynamicCounters::TCounterPtr ReadTimeLagMsPerPartition;
    };
    TVector<TPerPartitionCounters> PerPartitionCounters;

    ui32 ActiveReads;
    ui32 ReadsInQuotaQueue;
    ui32 Subscriptions;

    ui32 Partition;

    TVector<NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>> AvgReadBytes;

    NSlidingWindow::TSlidingWindow<NSlidingWindow::TMaxOperation<ui64>> WriteLagMs;

    std::shared_ptr<TPercentileCounter> ReadTimeLag;
    bool NoConsumer = false;

    bool DoInternalRead = false;
    bool MeterRead = true;


    bool Parsed = false;
};

class TUsersInfoStorage {
public:
    TUsersInfoStorage(
        TString dcId,
        const NPersQueue::TTopicConverterPtr& topicConverter,
        ui32 partition,
        const NKikimrPQ::TPQTabletConfig& config,
        const TString& CloudId,
        const TString& DbId,
        const TString& DbPath,
        const bool isServerless,
        const TString& FolderId);

    void Init(TActorId tabletActor, TActorId partitionActor, const TActorContext& ctx);

    void ParseDeprecated(const TString& key, const TString& data, const TActorContext& ctx);
    void Parse(const TString& key, const TString& data, const TActorContext& ctx);

    TUserInfo& GetOrCreate(const TString& user, const TActorContext& ctx, TMaybe<ui64> readRuleGeneration = {});
    const TUserInfo* GetIfExists(const TString& user) const;
    TUserInfo* GetIfExists(const TString& user);

    // iterate over a mutable list of UserInfo
    auto GetAll() {
        auto proj = [](auto& ni) -> std::pair<const TString&, TUserInfo&> { return {ni.first, *ni.second}; };
        return std::views::transform(UsersInfo, proj);
    }

    // iterate over a constant list of UserInfo
    auto ViewAll() const {
        auto proj = [](auto& ni) -> std::pair<const TString&, const TUserInfo&> { return {ni.first, *ni.second}; };
        return std::views::transform(UsersInfo, proj);
    }

    // iterate over a constant list of UserInfo with important flag or non-zero availability period
    auto ViewImportant() const {
        auto proj = [](auto& ni) -> std::pair<const TString&, const TUserInfo&> { return {ni.first, *ni.second}; };
        return std::views::transform(ImportantExtUsersInfoSlice, proj);
    }

    TUserInfoBase CreateUserInfo(const TString& user,
                             TMaybe<ui64> readRuleGeneration = {}) const;
    TUserInfo& Create(
        const TActorContext& ctx,
        const TString& user, const ui64 readRuleGeneration, bool important, TDuration availabilityPeriod, const TString& session,
        ui64 partitionSessionId, ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
        TInstant readFromTimestamp, const TActorId& pipeClient, bool anyCommits,
        const std::optional<TString>& committedMetadata = std::nullopt
    );

    void Clear(const TActorContext& ctx);

    void Remove(const TString& user, const TActorContext& ctx);

    struct TDetailedCounterSubgroup {
        ::NMonitoring::TDynamicCounterPtr Subgroup;
        TString Key;
    };

    void SetupDetailedMetrics(const TActorContext& ctx);
    void ResetDetailedMetrics();

    void SetImportant(TUserInfo& userInfo, bool important, TDuration availabilityPeriod);

private:

    TIntrusivePtr<TUserInfo> CreateUserInfo(const TActorContext& ctx,
                             const TString& user,
                             const ui64 readRuleGeneration,
                             bool important,
                             const TDuration availabilityPeriod,
                             const TString& session,
                             ui64 partitionSessionId,
                             ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
                             TInstant readFromTimestamp,
                             const TActorId& pipeClient,
                             bool anyCommits,
                             const std::optional<TString>& committedMetadata = std::nullopt) const;

    enum class EImportantSliceAction {
        Insert,
        Remove,
    };
    void UpdateImportantExtSlice(TUserInfo* userInfo, EImportantSliceAction action);

    TDetailedCounterSubgroup GetPartitionCounterSubgroupImpl(const TActorContext& ctx, const TString& monitoringProjectId) const;
    TDetailedCounterSubgroup GetPartitionCounterSubgroup(const TActorContext& ctx) const;
    TDetailedCounterSubgroup GetConsumerCounterSubgroup(const TActorContext& ctx, const NKikimrPQ::TPQTabletConfig_TConsumer& consumerConfig) const;
private:
    THashMap<TString, TIntrusivePtr<TUserInfo>> UsersInfo;
    THashMap<TString, TIntrusivePtr<TUserInfo>> ImportantExtUsersInfoSlice;

    const TString DCId;
    NPersQueue::TTopicConverterPtr TopicConverter;
    const ui32 Partition;
    NMonitoring::TDynamicCounterPtr StreamCountersSubgroup;

    TMaybe<TActorId> TabletActor;
    TMaybe<TActorId> PartitionActor;
    const NKikimrPQ::TPQTabletConfig& Config;

    TString CloudId;
    TString DbId;
    TString DbPath;
    bool IsServerless;
    TString FolderId;
    mutable ui64 CurReadRuleGeneration;
};


inline bool ImporantOrExtendedAvailabilityPeriod(const TUserInfoBase& userInfo) {
    return userInfo.Important || userInfo.AvailabilityPeriod > TDuration::Zero();
}

} //NPQ
} //NKikimr
