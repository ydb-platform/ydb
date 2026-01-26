#pragma once

#include "account_read_quoter.h"

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

typedef TProtobufTabletLabeledCounters<EClientLabeledCounters_descriptor> TUserLabeledCounters;

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
    TInstant ReadFromTimestamp;

    ui64 PartitionSessionId = 0;
    TActorId PipeClient;

    std::optional<TString> CommittedMetadata = std::nullopt;
};

struct TUserInfo: public TUserInfoBase {
    TUserInfo(
        const TActorContext& ctx,
        NMonitoring::TDynamicCounterPtr streamCountersSubgroup,
        NMonitoring::TDynamicCounterPtr partitionCountersSubgroup,
        const TString& user,
        const ui64 readRuleGeneration, const bool important, const NPersQueue::TTopicConverterPtr& topicConverter,
        const ui32 partition, const TString& session, ui64 partitionSession, ui32 gen, ui32 step, i64 offset,
        const ui64 readOffsetRewindSum, const TString& dcId, TInstant readFromTimestamp,
        const TString& dbPath, bool meterRead, const TActorId& pipeClient, bool anyCommits,
        const std::optional<TString>& committedMetadata = std::nullopt
    )
        : TUserInfoBase{user, readRuleGeneration, session, gen, step, offset, anyCommits, important,
                        readFromTimestamp, partitionSession, pipeClient, committedMetadata}
        , ActualTimestamps(false)
        , WriteTimestamp(TInstant::Zero())
        , CreateTimestamp(TInstant::Zero())
        , ReadTimestamp(TAppData::TimeProvider->Now())
        , ReadOffset(-1)
        , ReadWriteTimestamp(TInstant::Zero())
        , ReadCreateTimestamp(TInstant::Zero())
        , ReadOffsetRewindSum(readOffsetRewindSum)
        , ReadScheduled(false)
        , HasReadRule(false)
        , TopicConverter(topicConverter)
        , Counter(nullptr)
        , ActiveReads(0)
        , ReadsInQuotaQueue(0)
        , Subscriptions(0)
        , Partition(partition)
        , AvgReadBytes{{TDuration::Seconds(1), 1000}, {TDuration::Minutes(1), 1000},
                       {TDuration::Hours(1), 2000}, {TDuration::Days(1), 2000}}
        , WriteLagMs(TDuration::Minutes(1), 100)
        , NoConsumer(user == CLIENTID_WITHOUT_CONSUMER)
        , MeterRead(meterRead)
    {
        if (AppData(ctx)->Counters) {
            if (partitionCountersSubgroup) {
                SetupDetailedMetrics(ctx, partitionCountersSubgroup);
            }

            if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
                LabeledCounters.Reset(new TUserLabeledCounters(
                    EscapeBadChars(user) + "||" + EscapeBadChars(topicConverter->GetClientsideName()), partition, dbPath));

                SetupStreamCounters(streamCountersSubgroup);
            } else {
                LabeledCounters.Reset(new TUserLabeledCounters(
                    user + "/" + (important ? "1" : "0") + "/" + topicConverter->GetClientsideName(),
                    partition));

                SetupTopicCounters(ctx, dcId, ToString<ui32>(partition));
            }
        }
    }

    void ForgetSubscription(i64 endOffset, const TInstant& now);
    void UpdateReadingState();
    void UpdateReadingTimeAndState(i64 endOffset, TInstant now);
    void ReadDone(const TActorContext& ctx, const TInstant& now, ui64 readSize, ui32 readCount,
                  const TString& clientDC, const TActorId& tablet, bool isExternalRead, i64 endOffset);

    void SetupDetailedMetrics(const TActorContext& ctx, NMonitoring::TDynamicCounterPtr subgroup);
    void ResetDetailedMetrics();
    void SetupStreamCounters(NMonitoring::TDynamicCounterPtr subgroup);
    void SetupTopicCounters(const TActorContext& ctx, const TString& dcId, const TString& partition);

    void UpdateReadOffset(const i64 offset, TInstant writeTimestamp, TInstant createTimestamp, TInstant now, bool force = false);
    void AddTimestampToCache(const ui64 offset, TInstant writeTimestamp, TInstant createTimestamp, bool isUserRead, TInstant now);
    bool UpdateTimestampFromCache();
    void SetImportant(bool important);

    i64 GetReadOffset() const {
        return ReadOffset == -1 ? Offset : (ReadOffset + 1); //+1 because we want to track first not readed offset
    }
    TInstant GetReadTimestamp() const {
        return ReadTimestamp;
    }
    ui64 GetWriteLagMs() const {
        return WriteLagMs.GetValue();
    }

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
    THolder<TUserLabeledCounters> LabeledCounters;
    NPersQueue::TTopicConverterPtr TopicConverter;

    TWorkingTimeCounter Counter;
    NKikimr::NPQ::TMultiCounter BytesRead;
    NKikimr::NPQ::TMultiCounter MsgsRead;
    NKikimr::NPQ::TMultiCounter BytesReadGrpc;
    NKikimr::NPQ::TMultiCounter MsgsReadGrpc;
    TMap<TString, NKikimr::NPQ::TMultiCounter> BytesReadFromDC;

    // Per partition counters
    NMonitoring::TDynamicCounters::TCounterPtr BytesReadPerPartition;
    NMonitoring::TDynamicCounters::TCounterPtr MessagesReadPerPartition;
    NMonitoring::TDynamicCounters::TCounterPtr MessageLagByLastReadPerPartition;
    NMonitoring::TDynamicCounters::TCounterPtr MessageLagByCommittedPerPartition;
    NMonitoring::TDynamicCounters::TCounterPtr WriteTimeLagMsByLastReadPerPartition;
    NMonitoring::TDynamicCounters::TCounterPtr WriteTimeLagMsByCommittedPerPartition;
    NMonitoring::TDynamicCounters::TCounterPtr TimeSinceLastReadMsPerPartition;
    NMonitoring::TDynamicCounters::TCounterPtr ReadTimeLagMsPerPartition;

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

    THashMap<TString, TUserInfo>& GetAll();

    TUserInfoBase CreateUserInfo(const TString& user,
                             TMaybe<ui64> readRuleGeneration = {}) const;
    TUserInfo& Create(
        const TActorContext& ctx,
        const TString& user, const ui64 readRuleGeneration, bool important, const TString& session,
        ui64 partitionSessionId, ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
        TInstant readFromTimestamp, const TActorId& pipeClient, bool anyCommits,
        const std::optional<TString>& committedMetadata = std::nullopt
    );

    void Clear(const TActorContext& ctx);

    void Remove(const TString& user, const TActorContext& ctx);

    ::NMonitoring::TDynamicCounterPtr GetPartitionCounterSubgroup(const TActorContext& ctx) const;
    void SetupDetailedMetrics(const TActorContext& ctx);
    void ResetDetailedMetrics();
    bool DetailedMetricsAreEnabled() const;

private:

    TUserInfo CreateUserInfo(const TActorContext& ctx,
                             const TString& user,
                             const ui64 readRuleGeneration,
                             bool important,
                             const TString& session,
                             ui64 partitionSessionId,
                             ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
                             TInstant readFromTimestamp,
                             const TActorId& pipeClient,
                             bool anyCommits,
                             const std::optional<TString>& committedMetadata = std::nullopt) const;

private:
    THashMap<TString, TUserInfo> UsersInfo;

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

} //NPQ
} //NKikimr
