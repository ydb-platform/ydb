#pragma once

#include "working_time_counter.h"
#include "subscriber.h"
#include "percentile_counter.h"
#include "read_speed_limiter.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/sliding_window/sliding_window.h>

#include <util/generic/set.h>

namespace NKikimr {
namespace NPQ {

namespace NDeprecatedUserData {
    // [offset:64bit][generation:32bit][step:32bit][session:other bytes]
    TBuffer Serialize(ui64 offset, ui32 gen, ui32 step, const TString& session);
    void Parse(const TString& data, ui64& offset, ui32& gen, ui32& step, TString& session);
} // NDeprecatedUserInfo

static const ui32 MAX_USER_TS_CACHE_SIZE = 10'000;
static const ui64 MIN_TIMESTAMP_MS = 1'000'000'000'000ll; // around 2002 year
static const TString CLIENTID_WITHOUT_CONSUMER = "$without_consumer";

typedef TProtobufTabletLabeledCounters<EClientLabeledCounters_descriptor> TUserLabeledCounters;


class TQuotaTracker {

    class TAvgTracker {
    public:
        TAvgTracker(ui64 duration)
            : Duration(duration)
            , Sum(0)
        {
            Y_VERIFY(duration > 0);
        }

        void Update(i64 value, i64 ts)
        {
            Values.push_back(std::make_pair(value, ts));
            i64 newStart = ts - Duration;
            if (Values.size() > 1) {
                Sum += GetSum(Values.size() - 2);
                Y_VERIFY(Values.back().second >= Values.back().second);
            }
            while (Values.size() > 2 && newStart > Values[1].second) {
                Sum -= GetSum(0);
                Values.pop_front();
            }
        }

        ui64 GetAvg() {
             return (Values.size() > 1 && Values.back().second > Values.front().second)
                            ? Max<i64>(0, Sum / (Values.back().second - Values.front().second))
                            : 0;
        }

    private:

        i64 GetSum(ui32 pos) {
            Y_VERIFY(pos + 1 < Values.size());
            return (Values[pos + 1].first + Values[pos].first) * (Values[pos + 1].second - Values[pos].second) / 2;
        }

    private:
        ui64 Duration;
        i64 Sum;
        std::deque<std::pair<i64, i64>> Values;
    };


public:
    TQuotaTracker(const ui64 maxBurst, const ui64 speedPerSecond, const TInstant& timestamp)
        : AvailableSize(maxBurst)
        , SpeedPerSecond(speedPerSecond)
        , LastUpdateTime(timestamp)
        , MaxBurst(maxBurst)
        , AvgMin(60'000) //avg avail in bytes per sec for last minute
        , AvgSec(1000) //avg avail in bytes per sec
        , QuotedTime(0)
    {}

    ui64 GetQuotedTime() const {
        return QuotedTime;
    }

    void UpdateConfig(const ui64 maxBurst, const ui64 speedPerSecond)
    {
        SpeedPerSecond = speedPerSecond;
        MaxBurst = maxBurst;
        AvailableSize = maxBurst;
    }

    void Update(const TInstant& timestamp);

    bool CanExaust() const {
        return AvailableSize > 0;
    }

    void Exaust(const ui64 size, const TInstant& timestamp) {
        Update(timestamp);
        AvailableSize -= (i64)size;
        Update(timestamp);
    }

    ui64 GetAvailableAvgSec(const TInstant& timestamp) {
        Update(timestamp);
        return AvgSec.GetAvg();

    }

    ui64 GetAvailableAvgMin(const TInstant& timestamp) {
        Update(timestamp);
        return AvgMin.GetAvg();
    }

    ui64 GetTotalSpeed() const {
        return SpeedPerSecond;
    }

private:
    i64 AvailableSize;
    ui64 SpeedPerSecond;
    TInstant LastUpdateTime;
    ui64 MaxBurst;

    TAvgTracker AvgMin;
    TAvgTracker AvgSec;

    ui64 QuotedTime;
};

struct TReadSpeedLimiterHolder {
    TReadSpeedLimiterHolder(const TActorId& actor, const TTabletCountersBase& baseline)
        : Actor(actor)
    {
        Baseline.Populate(baseline);
    }

    TActorId Actor;
    TTabletCountersBase Baseline;
};

struct TUserInfo {
    THolder<TReadSpeedLimiterHolder> ReadSpeedLimiter;

    TString Session = "";
    ui32 Generation = 0;
    ui32 Step = 0;
    i64 Offset = 0;
    TInstant WriteTimestamp;
    TInstant CreateTimestamp;
    TInstant ReadTimestamp;
    bool ActualTimestamps = false;

    i64 ReadOffset = -1;
    TInstant ReadWriteTimestamp;
    TInstant ReadCreateTimestamp;
    ui64 ReadOffsetRewindSum = 0;

    bool ReadScheduled = false;

    //cache is used for storing WriteTime;CreateTime for offsets.
    //When client will commit to new position, timestamps for this offset could be in cache - not insane client should read data before commit
    std::deque<std::pair<ui64, std::pair<TInstant, TInstant>>> Cache;

    bool Important = false;
    TInstant ReadFromTimestamp;
    bool HasReadRule = false;
    TUserLabeledCounters LabeledCounters;
    TString User;
    ui64 ReadRuleGeneration = 0;
    TString Topic;

    std::deque<TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>> UserActs;

    std::deque<std::pair<TReadInfo, ui64>> ReadRequests;

    TQuotaTracker ReadQuota;

    TWorkingTimeCounter Counter;
    NKikimr::NPQ::TMultiCounter BytesRead;
    NKikimr::NPQ::TMultiCounter MsgsRead;
    TMap<TString, NKikimr::NPQ::TMultiCounter> BytesReadFromDC;

    ui32 ActiveReads;
    ui32 Subscriptions;
    i64 EndOffset;

    TVector<NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>> AvgReadBytes;

    NSlidingWindow::TSlidingWindow<NSlidingWindow::TMaxOperation<ui64>> WriteLagMs;

    std::shared_ptr<TPercentileCounter> ReadTimeLag;
    bool DoInternalRead = false;

    bool WriteInProgress = false;

    bool Parsed = false;

    void ForgetSubscription(const TInstant& now) {
        if (Subscriptions > 0)
            --Subscriptions;
        UpdateReadingTimeAndState(now);
    }

    void UpdateReadingState() {
        Counter.UpdateState(Subscriptions > 0 || ActiveReads > 0 || ReadRequests.size() > 0); //no data for read or got read requests from client
    }

    void UpdateReadingTimeAndState(TInstant now) {
        Counter.UpdateWorkingTime(now);
        UpdateReadingState();

        if (EndOffset == GetReadOffset()) { //no data to read, so emulate client empty reads
            WriteLagMs.Update(0, now);
        }
        if (Subscriptions > 0) {
            ReadTimestamp = now;
        }
    }

    void ReadDone(const TActorContext& ctx, const TInstant& now, ui64 readSize, ui32 readCount,
                  const TString& clientDC) {
        if (BytesRead && !clientDC.empty()) {
            if (BytesRead)
                BytesRead.Inc(readSize);
            if (MsgsRead)
                MsgsRead.Inc(readCount);
            auto it = BytesReadFromDC.find(clientDC);
            if (it == BytesReadFromDC.end()) {
                auto pos = Topic.find("--");
                if (pos != TString::npos) {
                    auto labels = GetLabels(clientDC, Topic.substr(pos + 2));
                    if (!labels.empty()) {
                        labels.pop_back();
                    }
                    it = BytesReadFromDC.emplace(clientDC,
                        TMultiCounter(GetServiceCounters(AppData(ctx)->Counters, "pqproxy|readSession"),
                                      labels, {{"ClientDC", clientDC},
                                               {"Client", User},
                                               {"ConsumerPath", NPersQueue::ConvertOldConsumerName(User, ctx)}},
                                      {"BytesReadFromDC"}, true)).first;
                }
            }
            if (it != BytesReadFromDC.end())
                it->second.Inc(readSize);
        }
        ReadQuota.Exaust(readSize, now);
        for (auto& avg : AvgReadBytes) {
            avg.Update(readSize, now);
        }
        Y_VERIFY(ActiveReads > 0);
        --ActiveReads;
        UpdateReadingTimeAndState(now);
        ReadTimestamp = now;
    }

    TUserInfo(
        const TActorContext& ctx, THolder<TReadSpeedLimiterHolder> readSpeedLimiter, const TString& user,
        const ui64 readRuleGeneration, const bool important, const TString& topic, const ui32 partition, const TString &session,
        ui32 gen, ui32 step, i64 offset, const ui64 readOffsetRewindSum, const TString& dcId,
        TInstant readFromTimestamp, const TString& cloudId, const TString& dbId, const TString& folderId,
        ui64 burst = 1'000'000'000, ui64 speed = 1'000'000'000, const TString& streamName = "undefined"
    )
        : ReadSpeedLimiter(std::move(readSpeedLimiter))
        , Session(session)
        , Generation(gen)
        , Step(step)
        , Offset(offset)
        , WriteTimestamp(TAppData::TimeProvider->Now())
        , CreateTimestamp(TAppData::TimeProvider->Now())
        , ReadTimestamp(TAppData::TimeProvider->Now())
        , ActualTimestamps(false)
        , ReadOffset(-1)
        , ReadWriteTimestamp(TAppData::TimeProvider->Now())
        , ReadCreateTimestamp(TAppData::TimeProvider->Now())
        , ReadOffsetRewindSum(readOffsetRewindSum)
        , ReadScheduled(false)
        , Important(important)
        , ReadFromTimestamp(readFromTimestamp)
        , HasReadRule(false)
        , LabeledCounters(user + "/" +(important ? "1" : "0") + "/" + topic, partition)
        , User(user)
        , ReadRuleGeneration(readRuleGeneration)
        , Topic(topic)
        , ReadQuota(burst, speed, TAppData::TimeProvider->Now())
        , Counter(nullptr)
        , ActiveReads(0)
        , Subscriptions(0)
        , EndOffset(0)
        , AvgReadBytes{{TDuration::Seconds(1), 1000}, {TDuration::Minutes(1), 1000},
                       {TDuration::Hours(1), 2000}, {TDuration::Days(1), 2000}}
        , WriteLagMs(TDuration::Minutes(1), 100)
        , DoInternalRead(user != CLIENTID_WITHOUT_CONSUMER)
    {
        if (AppData(ctx)->Counters) {
            if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
                if (DoInternalRead) {
                    SetupStreamCounters(ctx, dcId, ToString<ui32>(partition), streamName, cloudId, dbId, folderId);
                }
            } else {
                if (topic.find("--") == TString::npos)
                    return;
                SetupTopicCounters(ctx, dcId, ToString<ui32>(partition), topic);
            }
        }
    }


    void SetupStreamCounters(
            const TActorContext& ctx, const TString& dcId, const TString& partition,
            const TString& topic, const TString& cloudId, const TString& dbId, const TString& folderId
    ) {
        auto subgroup = GetCountersForStream(AppData(ctx)->Counters);
        auto aggregates = GetLabelsForStream(topic, cloudId, dbId, folderId);

        BytesRead = TMultiCounter(subgroup, aggregates, {{"consumer", User}},
                                  {"stream.internal_read.bytes_per_second",
                                   "stream.outgoing_bytes_per_second"}, true, "name");
        MsgsRead = TMultiCounter(subgroup, aggregates, {{"consumer", User}},
                                 {"stream.internal_read.records_per_second",
                                  "stream.outgoing_records_per_second"}, true, "name");

        Counter.SetCounter(subgroup,
                           {{"cloud", cloudId}, {"folder", folderId}, {"database", dbId}, {"stream", topic},
                            {"consumer", User}, {"host", dcId}, {"shard", partition}},
                           {"name", "stream.await_operating_milliseconds", true});

        ReadTimeLag.reset(new TPercentileCounter(
                     GetCountersForStream(AppData(ctx)->Counters), aggregates,
                     {{"consumer", User}, {"name", "stream.internal_read.time_lags_milliseconds"}}, "bin",
                     TVector<std::pair<ui64, TString>>{{100, "100"}, {200, "200"}, {500, "500"},
                                                        {1000, "1000"}, {2000, "2000"},
                                                        {5000, "5000"}, {10'000, "10000"},
                                                        {30'000, "30000"}, {60'000, "60000"},
                                                        {180'000,"180000"}, {9'999'999, "999999"}},
                        true));
    }

    void SetupTopicCounters(const TActorContext& ctx, const TString& dcId, const TString& partition,
                            const TString& topic) {
        auto subgroup = [&](const TString& subsystem) {
            return GetServiceCounters(AppData(ctx)->Counters, subsystem);
        };
        const TVector<NPQ::TLabelsInfo> aggr = NKikimr::NPQ::GetLabels(topic);
        TVector<std::pair<TString, TString>> additional_labels = {{"Client", User},
                                 {"ConsumerPath", NPersQueue::ConvertOldConsumerName(User, ctx)}
                                };

        Counter.SetCounter(subgroup("readingTime"),
                           {{"Client", User},
                            {"ConsumerPath", NPersQueue::ConvertOldConsumerName(User, ctx)},
                            {"host", dcId},
                            {"Partition", partition}},
                           {"sensor", "ReadTime", true});

        BytesRead = TMultiCounter(subgroup("pqproxy|readSession"), aggr, additional_labels,
                                  {"BytesRead"}, true);
        MsgsRead = TMultiCounter(subgroup("pqproxy|readSession"), aggr, additional_labels,
                                 {"MessagesRead"}, true);

        additional_labels.push_back({"sensor", "TimeLags"});
        ReadTimeLag.reset(new TPercentileCounter(subgroup("pqproxy|readTimeLag"), aggr,
                      additional_labels, "Interval",
                      TVector<std::pair<ui64, TString>>{{100, "100ms"}, {200, "200ms"}, {500, "500ms"},
                                                        {1000, "1000ms"}, {2000, "2000ms"},
                                                        {5000, "5000ms"}, {10'000, "10000ms"},
                                                        {30'000, "30000ms"}, {60'000, "60000ms"},
                                                        {180'000,"180000ms"}, {9'999'999, "999999ms"}},
                        true));

    }

    void SetQuota(const ui64 maxBurst, const ui64 speed) {
        ReadQuota.UpdateConfig(maxBurst, speed);
    }

    void Clear(const TActorContext& ctx);

    void UpdateReadOffset(const i64 offset, TInstant writeTimestamp, TInstant createTimestamp, TInstant now) {
        ReadOffset = offset;
        ReadWriteTimestamp = writeTimestamp;
        ReadCreateTimestamp = createTimestamp;
        WriteLagMs.Update((ReadWriteTimestamp - ReadCreateTimestamp).MilliSeconds(), ReadWriteTimestamp);
        if (Subscriptions > 0) {
            ReadTimestamp = now;
        }
    }

    void AddTimestampToCache(const ui64 offset, TInstant writeTimestamp, TInstant createTimestamp, bool isUserRead, TInstant now)
    {
        if ((ui64)Max<i64>(Offset, 0) == offset) {
            WriteTimestamp = writeTimestamp;
            CreateTimestamp = createTimestamp;
            ActualTimestamps = true;
            if (ReadOffset == -1) {
                UpdateReadOffset(offset, writeTimestamp, createTimestamp, now);
            }
        }
        if (isUserRead) {
            UpdateReadOffset(offset, writeTimestamp, createTimestamp, now);
            if (ReadTimeLag) {
                ReadTimeLag->IncFor((now - createTimestamp).MilliSeconds(), 1);
            }
        }
        if (!Cache.empty() && Cache.back().first >= offset) //already got data in cache
            return;
        Cache.push_back(std::make_pair(offset, std::make_pair(writeTimestamp, createTimestamp)));
        if (Cache.size() > MAX_USER_TS_CACHE_SIZE)
            Cache.pop_front();
    }

    bool UpdateTimestampFromCache()
    {
        while (!Cache.empty() && (i64)Cache.front().first < Offset) {
            Cache.pop_front();
        }
        if (!Cache.empty() && Cache.front().first == (ui64)Max<i64>(Offset, 0)) {
            WriteTimestamp = Cache.front().second.first;
            CreateTimestamp = Cache.front().second.second;
            ActualTimestamps = true;
            if (ReadOffset == -1) {
                UpdateReadOffset(Offset - 1, Cache.front().second.first, Cache.front().second.second, TAppData::TimeProvider->Now());
            }
            return true;
        }
        return false;
    }

    void SetImportant(bool important)
    {
        Important = important;
        LabeledCounters.SetGroup(User + "/" + (important ? "1" : "0") + "/" + Topic);
    }

    i64 GetReadOffset() const {
        return ReadOffset == -1 ? Offset : (ReadOffset + 1); //+1 because we want to track first not readed offset
    }

    TInstant GetReadTimestamp() const {
        return ReadTimestamp;
    }

    TInstant GetWriteTimestamp() const {
        return Offset == EndOffset ? TAppData::TimeProvider->Now() : WriteTimestamp;
    }

    TInstant GetCreateTimestamp() const {
        return Offset == EndOffset ? TAppData::TimeProvider->Now() : CreateTimestamp;
    }

    TInstant GetReadWriteTimestamp() const {
        TInstant ts =  ReadOffset == -1 ? WriteTimestamp : ReadWriteTimestamp;
        ts = GetReadOffset() >= EndOffset ? TAppData::TimeProvider->Now() : ts;
        return ts;
    }

    ui64 GetWriteLagMs() const {
        return WriteLagMs.GetValue();
    }

    TInstant GetReadCreateTimestamp() const {
        TInstant ts = ReadOffset == -1 ? CreateTimestamp : ReadCreateTimestamp;
        ts = GetReadOffset() >= EndOffset ? TAppData::TimeProvider->Now() : ts;
        return ts;
    }

};

class TUsersInfoStorage {
public:
    TUsersInfoStorage(TString dcId, ui64 tabletId, const TString& topicName, ui32 partition,
                      const TTabletCountersBase& counters, const NKikimrPQ::TPQTabletConfig& config,
                      const TString& CloudId, const TString& DbId, const TString& FolderId,
                      const TString& streamName);

    void Init(TActorId tabletActor, TActorId partitionActor);

    void ParseDeprecated(const TString& key, const TString& data, const TActorContext& ctx);
    void Parse(const TString& key, const TString& data, const TActorContext& ctx);

    TUserInfo& GetOrCreate(const TString& user, const TActorContext& ctx, TMaybe<ui64> readRuleGeneration = {});
    TUserInfo* GetIfExists(const TString& user);

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) {
        Config = config;
    }

    THashMap<TString, TUserInfo>& GetAll();

    TUserInfo& Create(
        const TActorContext& ctx, const TString& user, const ui64 readRuleGeneration, bool important, const TString &session,
        ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum, TInstant readFromTimestamp
    );

    void Clear(const TActorContext& ctx);

    void Remove(const TString& user, const TActorContext& ctx);

private:
    THolder<TReadSpeedLimiterHolder> CreateReadSpeedLimiter(const TString& user) const;

private:
    THashMap<TString, TUserInfo> UsersInfo;

    const TString DCId;
    ui64 TabletId;
    const TString TopicName;
    const ui32 Partition;
    TTabletCountersBase Counters;

    TMaybe<TActorId> TabletActor;
    TMaybe<TActorId> PartitionActor;
    NKikimrPQ::TPQTabletConfig Config;

    TString CloudId;
    TString DbId;
    TString FolderId;
    TString StreamName;
    ui64 CurReadRuleGeneration;
};

} //NPQ
} //NKikimr
