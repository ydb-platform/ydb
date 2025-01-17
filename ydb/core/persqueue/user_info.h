#pragma once

#include "working_time_counter.h"
#include "subscriber.h"
#include "percentile_counter.h"
#include "quota_tracker.h"
#include "account_read_quoter.h"
#include "metering_sink.h"
#include "dread_cache_service/caching_service.h"

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
static const TString CLIENTID_WITHOUT_CONSUMER = "$without_consumer";

typedef TProtobufTabletLabeledCounters<EClientLabeledCounters_descriptor> TUserLabeledCounters;

struct TUserInfoBase {
    TString User;
    ui64 ReadRuleGeneration = 0;

    TString Session = "";
    ui32 Generation = 0;
    ui32 Step = 0;
    i64 Offset = 0;

    bool Important = false;
    TInstant ReadFromTimestamp;

    ui64 PartitionSessionId = 0;
    TActorId PipeClient;
};

struct TUserInfo: public TUserInfoBase {
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

    bool HasReadRule = false;
    THolder<TUserLabeledCounters> LabeledCounters;
    NPersQueue::TTopicConverterPtr TopicConverter;

    TWorkingTimeCounter Counter;
    NKikimr::NPQ::TMultiCounter BytesRead;
    NKikimr::NPQ::TMultiCounter MsgsRead;
    NKikimr::NPQ::TMultiCounter BytesReadGrpc;
    NKikimr::NPQ::TMultiCounter MsgsReadGrpc;
    TMap<TString, NKikimr::NPQ::TMultiCounter> BytesReadFromDC;

    ui32 ActiveReads;
    ui32 ReadsInQuotaQueue;
    ui32 Subscriptions;
    i64 EndOffset;

    TVector<NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>> AvgReadBytes;

    NSlidingWindow::TSlidingWindow<NSlidingWindow::TMaxOperation<ui64>> WriteLagMs;

    std::shared_ptr<TPercentileCounter> ReadTimeLag;
    bool NoConsumer = false;

    bool DoInternalRead = false;
    bool MeterRead = true;

    bool Parsed = false;

    void ForgetSubscription(const TInstant& now) {
        if (Subscriptions > 0)
            --Subscriptions;
        UpdateReadingTimeAndState(now);
    }

    void UpdateReadingState() {
        Counter.UpdateState(Subscriptions > 0 || ActiveReads > 0 || ReadsInQuotaQueue > 0); //no data for read or got read requests from client
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
                  const TString& clientDC, const TActorId& tablet, bool isExternalRead) {
        Y_UNUSED(tablet);
        if (BytesRead && !clientDC.empty()) {
            BytesRead.Inc(readSize);
            if (!isExternalRead && BytesReadGrpc) {
                BytesReadGrpc.Inc(readSize);
            }

            if (MsgsRead) {
                MsgsRead.Inc(readCount);
                if (!isExternalRead && MsgsReadGrpc) {
                    MsgsReadGrpc.Inc(readCount);
                }
            }

            auto it = BytesReadFromDC.find(clientDC);
            if (it == BytesReadFromDC.end()) {
                auto pos = TopicConverter->GetFederationPath().find("/");
                if (pos != TString::npos) {
                    auto labels = NPersQueue::GetLabelsForCustomCluster(TopicConverter, clientDC);
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
        for (auto& avg : AvgReadBytes) {
            avg.Update(readSize, now);
        }
        Y_ABORT_UNLESS(ActiveReads > 0);
        --ActiveReads;
        UpdateReadingTimeAndState(now);
        ReadTimestamp = now;
    }

    TUserInfo(
        const TActorContext& ctx,
        NMonitoring::TDynamicCounterPtr streamCountersSubgroup,
        const TString& user,
        const ui64 readRuleGeneration, const bool important, const NPersQueue::TTopicConverterPtr& topicConverter,
        const ui32 partition, const TString& session, ui64 partitionSession, ui32 gen, ui32 step, i64 offset,
        const ui64 readOffsetRewindSum, const TString& dcId, TInstant readFromTimestamp,
        const TString& dbPath, bool meterRead, const TActorId& pipeClient
    )
        : TUserInfoBase{user, readRuleGeneration, session, gen, step, offset, important,
                        readFromTimestamp, partitionSession, pipeClient}
        , WriteTimestamp(TAppData::TimeProvider->Now())
        , CreateTimestamp(TAppData::TimeProvider->Now())
        , ReadTimestamp(TAppData::TimeProvider->Now())
        , ActualTimestamps(false)
        , ReadOffset(-1)
        , ReadWriteTimestamp(TAppData::TimeProvider->Now())
        , ReadCreateTimestamp(TAppData::TimeProvider->Now())
        , ReadOffsetRewindSum(readOffsetRewindSum)
        , ReadScheduled(false)
        , HasReadRule(false)
        , TopicConverter(topicConverter)
        , Counter(nullptr)
        , ActiveReads(0)
        , ReadsInQuotaQueue(0)
        , Subscriptions(0)
        , EndOffset(0)
        , AvgReadBytes{{TDuration::Seconds(1), 1000}, {TDuration::Minutes(1), 1000},
                       {TDuration::Hours(1), 2000}, {TDuration::Days(1), 2000}}
        , WriteLagMs(TDuration::Minutes(1), 100)
        , NoConsumer(user == CLIENTID_WITHOUT_CONSUMER)
        , MeterRead(meterRead)
    {
        if (AppData(ctx)->Counters) {
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

    void SetupStreamCounters(NMonitoring::TDynamicCounterPtr subgroup) {
        Y_ABORT_UNLESS(subgroup);
        TVector<std::pair<TString, TString>> subgroups;
        if (!NoConsumer) {
            subgroups.push_back({"consumer", User});
        }

        BytesRead = TMultiCounter(subgroup, {}, subgroups, {"topic.read.bytes"}, true, "name");
        MsgsRead = TMultiCounter(subgroup, {}, subgroups,{"topic.read.messages"}, true, "name");
        BytesReadGrpc = TMultiCounter(subgroup, {}, subgroups, {"api.grpc.topic.stream_read.bytes"}, true, "name");
        MsgsReadGrpc = TMultiCounter(subgroup, {}, subgroups, {"api.grpc.topic.stream_read.messages"}, true, "name");

        subgroups.emplace_back("name", "topic.read.lag_milliseconds");
        ReadTimeLag.reset(new TPercentileCounter(
                        subgroup, {}, subgroups, "bin",
                        TVector<std::pair<ui64, TString>>{{100, "100"}, {200, "200"}, {500, "500"},
                                                        {1000, "1000"}, {2000, "2000"},
                                                        {5000, "5000"}, {10'000, "10000"},
                                                        {30'000, "30000"}, {60'000, "60000"},
                                                        {180'000,"180000"}, {9'999'999, "999999"}},
                        true));
    }

    void SetupTopicCounters(const TActorContext& ctx, const TString& dcId, const TString& partition) {
        auto subgroup = [&](const TString& subsystem) {
            return GetServiceCounters(AppData(ctx)->Counters, subsystem);
        };
        auto aggr = NPersQueue::GetLabels(TopicConverter);
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

    void UpdateReadOffset(const i64 offset, TInstant writeTimestamp, TInstant createTimestamp, TInstant now, bool force = false) {
        ReadOffset = offset;
        ReadWriteTimestamp = writeTimestamp;
        ReadCreateTimestamp = createTimestamp;
        WriteLagMs.Update((ReadWriteTimestamp - ReadCreateTimestamp).MilliSeconds(), ReadWriteTimestamp);
        if (Subscriptions > 0 || force) {
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
        if (LabeledCounters && !AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
            LabeledCounters->SetGroup(User + "/" + (important ? "1" : "0") + "/" + TopicConverter->GetClientsideName());
        }
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
        const TActorContext& ctx, const TString& user, const ui64 readRuleGeneration, bool important, const TString& session,
        ui64 partitionSessionId, ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
        TInstant readFromTimestamp, const TActorId& pipeClient
    );

    void Clear(const TActorContext& ctx);

    void Remove(const TString& user, const TActorContext& ctx);

private:

    TUserInfo CreateUserInfo(const TActorContext& ctx,
                             const TString& user,
                             const ui64 readRuleGeneration,
                             bool important,
                             const TString& session,
                             ui64 partitionSessionId,
                             ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
                             TInstant readFromTimestamp, const TActorId& pipeClient) const;

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
