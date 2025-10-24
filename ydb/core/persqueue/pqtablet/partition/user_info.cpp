#include "user_info.h"
#include <ydb/core/persqueue/pqtablet/common/constants.h>

namespace NKikimr {
namespace NPQ {

TString EscapeBadChars(const TString& str) {
    TStringBuilder res;
    for (ui32 i = 0; i < str.size();++i) {
        if (str[i] == '|') res << '/';
        else res << str[i];
    }
    return res;
}

namespace NDeprecatedUserData {
    TBuffer Serialize(ui64 offset, ui32 gen, ui32 step, const TString& session) {
        TBuffer data;
        data.Resize(sizeof(ui64) + sizeof(ui32) * 2 + session.size());
        memcpy(data.Data(), &offset, sizeof(ui64));
        memcpy(data.Data() + sizeof(ui64), &gen, sizeof(ui32));
        memcpy(data.Data() + sizeof(ui64) + sizeof(ui32), &step, sizeof(ui32));
        memcpy(data.Data() + sizeof(ui64) + 2 * sizeof(ui32), session.data(), session.size());
        return data;
    }

    void Parse(const TString& data, ui64& offset, ui32& gen, ui32& step, TString& session) {
        AFL_ENSURE(sizeof(ui64) <= data.size());

        offset = *reinterpret_cast<const ui64*>(data.c_str());
        gen = 0;
        step = 0;
        if (data.size() > sizeof(ui64)) {
            gen = reinterpret_cast<const ui32*>(data.c_str() + sizeof(ui64))[0];
            step = reinterpret_cast<const ui32*>(data.c_str() + sizeof(ui64))[1];
            session = data.substr(sizeof(ui64) + 2 * sizeof(ui32));
        }
    }
} // NDeprecatedUserData

TUserInfo::TUserInfo(
    const TActorContext& ctx,
    NMonitoring::TDynamicCounterPtr streamCountersSubgroup,
    NMonitoring::TDynamicCounterPtr partitionCountersSubgroup,
    const TString& user,
    const ui64 readRuleGeneration, const bool important, const TDuration availabilityPeriod,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    const ui32 partition, const TString& session, ui64 partitionSession, ui32 gen, ui32 step, i64 offset,
    const ui64 readOffsetRewindSum, const TString& dcId, TInstant readFromTimestamp,
    const TString& dbPath, bool meterRead, const TActorId& pipeClient, bool anyCommits,
    const std::optional<TString>& committedMetadata
)
    : TUserInfoBase{user, readRuleGeneration, session, gen, step, offset, anyCommits, important, availabilityPeriod,
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
                user + "/" + (ImporantOrExtendedAvailabilityPeriod(*this) ? "1" : "0") + "/" + topicConverter->GetClientsideName(),
                partition));

            SetupTopicCounters(ctx, dcId, ToString<ui32>(partition));
        }
    }
}

void TUserInfo::ForgetSubscription(i64 endOffset, const TInstant& now) {
    if (Subscriptions > 0)
        --Subscriptions;
    UpdateReadingTimeAndState(endOffset, now);
}

void TUserInfo::UpdateReadingState() {
    Counter.UpdateState(Subscriptions > 0 || ActiveReads > 0 || ReadsInQuotaQueue > 0); //no data for read or got read requests from client
}

void TUserInfo::UpdateReadingTimeAndState(i64 endOffset, TInstant now) {
    Counter.UpdateWorkingTime(now);
    UpdateReadingState();

    if (endOffset == GetReadOffset()) { //no data to read, so emulate client empty reads
        WriteLagMs.Update(0, now);
    }
    if (Subscriptions > 0) {
        ReadTimestamp = now;
    }
}

void TUserInfo::ReadDone(const TActorContext& ctx, const TInstant& now, ui64 readSize, ui32 readCount,
                const TString& clientDC, const TActorId& tablet, bool isExternalRead, i64 endOffset) {
    Y_UNUSED(tablet);
    if (BytesReadPerPartition) {
        BytesReadPerPartition->Add(readSize);
    }
    if (MessagesReadPerPartition) {
        MessagesReadPerPartition->Add(readCount);
    }
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
    AFL_ENSURE(ActiveReads > 0);
    --ActiveReads;
    UpdateReadingTimeAndState(endOffset, now);
    ReadTimestamp = now;
}

void TUserInfo::SetupDetailedMetrics(const TActorContext& ctx, NMonitoring::TDynamicCounterPtr subgroup) {
    Y_ABORT_UNLESS(subgroup);

    if (BytesReadPerPartition) {
        // Don't recreate the counters if they already exist.
        return;
    }

    bool fcc = AppData()->PQConfig.GetTopicsAreFirstClassCitizen();

    auto consumerSubgroup = fcc
        ? subgroup->GetSubgroup("consumer", User)
        : subgroup->GetSubgroup("ConsumerPath", NPersQueue::ConvertOldConsumerName(User, ctx));

    auto getCounter = [&](const TString& forFCC, const TString& forFederation, bool deriv) {
        return consumerSubgroup->GetExpiringNamedCounter(
            fcc ? "name" : "sensor",
            fcc ? "topic.partition." + forFCC : forFederation + "PerPartition",
            deriv);
    };

    BytesReadPerPartition = getCounter("read.bytes", "BytesRead", true);
    MessagesReadPerPartition = getCounter("read.messages", "MessagesRead", true);
    MessageLagByLastReadPerPartition = getCounter("read.lag_messages", "MessageLagByLastRead", false);
    MessageLagByCommittedPerPartition = getCounter("committed_lag_messages", "MessageLagByCommitted", false);
    WriteTimeLagMsByLastReadPerPartition = getCounter("write.lag_milliseconds", "WriteTimeLagMsByLastRead", false);
    WriteTimeLagMsByCommittedPerPartition = getCounter("committed_read_lag_milliseconds", "WriteTimeLagMsByCommitted", false);
    TimeSinceLastReadMsPerPartition = getCounter("read.idle_milliseconds", "TimeSinceLastReadMs", false);
    ReadTimeLagMsPerPartition = getCounter("read.lag_milliseconds", "ReadTimeLagMs", false);
}

void TUserInfo::ResetDetailedMetrics() {
    BytesReadPerPartition.Reset();
    MessagesReadPerPartition.Reset();
    MessageLagByLastReadPerPartition.Reset();
    MessageLagByCommittedPerPartition.Reset();
    WriteTimeLagMsByLastReadPerPartition.Reset();
    WriteTimeLagMsByCommittedPerPartition.Reset();
    TimeSinceLastReadMsPerPartition.Reset();
    ReadTimeLagMsPerPartition.Reset();
}

void TUserInfo::SetupStreamCounters(NMonitoring::TDynamicCounterPtr subgroup) {
    AFL_ENSURE(subgroup);
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

void TUserInfo::SetupTopicCounters(const TActorContext& ctx, const TString& dcId, const TString& partition) {
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

void TUserInfo::UpdateReadOffset(const i64 offset, TInstant writeTimestamp, TInstant createTimestamp, TInstant now, bool force) {
    ReadOffset = offset;
    ReadWriteTimestamp = writeTimestamp;
    ReadCreateTimestamp = createTimestamp;
    WriteLagMs.Update((ReadWriteTimestamp - ReadCreateTimestamp).MilliSeconds(), ReadWriteTimestamp);
    if (Subscriptions > 0 || force) {
        ReadTimestamp = now;
    }
}

void TUserInfo::AddTimestampToCache(const ui64 offset, TInstant writeTimestamp, TInstant createTimestamp, bool isUserRead, TInstant now) {
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

bool TUserInfo::UpdateTimestampFromCache() {
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

void TUserInfo::SetImportant(bool important, TDuration availabilityPeriod) {
    Important = important;
    AvailabilityPeriod = availabilityPeriod;
    if (LabeledCounters && !AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
        LabeledCounters->SetGroup(User + "/" + (ImporantOrExtendedAvailabilityPeriod(*this) ? "1" : "0") + "/" + TopicConverter->GetClientsideName());
    }
}

TUsersInfoStorage::TUsersInfoStorage(
    TString dcId,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    ui32 partition,
    const NKikimrPQ::TPQTabletConfig& config,
    const TString& cloudId,
    const TString& dbId,
    const TString& dbPath,
    const bool isServerless,
    const TString& folderId,
    const TString& monitoringProjectId
)
    : DCId(std::move(dcId))
    , TopicConverter(topicConverter)
    , Partition(partition)
    , Config(config)
    , CloudId(cloudId)
    , DbId(dbId)
    , DbPath(dbPath)
    , IsServerless(isServerless)
    , FolderId(folderId)
    , MonitoringProjectId(monitoringProjectId)
    , CurReadRuleGeneration(0)
{
}

void TUsersInfoStorage::Init(TActorId tabletActor, TActorId partitionActor, const TActorContext& ctx) {
    AFL_ENSURE(UsersInfo.empty());
    AFL_ENSURE(!TabletActor);
    AFL_ENSURE(!PartitionActor);
    TabletActor = tabletActor;
    PartitionActor = partitionActor;

    if (AppData(ctx)->Counters && AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
        StreamCountersSubgroup = NPersQueue::GetCountersForTopic(AppData(ctx)->Counters, IsServerless);
        auto subgroups = NPersQueue::GetSubgroupsForTopic(TopicConverter, CloudId, DbId, DbPath, FolderId);
        for (auto& group : subgroups) {
            StreamCountersSubgroup = StreamCountersSubgroup->GetSubgroup(group.first, group.second);
        }
    }
}

void TUsersInfoStorage::ParseDeprecated(const TString& key, const TString& data, const TActorContext& ctx) {
    AFL_ENSURE(key.size() >= TKeyPrefix::MarkedSize());
    AFL_ENSURE(key[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkUserDeprecated);
    TString user = key.substr(TKeyPrefix::MarkedSize());

    TUserInfo* userInfo = GetIfExists(user);
    if (userInfo && userInfo->Parsed) {
        return;
    }

    ui64 offset = 0;
    ui32 gen = 0;
    ui32 step = 0;
    TString session;
    NDeprecatedUserData::Parse(data, offset, gen, step, session);
    AFL_ENSURE(offset <= (ui64)Max<i64>())("description", "Offset is too big")("offset", offset);

    if (!userInfo) {
        Create(ctx, user, 0, false, TDuration::Zero(), session, 0, gen, step, static_cast<i64>(offset), 0, TInstant::Zero(), {}, false);
    } else {
        userInfo->Session = session;
        userInfo->Generation = gen;
        userInfo->Step = step;
        userInfo->Offset = static_cast<i64>(offset);
    }
}

void TUsersInfoStorage::Parse(const TString& key, const TString& data, const TActorContext& ctx) {
    AFL_ENSURE(key.size() >= TKeyPrefix::MarkedSize());
    AFL_ENSURE(key[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkUser);
    TString user = key.substr(TKeyPrefix::MarkedSize());

    AFL_ENSURE(sizeof(ui64) <= data.size());

    NKikimrPQ::TUserInfo userData;
    bool res = userData.ParseFromString(data);
    AFL_ENSURE(res);

    AFL_ENSURE(userData.GetOffset() <= (ui64)Max<i64>())("description", "Offset is too big")("offset", userData.GetOffset());
    i64 offset = static_cast<i64>(userData.GetOffset());

    TUserInfo* userInfo = GetIfExists(user);
    if (!userInfo) {
        Create(
            ctx, user, userData.GetReadRuleGeneration(), false, TDuration::Zero(), userData.GetSession(), userData.GetPartitionSessionId(),
            userData.GetGeneration(), userData.GetStep(), offset,
            userData.GetOffsetRewindSum(), TInstant::Zero(),  {}, userData.GetAnyCommits(),
            userData.HasCommittedMetadata() ? static_cast<std::optional<TString>>(userData.GetCommittedMetadata()) : std::nullopt
        );
    } else {
        userInfo->Session = userData.GetSession();
        userInfo->Generation = userData.GetGeneration();
        userInfo->Step = userData.GetStep();
        userInfo->Offset = offset;
        userInfo->ReadOffsetRewindSum = userData.GetOffsetRewindSum();
        userInfo->ReadRuleGeneration = userData.GetReadRuleGeneration();
    }
    userInfo = GetIfExists(user);
    AFL_ENSURE(userInfo);
    userInfo->Parsed = true;
}

void TUsersInfoStorage::Remove(const TString& user, const TActorContext&) {
    auto it = UsersInfo.find(user);
    AFL_ENSURE(it != UsersInfo.end());
    if (ImporantOrExtendedAvailabilityPeriod(*it->second)) {
        UpdateImportantExtSlice(it->second.Get(), EImportantSliceAction::Remove);
    } else {
        AFL_ENSURE(!ImportantExtUsersInfoSlice.contains(user))("user", user);
    }
    UsersInfo.erase(it);
}

TUserInfo& TUsersInfoStorage::GetOrCreate(const TString& user, const TActorContext& ctx, TMaybe<ui64> readRuleGeneration) {
    auto it = UsersInfo.find(user.empty() ? CLIENTID_WITHOUT_CONSUMER : user);
    if (it == UsersInfo.end()) {
        return Create(
                ctx, user, readRuleGeneration ? *readRuleGeneration : ++CurReadRuleGeneration, false, TDuration::Zero(), "", 0,
                0, 0, 0, 0, TInstant::Zero(), {}, false
        );
    }
    return *it->second;
}

::NMonitoring::TDynamicCounterPtr TUsersInfoStorage::GetPartitionCounterSubgroup(const TActorContext& ctx) const {
    if (!DetailedMetricsAreEnabled()) {
        return nullptr;
    }
    auto counters = AppData(ctx)->Counters;
    if (!counters) {
        return nullptr;
    }
    if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
        auto s = counters
            ->GetSubgroup("counters", IsServerless ? "topics_per_partition_serverless" : "topics_per_partition")
            ->GetSubgroup("host", "");
        if (!MonitoringProjectId.empty()) {
            s = s->GetSubgroup("monitoring_project_id", MonitoringProjectId);
        }
        return s
            ->GetSubgroup("database", Config.GetYdbDatabasePath())
            ->GetSubgroup("cloud_id", CloudId)
            ->GetSubgroup("folder_id", FolderId)
            ->GetSubgroup("database_id", DbId)
            ->GetSubgroup("topic", TopicConverter->GetClientsideName())
            ->GetSubgroup("partition_id", ToString(Partition));
    } else {
        auto s = counters
            ->GetSubgroup("counters", "topics_per_partition")
            ->GetSubgroup("host", "cluster");
        if (!MonitoringProjectId.empty()) {
            s = s->GetSubgroup("monitoring_project_id", MonitoringProjectId);
        }
        return s
            ->GetSubgroup("Account", TopicConverter->GetAccount())
            ->GetSubgroup("TopicPath", TopicConverter->GetFederationPath())
            ->GetSubgroup("OriginDC", TopicConverter->GetCluster())
            ->GetSubgroup("Partition", ToString(Partition));
    }
}


void TUsersInfoStorage::SetupDetailedMetrics(const TActorContext& ctx) {
    auto subgroup = GetPartitionCounterSubgroup(ctx);
    if (!subgroup) {
        return;  // TODO(qyryq) Y_ABORT_UNLESS?
    }
    for (auto&& userInfo : GetAll()) {
        userInfo.second.SetupDetailedMetrics(ctx, subgroup);
    }
}

void TUsersInfoStorage::ResetDetailedMetrics() {
    for (auto&& userInfo : GetAll()) {
        userInfo.second.ResetDetailedMetrics();
    }
}

bool TUsersInfoStorage::DetailedMetricsAreEnabled() const {
    return AppData()->FeatureFlags.GetEnableMetricsLevel() && (Config.HasMetricsLevel() && Config.GetMetricsLevel() == METRICS_LEVEL_DETAILED);
}

const TUserInfo* TUsersInfoStorage::GetIfExists(const TString& user) const {
    auto it = UsersInfo.find(user);
    return it != UsersInfo.end() ? &*it->second : nullptr;
}

TUserInfo* TUsersInfoStorage::GetIfExists(const TString& user) {
    auto it = UsersInfo.find(user);
    return it != UsersInfo.end() ? &*it->second : nullptr;
}

TIntrusivePtr<TUserInfo> TUsersInfoStorage::CreateUserInfo(const TActorContext& ctx,
                                            const TString& user,
                                            const ui64 readRuleGeneration,
                                            bool important,
                                            const TDuration availabilityPeriod,
                                            const TString& session,
                                            ui64 partitionSessionId,
                                            ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
                                            TInstant readFromTimestamp, const TActorId& pipeClient, bool anyCommits,
                                            const std::optional<TString>& committedMetadata) const
{
    TString defaultServiceType = AppData(ctx)->PQConfig.GetDefaultClientServiceType().GetName();
    TString userServiceType = "";
    for (auto& consumer : Config.GetConsumers()) {
        if (consumer.GetName() == user) {
            userServiceType = consumer.GetServiceType();
            break;
        }
    }

    bool meterRead = userServiceType.empty() || userServiceType == defaultServiceType;

    return MakeIntrusive<TUserInfo>(
        ctx, StreamCountersSubgroup, GetPartitionCounterSubgroup(ctx),
        user, readRuleGeneration, important, availabilityPeriod, TopicConverter, Partition,
        session, partitionSessionId, gen, step, offset, readOffsetRewindSum, DCId, readFromTimestamp, DbPath,
        meterRead, pipeClient, anyCommits, committedMetadata
    );
}

TUserInfoBase TUsersInfoStorage::CreateUserInfo(const TString& user,
                                            TMaybe<ui64> readRuleGeneration) const
{
    return TUserInfoBase{user, readRuleGeneration ? *readRuleGeneration : ++CurReadRuleGeneration,
                          "", 0, 0, 0, false, false, TDuration::Zero(), {}, 0, {}};
}

TUserInfo& TUsersInfoStorage::Create(
        const TActorContext& ctx, const TString& user, const ui64 readRuleGeneration,
        bool important, const TDuration availabilityPeriod, const TString& session,
        ui64 partitionSessionId, ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
        TInstant readFromTimestamp, const TActorId& pipeClient, bool anyCommits,
        const std::optional<TString>& committedMetadata
) {
    TIntrusivePtr<TUserInfo> userInfo = CreateUserInfo(ctx, user, readRuleGeneration, important, availabilityPeriod, session, partitionSessionId,
                                              gen, step, offset, readOffsetRewindSum, readFromTimestamp, pipeClient,
                                              anyCommits, committedMetadata);
    auto result = UsersInfo.emplace(user, userInfo);
    AFL_ENSURE(result.second);
    if (ImporantOrExtendedAvailabilityPeriod(*userInfo)) {
        UpdateImportantExtSlice(userInfo.Get(), EImportantSliceAction::Insert);
    }
    return *userInfo;
}

void TUsersInfoStorage::UpdateImportantExtSlice(TUserInfo* userInfo, EImportantSliceAction action) {
    Y_ASSERT(userInfo != nullptr);
    switch (action) {
        using enum EImportantSliceAction;
        case Insert: {
            AFL_ENSURE(ImportantExtUsersInfoSlice.emplace(userInfo->User, userInfo).second)("user", userInfo->User);
            break;
        }
        case Remove: {
            AFL_ENSURE(ImportantExtUsersInfoSlice.erase(userInfo->User) > 0)("user", userInfo->User);
            break;
        }
    }
}

void TUsersInfoStorage::Clear(const TActorContext&) {
    ImportantExtUsersInfoSlice.clear();
    UsersInfo.clear();
}

void TUsersInfoStorage::SetImportant(TUserInfo& userInfo, bool important, TDuration availabilityPeriod) {
    bool prev = ImporantOrExtendedAvailabilityPeriod(userInfo);
    userInfo.SetImportant(important, availabilityPeriod);
    bool curr = ImporantOrExtendedAvailabilityPeriod(userInfo);
    if (prev && !curr) {
        UpdateImportantExtSlice(&userInfo, EImportantSliceAction::Remove);
    } else if (!prev && curr) {
        UpdateImportantExtSlice(&userInfo, EImportantSliceAction::Insert);
    }
}

} //NPQ
} //NKikimr
