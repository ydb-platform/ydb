#include "user_info.h"

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

TUsersInfoStorage::TUsersInfoStorage(
    TString dcId,
    const NPersQueue::TTopicConverterPtr& topicConverter,
    ui32 partition,
    const NKikimrPQ::TPQTabletConfig& config,
    const TString& cloudId,
    const TString& dbId,
    const TString& dbPath,
    const bool isServerless,
    const TString& folderId
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
        Create(ctx, user, 0, false, session, 0, gen, step, static_cast<i64>(offset), 0, TInstant::Zero(), {}, false);
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
            ctx, user, userData.GetReadRuleGeneration(), false, userData.GetSession(), userData.GetPartitionSessionId(),
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
    UsersInfo.erase(it);
}

TUserInfo& TUsersInfoStorage::GetOrCreate(const TString& user, const TActorContext& ctx, TMaybe<ui64> readRuleGeneration) {
    AFL_ENSURE(!user.empty());
    auto it = UsersInfo.find(user);
    if (it == UsersInfo.end()) {
        return Create(
                ctx, user, readRuleGeneration ? *readRuleGeneration : ++CurReadRuleGeneration, false, "", 0,
                0, 0, 0, 0, TInstant::Zero(), {}, false
        );
    }
    return it->second;
}

::NMonitoring::TDynamicCounterPtr TUsersInfoStorage::GetPartitionCounterSubgroup(const TActorContext& ctx) const {
    if (!Config.GetEnablePerPartitionCounters()) {
        return nullptr;
    }
    auto counters = AppData(ctx)->Counters;
    if (!counters) {
        return nullptr;
    }
    if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
        return counters
            ->GetSubgroup("counters", IsServerless ? "topics_per_partition_serverless" : "topics_per_partition")
            ->GetSubgroup("host", "")
            ->GetSubgroup("database", Config.GetYdbDatabasePath())
            ->GetSubgroup("cloud_id", CloudId)
            ->GetSubgroup("folder_id", FolderId)
            ->GetSubgroup("database_id", DbId)
            ->GetSubgroup("topic", TopicConverter->GetPrimaryPath())  // TODO(qyryq) Is it a correct value?
            ->GetSubgroup("partition_id", ToString(Partition));
    } else {
        return counters
            ->GetSubgroup("counters", "topics_per_partition")
            ->GetSubgroup("host", "cluster")
            ->GetSubgroup("Account", TopicConverter->GetAccount())
            ->GetSubgroup("TopicPath", TopicConverter->GetFederationPath())
            ->GetSubgroup("OriginDC", TopicConverter->GetCluster())
            ->GetSubgroup("Partition", ToString(Partition));
    }
}


void TUsersInfoStorage::SetupPerPartitionCounters(const TActorContext& ctx) {
    auto subgroup = GetPartitionCounterSubgroup(ctx);
    if (!subgroup) {
        return;  // TODO(qyryq) Y_ABORT_UNLESS?
    }
    for (auto& userInfo : GetAll()) {
        userInfo.second.SetupPerPartitionCounters(ctx, subgroup);
    }
}

void TUsersInfoStorage::ResetPerPartitionCounters() {
    for (auto& userInfo : GetAll()) {
        userInfo.second.ResetPerPartitionCounters();
    }
}

const TUserInfo* TUsersInfoStorage::GetIfExists(const TString& user) const {
    auto it = UsersInfo.find(user);
    return it != UsersInfo.end() ? &it->second : nullptr;
}

TUserInfo* TUsersInfoStorage::GetIfExists(const TString& user) {
    auto it = UsersInfo.find(user);
    return it != UsersInfo.end() ? &it->second : nullptr;
}

THashMap<TString, TUserInfo>& TUsersInfoStorage::GetAll() {
    return UsersInfo;
}

TUserInfo TUsersInfoStorage::CreateUserInfo(const TActorContext& ctx,
                                            const TString& user,
                                            const ui64 readRuleGeneration,
                                            bool important,
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

    return {
        ctx, StreamCountersSubgroup, GetPartitionCounterSubgroup(ctx),
        user, readRuleGeneration, important, TopicConverter, Partition,
        session, partitionSessionId, gen, step, offset, readOffsetRewindSum, DCId, readFromTimestamp, DbPath,
        meterRead, pipeClient, anyCommits, committedMetadata
    };
}

TUserInfoBase TUsersInfoStorage::CreateUserInfo(const TString& user,
                                            TMaybe<ui64> readRuleGeneration) const
{
    return TUserInfoBase{user, readRuleGeneration ? *readRuleGeneration : ++CurReadRuleGeneration,
                          "", 0, 0, 0, false, false, {}, 0, {}};
}

TUserInfo& TUsersInfoStorage::Create(
        const TActorContext& ctx, const TString& user, const ui64 readRuleGeneration, bool important, const TString& session,
        ui64 partitionSessionId, ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
        TInstant readFromTimestamp, const TActorId& pipeClient, bool anyCommits,
        const std::optional<TString>& committedMetadata
) {
    auto userInfo = CreateUserInfo(ctx, user, readRuleGeneration, important, session, partitionSessionId,
                                              gen, step, offset, readOffsetRewindSum, readFromTimestamp, pipeClient,
                                              anyCommits, committedMetadata);
    auto result = UsersInfo.emplace(user, std::move(userInfo));
    AFL_ENSURE(result.second);
    return result.first->second;
}

void TUsersInfoStorage::Clear(const TActorContext&) {
    UsersInfo.clear();
}

} //NPQ
} //NKikimr
