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
        Y_VERIFY(sizeof(ui64) <= data.size());

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
    Y_VERIFY(UsersInfo.empty());
    Y_VERIFY(!TabletActor);
    Y_VERIFY(!PartitionActor);
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
    Y_VERIFY(key.size() >= TKeyPrefix::MarkedSize());
    Y_VERIFY(key[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkUserDeprecated);
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
    Y_VERIFY(offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, offset);

    if (!userInfo) {
        Create(ctx, user, 0, false, session, gen, step, static_cast<i64>(offset), 0, TInstant::Zero());
    } else {
        userInfo->Session = session;
        userInfo->Generation = gen;
        userInfo->Step = step;
        userInfo->Offset = static_cast<i64>(offset);
    }
}

void TUsersInfoStorage::Parse(const TString& key, const TString& data, const TActorContext& ctx) {
    Y_VERIFY(key.size() >= TKeyPrefix::MarkedSize());
    Y_VERIFY(key[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkUser);
    TString user = key.substr(TKeyPrefix::MarkedSize());

    Y_VERIFY(sizeof(ui64) <= data.size());

    NKikimrPQ::TUserInfo userData;
    bool res = userData.ParseFromString(data);
    Y_VERIFY(res);

    Y_VERIFY(userData.GetOffset() <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, userData.GetOffset());
    i64 offset = static_cast<i64>(userData.GetOffset());

    TUserInfo* userInfo = GetIfExists(user);
    if (!userInfo) {
        Create(
            ctx, user, userData.GetReadRuleGeneration(), false, userData.GetSession(),
            userData.GetGeneration(), userData.GetStep(), offset, userData.GetOffsetRewindSum(), TInstant::Zero()
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
    Y_VERIFY(userInfo);
    userInfo->Parsed = true;
}

void TUsersInfoStorage::Remove(const TString& user, const TActorContext&) {
    auto it = UsersInfo.find(user);
    Y_VERIFY(it != UsersInfo.end());
    UsersInfo.erase(it);
}

TUserInfo& TUsersInfoStorage::GetOrCreate(const TString& user, const TActorContext& ctx, TMaybe<ui64> readRuleGeneration) {
    Y_VERIFY(!user.empty());
    auto it = UsersInfo.find(user);
    if (it == UsersInfo.end()) {
        return Create(ctx, user, readRuleGeneration ? *readRuleGeneration : ++CurReadRuleGeneration, false, "", 0, 0, 0, 0, TInstant::Zero());
    }
    return it->second;
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
                                            ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum,
                                            TInstant readFromTimestamp) const
{
    TString defaultServiceType = AppData(ctx)->PQConfig.GetDefaultClientServiceType().GetName();
    TString userServiceType = "";
    for (ui32 i = 0; i < Config.ReadRulesSize(); ++i) {
        if (Config.GetReadRules(i) == user) {
            userServiceType = Config.ReadRuleServiceTypesSize() > i ? Config.GetReadRuleServiceTypes(i) : "";
            break;
        }
    }

    bool meterRead = userServiceType.empty() || userServiceType == defaultServiceType;


    return { 
        ctx, StreamCountersSubgroup,
        user, readRuleGeneration, important, TopicConverter, Partition,
        session, gen, step, offset, readOffsetRewindSum, DCId, readFromTimestamp, DbPath,
        meterRead
    };
}

TUserInfoBase TUsersInfoStorage::CreateUserInfo(const TString& user,
                                            TMaybe<ui64> readRuleGeneration) const
{
    return TUserInfoBase{user, readRuleGeneration ? *readRuleGeneration : ++CurReadRuleGeneration,
                          "", 0, 0, 0, false, {}};
}

TUserInfo& TUsersInfoStorage::Create(
    const TActorContext& ctx, const TString& user, const ui64 readRuleGeneration, bool important, const TString& session,
    ui32 gen, ui32 step, i64 offset, ui64 readOffsetRewindSum, TInstant readFromTimestamp
) {
    auto userInfo = CreateUserInfo(ctx, user, readRuleGeneration, important, session, gen, step, offset, readOffsetRewindSum, readFromTimestamp);
    auto result = UsersInfo.emplace(user, std::move(userInfo));
    Y_VERIFY(result.second);
    return result.first->second;
}

void TUsersInfoStorage::Clear(const TActorContext&) {
    UsersInfo.clear();
}

} //NPQ
} //NKikimr
