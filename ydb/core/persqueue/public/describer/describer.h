#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NPQ::NDescriber {

enum EEv : ui32 {
    EvDescribeTopicsResponse = InternalEventSpaceBegin(NPQ::NEvents::EServices::DESCRIBER_SERVICE),
    EvEnd
};

enum class EStatus {
    SUCCESS,
    NOT_FOUND,
    NOT_TOPIC,
    UNAUTHORIZED,
    UNAUTHORIZED_WITH_DESCRIBE_ACCESS,
    UNKNOWN_ERROR
};

enum class EPermissionOperand {
    AND,
    OR
};

struct TAccessRights {
    TAccessRights()
        : Operand(EPermissionOperand::OR)
    {
    }

    TAccessRights(NACLib::EAccessRights right)
        : Operand(EPermissionOperand::OR)
        , AccessRights({right})
    {
    }

    explicit TAccessRights(const std::vector<NACLib::EAccessRights>& accessRights)
        : Operand(EPermissionOperand::OR)
        , AccessRights(accessRights)
    {
    }

    TAccessRights(EPermissionOperand operand, const std::vector<NACLib::EAccessRights>& accessRights)
        : Operand(operand)
        , AccessRights(accessRights)
    {
    }

    EPermissionOperand Operand = EPermissionOperand::AND;
    std::vector<NACLib::EAccessRights> AccessRights;
};

struct TTopicInfo {
    EStatus Status = EStatus::NOT_FOUND;

    // Real topic path. If original topic path is CDC than real path is different.
    TString RealPath;
    bool CdcStream = false;
    TString CdcStreamName;

    ui64 CreateStep = 0;
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> Info;
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TDirEntryInfo> Self;
    TIntrusivePtr<TSecurityObject> SecurityObject;
};

struct TEvDescribeTopicsResponse : public NActors::TEventLocal<TEvDescribeTopicsResponse, EEv::EvDescribeTopicsResponse> {

    TEvDescribeTopicsResponse(std::unordered_map<TString, TTopicInfo>&& topics, bool usedSyncVersion)
        : Topics(std::move(topics))
        , UsedSyncVersion(usedSyncVersion)
    {
    }

    // The original topic path (from request) -> TopicInfo
    std::unordered_map<TString, TTopicInfo> Topics;
    bool UsedSyncVersion = false;
};

struct TDescribeSettings {
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TAccessRights AccessRights;
    bool ForceSyncVersion = false;
};

NActors::IActor* CreateDescriberActor(const NActors::TActorId& parent,
                                      const TString& databasePath,
                                      const std::unordered_set<TString>&& topicPaths,
                                      const TDescribeSettings& settings = {});

Ydb::StatusIds::StatusCode Convert(const EStatus status);
TString Description(const TString& topicPath, const EStatus status);

} // namespace NKikimr::NPQ::NDescriber
