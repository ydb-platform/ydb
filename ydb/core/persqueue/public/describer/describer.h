#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>

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
    BAD_REQUEST,
    UNKNOWN_ERROR
};


struct TAccessRights {
    TAccessRights() = default;

    TAccessRights(ui32 access)
        : Access(access)
    {
    }

    TAccessRights(ui32 access, ui32 accessOr)
        : Access(access)
        , AccessOr(accessOr)
    {
    }

   ui32 Access = NACLib::DescribeSchema;
   std::optional<ui32> AccessOr;
};

struct TTopicInfo {
    EStatus Status = EStatus::NOT_FOUND;
    // SchemeCache status/kind/info as returned; msgbus maps these to historical error strings.
    NSchemeCache::TSchemeCacheNavigate::EStatus NavigateStatus = NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown;
    NSchemeCache::TSchemeCacheNavigate::EKind Kind = NSchemeCache::TSchemeCacheNavigate::KindUnknown;

    // Real topic path. If original topic path is CDC than real path is different.
    TString RealPath;
    bool CdcStream = false;
    TString CdcStreamName;

    ui64 CreateStep = 0;
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> Info;
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TDirEntryInfo> Self;
    TIntrusivePtr<TSecurityObject> SecurityObject;
    // Filled on SUCCESS via NamesFromConfig. Null if the topic was not found.
    NNameResolver::TTopicNamesPtr Names;
    NSchemeCache::TDomainInfo::TPtr DomainInfo;

    bool IsServerless() const {
        return DomainInfo && DomainInfo->IsServerless();
    }
};

struct TEvDescribeTopicsResponse : public NActors::TEventLocal<TEvDescribeTopicsResponse, EEv::EvDescribeTopicsResponse> {

    TEvDescribeTopicsResponse(absl::flat_hash_map<TString, TTopicInfo>&& topics, bool usedSyncVersion)
        : Topics(std::move(topics))
        , UsedSyncVersion(usedSyncVersion)
    {
    }

    // The original topic path (from request) -> TopicInfo
    absl::flat_hash_map<TString, TTopicInfo> Topics;
    bool UsedSyncVersion = false;
};

struct TDescribeSettings {
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TAccessRights AccessRights;
    bool ForceSyncVersion = false;
    TString LocalDc;
};

NActors::IActor* CreateDescriberActor(const NActors::TActorId& parent,
                                      const TString& databasePath,
                                      absl::flat_hash_set<TString>&& topicPaths,
                                      const TDescribeSettings& settings = {});

Ydb::StatusIds::StatusCode Convert(const EStatus status);
TString Description(const TString& topicPath, const EStatus status);

} // namespace NKikimr::NPQ::NDescriber
