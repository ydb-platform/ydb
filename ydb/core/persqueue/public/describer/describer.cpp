#include "describer.h"

#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_DESCRIBER

#define LOG_PREFIX NActors::TlsActivationContext->AsActorContext().SelfID

namespace NKikimr::NPQ::NDescriber {

namespace {

using namespace NSchemeCache;

bool HasAccess(const TDescribeSettings& settings, TIntrusivePtr<TSecurityObject> securityObject) {
    if (!settings.UserToken) {
        return true;
    }
    if (securityObject->CheckAccess(settings.AccessRights.Access, *settings.UserToken)) {
        return true;
    }
    if (settings.AccessRights.AccessOr) {
        return securityObject->CheckAccess(*settings.AccessRights.AccessOr, *settings.UserToken);
    }
    return false;
}

class TDescribeActor : public TActorBootstrapped<TDescribeActor> {
public:
    TDescribeActor(const NActors::TActorId& parent, const TString& databasePath, const std::unordered_set<TString>&& topicPaths, const TDescribeSettings& settings)
        : Parent(parent)
        , DatabasePath(databasePath)
        , TopicPaths(std::move(topicPaths))
        , Settings(settings)
    {
    }

    void Bootstrap() {
        Become(&TDescribeActor::StateWork);
        RetryWithSyncVersion = Settings.ForceSyncVersion;
        UsedSyncVersion = Settings.ForceSyncVersion;
        DoRequest(TopicPaths);
    }

    void DoRequest(const std::unordered_set<TString>& topicPath) {
        YDB_LOG_DEBUG("Create request with",
            {"logPrefix", LOG_PREFIX},
            {"topicPaths", JoinRange(", ", topicPath.begin(), topicPath.end())},
            {"syncVersion", RetryWithSyncVersion});

        auto schemeRequest = std::make_unique<TSchemeCacheNavigate>(1);
        schemeRequest->DatabaseName = DatabasePath;

        auto addEntry = [&](const TString& topic) {
            auto split = NKikimr::SplitPath(topic);

            schemeRequest->ResultSet.emplace_back();
            auto& entry = schemeRequest->ResultSet.back();
            entry.Path.insert(entry.Path.end(), split.begin(), split.end());
            entry.Operation = TSchemeCacheNavigate::OpList;
            entry.SyncVersion = RetryWithSyncVersion;
            entry.ShowPrivatePath = true;
        };

        for (const auto& topic : topicPath) {
            auto normalizedPath = NKikimr::NormalizePath(DatabasePath, CanonizePath(topic));
            PathToOriginalPath[normalizedPath] = topic;
            addEntry(normalizedPath);
        }

        Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeRequest.release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        YDB_LOG_DEBUG("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"logPrefix", LOG_PREFIX});
        auto& result = ev->Get()->Request;

        std::unordered_set<TString> unknownPaths;

        for (size_t i = 0; i < result->ResultSet.size(); ++i) {
            const auto& entry = result->ResultSet[i];
            auto realPath = CanonizePath(NKikimr::JoinPath(entry.Path));
            Y_ASSERT(PathToOriginalPath.contains(realPath));
            auto originalPath = PathToOriginalPath[realPath];

            bool isCDCStream = false;
            TString cdcStreamName;

            auto it = CDCPaths.find(realPath);
            if (it != CDCPaths.end()) {
                originalPath = it->second.OriginalPath;
                isCDCStream = true;
                cdcStreamName = it->second.CdcStreamName;
            }

            switch (entry.Status) {
                case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                    [[fallthrough]];
                case TSchemeCacheNavigate::EStatus::RootUnknown: {
                    if (RetryWithSyncVersion) {
                        if (entry.SecurityObject && !HasAccess(Settings, entry.SecurityObject)) {
                            YDB_LOG_DEBUG("Path UNAUTHORIZED",
                                {"logPrefix", LOG_PREFIX},
                                {"realPath", realPath});

                            Result[originalPath] = TTopicInfo{
                                .Status = EStatus::UNAUTHORIZED
                            };
                        } else {
                            YDB_LOG_DEBUG("Path not found",
                                {"logPrefix", LOG_PREFIX},
                                {"realPath", realPath});

                            Result[originalPath] = TTopicInfo{
                                .Status = EStatus::NOT_FOUND
                            };
                        }
                    } else {
                        unknownPaths.insert(realPath);
                    }
                    break;
                }
                case TSchemeCacheNavigate::EStatus::AccessDenied: {
                    LOG_D("Path '" << realPath << "' ACCESS DENIED");
                    Result[originalPath] = TTopicInfo{
                        .Status = EStatus::UNAUTHORIZED
                    };
                    break;
                }
                case TSchemeCacheNavigate::EStatus::Ok: {
                    if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                        YDB_LOG_DEBUG("Path is CDC",
                            {"logPrefix", LOG_PREFIX},
                            {"realPath", realPath});

                        CDCPaths[TStringBuilder() << realPath << "/streamImpl"] = {
                            .OriginalPath = originalPath,
                            .CdcStreamName = entry.Self->Info.GetName()
                        };
                        break;
                    } else if (entry.Kind == TSchemeCacheNavigate::EKind::KindTopic) {
                        if (!entry.PQGroupInfo || entry.PQGroupInfo->Description.GetBalancerTabletID() == 0) {
                            if (RetryWithSyncVersion) {
                                YDB_LOG_DEBUG("Path not found",
                                    {"logPrefix", LOG_PREFIX},
                                    {"realPath", realPath});
                                Result[originalPath] = TTopicInfo{
                                    .Status = EStatus::NOT_FOUND
                                };
                            } else {
                                unknownPaths.insert(realPath);
                            }
                        } else {
                            if (!HasAccess(Settings, entry.SecurityObject)) {
                                YDB_LOG_DEBUG("Path UNAUTHORIZED",
                                    {"logPrefix", LOG_PREFIX},
                                    {"realPath", realPath});

                                Result[originalPath] = TTopicInfo{
                                    .Status = entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)
                                            ? EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS : EStatus::UNAUTHORIZED
                                };
                            } else {
                                YDB_LOG_DEBUG("Path SUCCESS",
                                    {"logPrefix", LOG_PREFIX},
                                    {"realPath", realPath});
                                Result[originalPath] = TTopicInfo{
                                    .Status = EStatus::SUCCESS,
                                    .RealPath = realPath,
                                    .CdcStream = isCDCStream,
                                    .CdcStreamName = cdcStreamName,
                                    .CreateStep = entry.CreateStep,
                                    .Info = entry.PQGroupInfo,
                                    .Self = entry.Self,
                                    .SecurityObject = entry.SecurityObject
                                };
                            }
                        }
                    } else {
                        YDB_LOG_DEBUG("Path is not a",
                            {"logPrefix", LOG_PREFIX},
                            {"realPath", realPath},
                            {"topic", entry.Kind});
                        if (Settings.UserToken && !entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)) {
                            YDB_LOG_DEBUG("Path UNAUTHORIZED",
                                {"logPrefix", LOG_PREFIX},
                                {"realPath", realPath});
                            Result[originalPath] = TTopicInfo{
                                .Status = EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS
                            };
                        } else {
                            Result[originalPath] = TTopicInfo{
                                .Status = EStatus::NOT_TOPIC,
                                .RealPath = realPath
                            };
                        }
                    }
                    break;
                }
                default: {
                    YDB_LOG_DEBUG("Path unknown error",
                        {"logPrefix", LOG_PREFIX},
                        {"realPath", realPath});
                    Result[originalPath] = TTopicInfo{
                        .Status = EStatus::UNKNOWN_ERROR,
                        .RealPath = realPath
                    };
                    break;
                }
            }
        }

        if (!unknownPaths.empty()) {
            RetryWithSyncVersion = true;
            UsedSyncVersion = true;
            return DoRequest(unknownPaths);
        }

        if (!CDCPaths.empty() && !RetryWithCDC) {
            RetryWithSyncVersion = false;
            RetryWithCDC = true;

            std::unordered_set<TString> newPath;
            newPath.reserve(CDCPaths.size());
            for (auto& [path, _] : CDCPaths) {
                newPath.insert(path);
            }

            return DoRequest(newPath);
        }

        Send(Parent, new TEvDescribeTopicsResponse(std::move(Result), UsedSyncVersion));
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const NActors::TActorId Parent;
    const TString DatabasePath;
    const std::unordered_set<TString> TopicPaths;
    const TDescribeSettings Settings;
    // normalized path -> original path
    std::unordered_map<TString, TString> PathToOriginalPath;

    bool RetryWithSyncVersion = false;
    bool UsedSyncVersion = false;
    bool RetryWithCDC = false;
    // CDC topic path -> original topic path
    struct TCDCTopicInfo {
        TString OriginalPath;
        TString CdcStreamName;
    };
    std::unordered_map<TString, TCDCTopicInfo> CDCPaths;
    std::unordered_map<TString, TTopicInfo> Result;
};

} // namespace

NActors::IActor* CreateDescriberActor(const NActors::TActorId& parent, const TString& databasePath, const std::unordered_set<TString>&& topicPaths, const TDescribeSettings& settings) {
    return new TDescribeActor(parent, databasePath, std::move(topicPaths), settings);
}


Ydb::StatusIds::StatusCode Convert(const EStatus status) {
    switch (status) {
        case EStatus::SUCCESS:
            return Ydb::StatusIds::SUCCESS;
        case EStatus::NOT_FOUND:
        case EStatus::NOT_TOPIC:
            return Ydb::StatusIds::NOT_FOUND;
        case EStatus::UNAUTHORIZED:
        case EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
            return Ydb::StatusIds::UNAUTHORIZED;
        case EStatus::UNKNOWN_ERROR:
            return Ydb::StatusIds::INTERNAL_ERROR;
    }
}

TString Description(const TString& topicPath, const EStatus status) {
    switch (status) {
        case EStatus::SUCCESS:
            return TStringBuilder() << "The topic '" << topicPath << "' has been successfully described";
        case EStatus::NOT_FOUND:
        case EStatus::UNAUTHORIZED:
            return TStringBuilder() << "You do not have access permissions or the '" << topicPath << "' does not exist";
        case EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
            return TStringBuilder() << "You do not have access permissions to the '" << topicPath << "' topic";
        case EStatus::NOT_TOPIC:
            return TStringBuilder() << "The '" << topicPath << "' path is not a topic";
        case EStatus::UNKNOWN_ERROR:
            return TStringBuilder() << "Error describing the path '" << topicPath << "'";
    }
}

}
