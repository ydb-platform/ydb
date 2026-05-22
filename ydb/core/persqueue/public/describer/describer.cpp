#include "describer.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_DESCRIBER

namespace NKikimr::NPQ::NDescriber {

using namespace NSchemeCache;

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
        YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Create request [ ] with",
            {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID},
            {"#_num_0", JoinRange(", ", topicPath.begin(), topicPath.end())},
            {"SyncVersion", RetryWithSyncVersion});

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
        YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult",
            {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID});
        auto& result = ev->Get()->Request;

        std::unordered_set<TString> unknownPaths;

        for (size_t i = 0; i < result->ResultSet.size(); ++i) {
            const auto& entry = result->ResultSet[i];
            auto realPath = CanonizePath(NKikimr::JoinPath(entry.Path));
            Y_ASSERT(PathToOriginalPath.contains(realPath));
            auto originalPath = PathToOriginalPath[realPath];

            auto it = CDCPaths.find(realPath);
            if (it != CDCPaths.end()) {
                originalPath = it->second;
            }

            switch (entry.Status) {
                case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case TSchemeCacheNavigate::EStatus::RootUnknown: {
                    if (RetryWithSyncVersion) {
                        YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Path ' ' not found",
                            {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID},
                            {"realPath", realPath});
                        Result[originalPath] = TTopicInfo{
                            .Status = EStatus::NOT_FOUND
                        };
                    } else {
                        unknownPaths.insert(realPath);
                    }
                    break;
                }
                case TSchemeCacheNavigate::EStatus::Ok: {
                    if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                        YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Path ' ' is a CDC",
                            {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID},
                            {"realPath", realPath});
                        CDCPaths[TStringBuilder() << realPath << "/streamImpl"] = originalPath;
                    } else if (entry.Kind == TSchemeCacheNavigate::EKind::KindTopic) {
                        if (!entry.PQGroupInfo || entry.PQGroupInfo->Description.GetBalancerTabletID() == 0) {
                            if (RetryWithSyncVersion) {
                                YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Path ' ' not found",
                                    {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID},
                                    {"realPath", realPath});
                                Result[originalPath] = TTopicInfo{
                                    .Status = EStatus::NOT_FOUND
                                };
                            } else {
                                unknownPaths.insert(realPath);
                            }
                        } else {
                            if (Settings.UserToken && !entry.SecurityObject->CheckAccess(Settings.AccessRights, *Settings.UserToken)) {
                                YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Path ' ' UNAUTHORIZED",
                                    {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID},
                                    {"realPath", realPath});
                                Result[originalPath] = TTopicInfo{
                                    .Status = entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)
                                            ? EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS : EStatus::UNAUTHORIZED
                                };
                            } else {
                                YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Path ' ' SUCCESS",
                                    {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID},
                                    {"realPath", realPath});
                                Result[originalPath] = TTopicInfo{
                                    .Status = EStatus::SUCCESS,
                                    .RealPath = realPath,
                                    .CdcStream = CDCPaths.contains(realPath),
                                    .CreateStep = entry.CreateStep,
                                    .Info = entry.PQGroupInfo,
                                    .Self = entry.Self,
                                    .SecurityObject = entry.SecurityObject
                                };
                            }
                        }
                    } else {
                        YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Path ' ' is not a",
                            {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID},
                            {"realPath", realPath},
                            {"topic", entry.Kind});
                        if (Settings.UserToken && !entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)) {
                            YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Path ' ' UNAUTHORIZED",
                                {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID},
                                {"realPath", realPath});
                            Result[originalPath] = TTopicInfo{
                                .Status = EStatus::UNAUTHORIZED
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
                    YDB_LOG_CTX_DEBUG(*NActors::TlsActivationContext, "Path ' ' unknown error",
                        {"SelfID", NActors::TlsActivationContext->AsActorContext().SelfID},
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
    std::unordered_map<TString, TString> CDCPaths;
    std::unordered_map<TString, TTopicInfo> Result;
};


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
