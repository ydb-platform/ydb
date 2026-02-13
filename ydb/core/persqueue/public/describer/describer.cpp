#include "describer.h"

#define LOG_PREFIX "[" << NActors::TlsActivationContext->AsActorContext().SelfID << "] "
#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_DESCRIBER, LOG_PREFIX << stream)
#define LOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::PQ_DESCRIBER, LOG_PREFIX << stream)
#define LOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_DESCRIBER, LOG_PREFIX << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_DESCRIBER, LOG_PREFIX << stream)


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
        DoRequest(TopicPaths);
    }

    void DoRequest(const std::unordered_set<TString>& topicPath) {
        LOG_D("Create request [" << JoinRange(", ", topicPath.begin(), topicPath.end()) << "] with SyncVersion=" << RetryWithSyncVersion);

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
            addEntry(topic);
        }

        Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeRequest.release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult");
        auto& result = ev->Get()->Request;

        std::unordered_set<TString> unknownPaths;

        for (size_t i = 0; i < result->ResultSet.size(); ++i) {
            const auto& entry = result->ResultSet[i];
            auto realPath = CanonizePath(NKikimr::JoinPath(entry.Path));

            auto originalPath = realPath;
            auto it = CDCPaths.find(realPath);
            if (it != CDCPaths.end()) {
                originalPath = it->second;
            }

            switch (entry.Status) {
                case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case TSchemeCacheNavigate::EStatus::RootUnknown: {
                    if (RetryWithSyncVersion) {
                        LOG_D("Path '" << realPath << "' not found");
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
                        LOG_D("Path '" << realPath << "' is a CDC");
                        CDCPaths[TStringBuilder() << originalPath << "/streamImpl"] = originalPath;
                    } else if (entry.Kind == TSchemeCacheNavigate::EKind::KindTopic) {
                        if (!entry.PQGroupInfo || entry.PQGroupInfo->Description.GetBalancerTabletID() == 0) {
                            if (RetryWithSyncVersion) {
                                LOG_D("Path '" << realPath << "' not found");
                                Result[originalPath] = TTopicInfo{
                                    .Status = EStatus::NOT_FOUND
                                };
                            } else {
                                unknownPaths.insert(realPath);
                            }
                        } else {
                            if (Settings.UserToken && !entry.SecurityObject->CheckAccess(Settings.AccessRights, *Settings.UserToken)) {
                                LOG_D("Path '" << realPath << "' UNAUTHORIZED");
                                Result[originalPath] = TTopicInfo{
                                    .Status = EStatus::UNAUTHORIZED
                                };
                            } else {
                                LOG_D("Path '" << realPath << "' SUCCESS");
                                Result[originalPath] = TTopicInfo{
                                    .Status = EStatus::SUCCESS,
                                    .RealPath = realPath,
                                    .Info = entry.PQGroupInfo,
                                    .SecurityObject = entry.SecurityObject
                                };
                            }
                        }
                    } else {
                        LOG_D("Path '" << realPath << "' is not a topic: " << entry.Kind);
                        if (Settings.UserToken && !entry.SecurityObject->CheckAccess(NACLib::EAccessRights::DescribeSchema, *Settings.UserToken)) {
                            LOG_D("Path '" << realPath << "' UNAUTHORIZED");
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
                    LOG_D("Path '" << realPath << "' unknown error");
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
            return TStringBuilder() << "You do not have access or the '" << topicPath << "' does not exist";
        case EStatus::NOT_TOPIC:
            return TStringBuilder() << "The '" << topicPath << "' path is not a topic";
        case EStatus::UNKNOWN_ERROR:
            return TStringBuilder() << "Error describing the path '" << topicPath << "'";
    }
}

}
