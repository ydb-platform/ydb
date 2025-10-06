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
    TDescribeActor(const NActors::TActorId& parent, const TString& databasePath, const std::vector<TString>&& topicPaths)
        : Parent(parent)
        , DatabasePath(databasePath)
        , TopicPaths(std::move(topicPaths))
    {
    }

    void Bootstrap() {
        Become(&TDescribeActor::StateWork);
        Result.resize(TopicPaths.size());
        DoRequest(TopicPaths);
    }

    void DoRequest(const std::vector<TString>& topicPath) {
        LOG_D("Create request with SyncVersion=" << RetryWithSyncVersion);

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
            addEntry(TStringBuilder() << topic << "/streamImpl");
        }

        Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeRequest.release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult");
        auto& result = ev->Get()->Request;

        for (size_t i = 0; i < result->ResultSet.size(); ++i) {
            auto topicIndex = i / 2;
            bool topicVariant = i % 2 == 0;
            bool cdcVariant = i % 2 == 1;

            const auto& entry = result->ResultSet[i];
            const auto& topic = TopicPaths[i / 2];

            auto path = CanonizePath(NKikimr::JoinPath(entry.Path));
            switch (entry.Status) {
                case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case TSchemeCacheNavigate::EStatus::RootUnknown: {
                    if (cdcVariant && Result[topicIndex].Status == TEvDescribeTopicsResponse::EStatus::SUCCESS) {
                        LOG_D("Path '" << path << "' skip internal path");
                        continue;
                    }
                    if (RetryWithSyncVersion) {
                        LOG_D("Path '" << path << "' not found");
                        Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                            .Status = TEvDescribeTopicsResponse::EStatus::NOT_FOUND,
                            .OriginalPath = topic
                        };
                        continue;
                    } else {
                        RetryWithSyncVersion = true;
                        return DoRequest(TopicPaths);
                    }
                }
                case TSchemeCacheNavigate::EStatus::Ok: {
                    if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                        if (topicVariant) {
                            LOG_D("Path '" << path << "' skip CDC");
                            continue;
                        }
                        LOG_D("Path '" << path << "' not topic: " << entry.Kind);
                        Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                            .Status = TEvDescribeTopicsResponse::EStatus::NOT_TOPIC,
                            .OriginalPath = topic
                        };
                    } else if (entry.Kind == TSchemeCacheNavigate::EKind::KindTopic) {
                        LOG_D("Path '" << path << "' SUCCESS");
                        Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                            .Status = TEvDescribeTopicsResponse::EStatus::SUCCESS,
                            .OriginalPath = topic,
                            .RealPath = CanonizePath(NKikimr::JoinPath(entry.Path)),
                            .Info = entry.PQGroupInfo
                        };
                    } else {
                        LOG_D("Path '" << path << "' not topic: " << entry.Kind);
                        Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                            .Status = TEvDescribeTopicsResponse::EStatus::NOT_TOPIC,
                            .OriginalPath = topic
                        };
                    }
                }
                default: {
                    LOG_D("Path '" << path << "' unknow error");
                    Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                        .Status = TEvDescribeTopicsResponse::EStatus::UNKNOWN_ERROR,
                        .OriginalPath = topic
                    };
                    continue;
                }
            }
        }

        Send(Parent, new TEvDescribeTopicsResponse(std::move(Result)));
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
    const std::vector<TString> TopicPaths;

    bool RetryWithSyncVersion = false;
    std::vector<TEvDescribeTopicsResponse::TTopicInfo> Result;
};


NActors::IActor* CreateDescriberActor(const NActors::TActorId& parent, const TString& databasePath, const std::vector<TString>&& topicPaths) {
    return new TDescribeActor(parent, databasePath, std::move(topicPaths));
}
}
