#include "describer.h"


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
        //LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult");
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
                case TSchemeCacheNavigate::EStatus::RootUnknown:
                    if (cdcVariant && Result[topicIndex].Status == TEvDescribeTopicsResponse::EStatus::SUCCESS) {
                        continue;
                    }
                    if (RetryWithSyncVersion) {
                        Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                            .Status = TEvDescribeTopicsResponse::EStatus::NOT_FOUND,
                            .OriginalPath = topic
                        };
                        continue;
                    } else {
                        RetryWithSyncVersion = true;
                        return DoRequest(TopicPaths);
                    }
                case TSchemeCacheNavigate::EStatus::Ok:
                    break;
                default:
                    Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                        .Status = TEvDescribeTopicsResponse::EStatus::UNKNOWN_ERROR,
                        .OriginalPath = topic
                    };
                    continue;
            }
            if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                if (topicVariant) {
                    continue;
                }
                Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                    .Status = TEvDescribeTopicsResponse::EStatus::NOT_TOPIC,
                    .OriginalPath = topic
                };
            } else if (entry.Kind == TSchemeCacheNavigate::EKind::KindTopic) {
                Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                    .Status = TEvDescribeTopicsResponse::EStatus::SUCCESS,
                    .OriginalPath = topic,
                    .RealPath = CanonizePath(NKikimr::JoinPath(entry.Path)),
                    .Info = entry.PQGroupInfo
                };
            } else {
                Result[topicIndex] = TEvDescribeTopicsResponse::TTopicInfo{
                    .Status = TEvDescribeTopicsResponse::EStatus::NOT_TOPIC,
                    .OriginalPath = topic
                };
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
