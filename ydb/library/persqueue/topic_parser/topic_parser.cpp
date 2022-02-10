#include "topic_parser.h" 
 
#include <ydb/core/base/appdata.h>

#include <util/folder/path.h>

namespace NPersQueue { 
 
namespace {
    TString FullPath(const TMaybe<TString> &database, const TString &path) {
        if (database.Defined() && !path.StartsWith(*database) && !path.Contains('\0')) { 
            try { 
                return (TFsPath(*database) / path).GetPath(); 
            } catch(...) { 
                return path; 
            } 
        } else {
            return path;
        }
    }
}

TString GetFullTopicPath(const NActors::TActorContext& ctx, TMaybe<TString> database, const TString& topicPath) {
    if (NKikimr::AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        return FullPath(database, topicPath);
    } else {
        return topicPath;
    }
}

TString ConvertNewConsumerName(const TString& consumer, const NActors::TActorContext& ctx) {
    if (NKikimr::AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        return consumer;
    } else {
        return ConvertNewConsumerName(consumer);
    }
}

TString ConvertOldConsumerName(const TString& consumer, const NActors::TActorContext& ctx) {
    if (NKikimr::AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        return consumer;
    } else {
        return ConvertOldConsumerName(consumer);
    }
}

TString MakeConsumerPath(const TString& consumer) {
    TStringBuilder res;
    res.reserve(consumer.size());
    for (ui32 i = 0; i < consumer.size(); ++i) {
        if (consumer[i] == '@') {
            res << "/";
        } else {
            res << consumer[i];
        }
    }
    if (res.find("/") == TString::npos) {
        return TStringBuilder() << "shared/" << res;
    }
    return res;
}

TString NormalizeFullPath(const TString& fullPath) {
    if (fullPath.StartsWith("/"))
        return fullPath.substr(1);
    else {
        return fullPath;
    }
}
 
TTopicsListController::TTopicsListController(
        const std::shared_ptr<TTopicNamesConverterFactory>& converterFactory,
        bool haveClusters, const TVector<TString>& clusters, const TString& localCluster
)
    : ConverterFactory(converterFactory)
    , HaveClusters(haveClusters)
{
    UpdateClusters(clusters, localCluster);
}

void TTopicsListController::UpdateClusters(const TVector<TString>& clusters, const TString& localCluster) {
    if (!HaveClusters)
        return;

    Clusters = clusters;
    LocalCluster = localCluster;
}

TTopicsToConverter TTopicsListController::GetReadTopicsList(
        const THashSet<TString>& clientTopics, bool onlyLocal, const TString& database) const
{
    TTopicsToConverter result;
    auto PutTopic = [&] (const TString& topic, const TString& dc) {
        auto converter = ConverterFactory->MakeTopicNameConverter(topic, dc, database);
        result[converter->GetPrimaryPath()] = converter;
    };

    for (const auto& t : clientTopics) {
        if (onlyLocal) {
            PutTopic(t, LocalCluster);
        } else if (HaveClusters){
            for(const auto& c : Clusters) {
                PutTopic(t, c);
            }
        } else {
            PutTopic(t, TString());
        }
    }
    return result;
}
TConverterPtr TTopicsListController::GetWriteTopicConverter(
        const TString& clientName, const TString& database
) {
    return ConverterFactory->MakeTopicNameConverter(clientName, LocalCluster, database);
}

TConverterFactoryPtr TTopicsListController::GetConverterFactory() const {
    return ConverterFactory;
};

} // namespace NPersQueue 

