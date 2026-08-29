#include "counters.h"

namespace NPersQueue {

::NMonitoring::TDynamicCounterPtr GetCounters(::NMonitoring::TDynamicCounterPtr counters,
                                            const TString& subsystem, const TTopicCounterNames& topic)
{
    TString cluster = topic.Cluster;
    cluster.to_title();
    return ::NKikimr::GetServiceCounters(counters, "pqproxy|" + subsystem)
            ->GetSubgroup("OriginDC", cluster)
            ->GetSubgroup("Producer", topic.LegacyProducer)
            ->GetSubgroup("TopicPath", topic.FederationPath)
            ->GetSubgroup("Account", topic.Account)
            ->GetSubgroup("Topic", topic.ShortClientsideName);
}

::NMonitoring::TDynamicCounterPtr GetCountersForTopic(::NMonitoring::TDynamicCounterPtr counters, bool isServerless)
{
    return counters->GetSubgroup("counters", isServerless ? "datastreams_serverless" : "datastreams");
}

TVector<TPQLabelsInfo> GetLabelsForCustomCluster(const TTopicCounterNames& topic, TString cluster)
{
    cluster.to_title();
    TVector<TPQLabelsInfo> res = {
            {{{"Account", topic.Account}}, {"total"}},
            {{{"Producer", topic.LegacyProducer}}, {"total"}},
            {{{"Topic", topic.ShortClientsideName}, {"TopicPath", topic.FederationPath}}, {"total", "total"}},
            {{{"OriginDC", cluster}}, {"cluster"}}
    };
    return res;
}

TVector<TPQLabelsInfo> GetLabels(const TTopicCounterNames& topic)
{
    return GetLabelsForCustomCluster(topic, topic.Cluster);
}

TVector<std::pair<TString, TString>> GetSubgroupsForTopic(const TTopicCounterNames& topic, const TString& cloudId,
                                        const TString& dbId, const TString& dbPath, const TString& folderId) {
    TVector<std::pair<TString, TString>> res = {
            {"database", dbPath},
            {"cloud_id", cloudId},
            {"folder_id", folderId},
            {"database_id", dbId},
            {"topic", topic.ClientsideName}};
    return res;
}

} // namespace NPersQueue
