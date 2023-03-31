#include "counters.h"

namespace NPersQueue {

::NMonitoring::TDynamicCounterPtr GetCounters(::NMonitoring::TDynamicCounterPtr counters,
                                            const TString& subsystem, const TTopicConverterPtr& topic)
{
    TString cluster = topic->GetCluster();
    cluster.to_title();
    return ::NKikimr::GetServiceCounters(counters, "pqproxy|" + subsystem)
            ->GetSubgroup("OriginDC", cluster)
            ->GetSubgroup("Producer", topic->GetLegacyProducer())
            ->GetSubgroup("TopicPath", topic->GetFederationPath())
            ->GetSubgroup("Account", topic->GetAccount())
            ->GetSubgroup("Topic", topic->GetShortClientsideName());
}

::NMonitoring::TDynamicCounterPtr GetCountersForTopic(::NMonitoring::TDynamicCounterPtr counters, bool isServerless)
{
    return counters->GetSubgroup("counters", isServerless ? "datastreams_serverless" : "datastreams");
}

TVector<TPQLabelsInfo> GetLabelsForCustomCluster(const TTopicConverterPtr& topic, TString cluster)
{
    cluster.to_title();
    TVector<TPQLabelsInfo> res = {
            {{{"Account", topic->GetAccount()}}, {"total"}},
            {{{"Producer", topic->GetLegacyProducer()}}, {"total"}},
            {{{"Topic", topic->GetShortClientsideName()}, {"TopicPath", topic->GetFederationPath()}}, {"total", "total"}},
            {{{"OriginDC", cluster}}, {"cluster"}}
    };
    return res;
}

TVector<TPQLabelsInfo> GetLabels(const TTopicConverterPtr& topic)
{
    return GetLabelsForCustomCluster(topic, topic->GetCluster());
}

TVector<std::pair<TString, TString>> GetSubgroupsForTopic(const TTopicConverterPtr& topic, const TString& cloudId,
                                        const TString& dbId, const TString& dbPath, const TString& folderId) {
    TVector<std::pair<TString, TString>> res = {
            {"database", dbPath},
            {"cloud_id", cloudId},
            {"folder_id", folderId},
            {"database_id", dbId},
            {"topic", topic->GetClientsideName()}};
    return res;
}

} // namespace NPersQueue
