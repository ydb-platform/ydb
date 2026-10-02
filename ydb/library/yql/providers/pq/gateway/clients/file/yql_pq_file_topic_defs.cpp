#include "yql_pq_file_topic_defs.h"

namespace NYql {

TDummyTopic::TDummyTopic(const TString& cluster, const TString& topicName, const TMaybe<TString>& path, size_t partitionCount)
    : Cluster(cluster)
    , TopicName(topicName)
    , Path(path)
    , PartitionsCount(partitionCount)
{}

TDummyTopic& TDummyTopic::SetPartitionsCount(size_t count) {
    PartitionsCount = count;
    return *this;
}

TString SkipDatabasePrefix(const TString& path, const TString& database) {
    TStringBuf pathCanonized(path);
    pathCanonized.SkipPrefix(database);
    pathCanonized.SkipPrefix("/");
    return TString(pathCanonized);
}

} // namespace NYql
