#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYql {

struct TDummyTopic {
    TDummyTopic(const TString& cluster, const TString& topicName, const TMaybe<TString>& path = {}, size_t partitionCount = 1);

    TDummyTopic& SetPartitionsCount(size_t count);

    TString Cluster;
    TString TopicName;
    TMaybe<TString> Path;
    size_t PartitionsCount;
    bool CancelOnFileFinish = false;
};

using TClusterNPath = std::pair<TString, TString>;

struct TFileTopicClientSettings {
    TString Database;
    bool SkipDatabasePrefix = false;
};

TString SkipDatabasePrefix(const TString& path, const TString& database);

} // namespace NYql
