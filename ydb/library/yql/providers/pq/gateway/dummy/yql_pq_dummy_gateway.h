#pragma once
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>

namespace NYql {

struct TDummyTopic {
    TDummyTopic(const TString& cluster, const TString& path)
        : Cluster(cluster)
        , Path(path)
    {
    }

    TDummyTopic& SetPartitionsCount(size_t count) {
        PartitionsCount = count;
        return *this;
    }

    TString Cluster;
    TString Path;
    size_t PartitionsCount = 1;
};

// Dummy Pq gateway for tests.
class TDummyPqGateway : public IPqGateway {
public:
    TDummyPqGateway& AddDummyTopic(const TDummyTopic& topic);

public:
    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override;
    NThreading::TFuture<void> CloseSession(const TString& sessionId) override;

    ::NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& path,
        const TString& token) override;

    NThreading::TFuture<TListStreams> ListStreams(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& token,
        ui32 limit,
        const TString& exclusiveStartStreamName = {}) override;

    void UpdateClusterConfigs(
        const TString& clusterName,
        const TString& endpoint,
        const TString& database,
        bool secure) override;

private:
    mutable TMutex Mutex;
    THashMap<std::pair<TString, TString>, TDummyTopic> Topics;
    THashSet<TString> OpenedSessions;
};

} // namespace NYql
