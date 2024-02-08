#pragma once
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>

namespace NYql {

struct IPqGateway : public TThrRefBase {
    using TPtr = TIntrusivePtr<IPqGateway>;

    struct TListStreams {
        TVector<TString> Names;
    };

    virtual NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) = 0;
    virtual NThreading::TFuture<void> CloseSession(const TString& sessionId) = 0;

    // CM API.
    virtual ::NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) = 0;

    // DS API.
    virtual NThreading::TFuture<TListStreams> ListStreams(const TString& sessionId, const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName = {}) = 0;

    virtual void UpdateClusterConfigs(
        const TString& clusterName,
        const TString& endpoint,
        const TString& database,
        bool secure) = 0;
};

} // namespace NYql
