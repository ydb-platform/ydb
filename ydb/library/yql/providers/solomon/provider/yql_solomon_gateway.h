#pragma once

#include <ydb/library/yql/providers/common/gateway/yql_provider_gateway.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <library/cpp/threading/future/future.h>

namespace NYql {

struct TSolomonMetadata : public TThrRefBase
{
    using TPtr = TIntrusivePtr<TSolomonMetadata>;

    bool DoesExist = false;
};

class ISolomonGateway : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ISolomonGateway>;

    struct TGetMetaRequest {
        TString Cluster;
        TString Name;
    };

    struct TGetMetaResult : public NCommon::TOperationResult {
        TSolomonMetadata::TPtr Meta;
    };

    virtual ~ISolomonGateway() = default;

    virtual NThreading::TFuture<TGetMetaResult> GetMeta(
        const TGetMetaRequest& request) const = 0;

    virtual TMaybe<TSolomonClusterConfig> GetClusterConfig(const TStringBuf cluster) const = 0;
    virtual bool HasCluster(const TStringBuf cluster) const = 0;
};

} // namespace NYql
