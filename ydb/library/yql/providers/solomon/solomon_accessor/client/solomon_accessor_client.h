#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_client_utils.h>

namespace NYql::NSo {

class ISolomonAccessorClient {
public:
    using TPtr = std::shared_ptr<ISolomonAccessorClient>;
    using TWeakPtr = std::weak_ptr<ISolomonAccessorClient>;

    virtual ~ISolomonAccessorClient() = default;

    static TPtr Make(
        NYql::NSo::NProto::TDqSolomonSource source,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider);
    
public:
    virtual NThreading::TFuture<TGetLabelsResponse> GetLabelNames(const TSelectors& selectors, TInstant from, TInstant to) const = 0;
    virtual NThreading::TFuture<TListMetricsResponse> ListMetrics(const TSelectors& selectors, TInstant from, TInstant to, int pageSize, int page) const = 0;
    virtual NThreading::TFuture<TGetPointsCountResponse> GetPointsCount(const TSelectors& selectors, TInstant from, TInstant to) const = 0;
    virtual NThreading::TFuture<TGetDataResponse> GetData(const TSelectors& selectors, TInstant from, TInstant to) const = 0;
    virtual NThreading::TFuture<TGetDataResponse> GetData(const TString& program, TInstant from, TInstant to) const = 0;
};

} // namespace NYql::NSo
