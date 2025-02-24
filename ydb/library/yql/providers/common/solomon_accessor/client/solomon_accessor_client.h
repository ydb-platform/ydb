#pragma once

#include <library/cpp/json/json_reader.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/library/yql/providers/common/solomon_accessor/grpc/solomon_accessor_pb.pb.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

namespace NYql::NSo {

class TMetric {
public:
    TMetric(const NJson::TJsonValue& value);

public:
    std::map<TString, TString> Labels;
    yandex::monitoring::api::v3::MetricType Type;
    TString CreatedAt;
};

class TTimeseries {
public:
    TString Name;
    yandex::monitoring::api::v3::MetricType Type;
    std::vector<int64_t> Timestamps;
    std::vector<double> Values;
};

class ISolomonAccessorClient {
public:
    using TPtr = std::shared_ptr<ISolomonAccessorClient>;
    using TWeakPtr = std::weak_ptr<ISolomonAccessorClient>;

    virtual ~ISolomonAccessorClient() = default;

    static TPtr Make(
        NYql::NSo::NProto::TDqSolomonSource&& settings,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider);
    
    class TListMetricsResult {
    public:
        TListMetricsResult(const TString& errorMsg);
        TListMetricsResult(std::vector<TMetric>&& result);

    public:
        bool Success;
        TString ErrorMsg;
        std::vector<TMetric> Result;
    };

    class TGetDataResult {
    public:
        TGetDataResult(const TString& errorMsg);
        TGetDataResult(std::vector<TTimeseries>&& result);

    public:
        bool Success;
        TString ErrorMsg;
        std::vector<TTimeseries> Result;
    };

public:
    virtual NThreading::TFuture<TListMetricsResult> ListMetrics(const TString& selectors, int pageSize, int page) = 0;
    virtual NThreading::TFuture<TGetDataResult> GetData(const std::vector<TString>& selectors) = 0;
};

} // namespace NYql::NSo
