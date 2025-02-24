#pragma once

#include <library/cpp/json/json_reader.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

namespace NYql::NSo {

class Metric {
public:
    Metric(const NJson::TJsonValue &value);

public:
    std::map<TString, TString> Labels;
    int Type;
    TString CreatedAt;
};

class Timeseries {
public:
    TString Name;
    int Type;
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
        TListMetricsResult(const TString &);
        TListMetricsResult(std::vector<Metric> &&);

    public:
        bool Success;
        TString ErrorMsg;
        std::vector<Metric> Result;
    };

    class TGetDataResult {
    public:
    TGetDataResult(const TString &);
    TGetDataResult(std::vector<Timeseries> &&);

    public:
        bool Success;
        TString ErrorMsg;
        std::vector<Timeseries> Result;
    };

public:
    virtual NThreading::TFuture<TListMetricsResult> ListMetrics(const TString &selectors, int pageSize, int page) = 0;
    virtual NThreading::TFuture<TGetDataResult> GetData(const std::vector<TString> &selectors) = 0;
};

} // namespace NYql
