#pragma once

#include <library/cpp/json/json_reader.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

namespace NYql::NSo {

class TMetric {
public:
    explicit TMetric(const NJson::TJsonValue& value);

public:
    std::map<TString, TString> Labels;
    TString Type;
    TString CreatedAt;
};

class TTimeseries {
public:
    TString Name;
    std::map<TString, TString> Labels;
    TString Type;
    std::vector<int64_t> Timestamps;
    std::vector<double> Values;
};

class ISolomonAccessorClient {
public:
    using TPtr = std::shared_ptr<ISolomonAccessorClient>;
    using TWeakPtr = std::weak_ptr<ISolomonAccessorClient>;

    virtual ~ISolomonAccessorClient() = default;

    static TPtr Make(
        const NYql::NSo::NProto::TDqSolomonSource& source,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider);
    
    class TListMetricsResult {
    public:
        explicit TListMetricsResult();
        explicit TListMetricsResult(const TString& errorMsg);
        explicit TListMetricsResult(size_t pagesCount, std::vector<TMetric>&& result);

    public:
        bool Success;
        TString ErrorMsg;
        size_t PagesCount;
        std::vector<TMetric> Result;
    };

    class TGetDataResult {
    public:
        explicit TGetDataResult();
        explicit TGetDataResult(const TString& errorMsg);
        explicit TGetDataResult(std::vector<TTimeseries>&& result);

    public:
        bool Success;
        TString ErrorMsg;
        std::vector<TTimeseries> Result;
    };

public:
    virtual NThreading::TFuture<TListMetricsResult> ListMetrics(const TString& selectors, int pageSize, int page) const = 0;
    virtual NThreading::TFuture<TGetDataResult> GetData(const std::vector<TString>& selectors) const = 0;
};

} // namespace NYql::NSo
