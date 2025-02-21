#pragma once

#include <library/cpp/json/json_reader.h>
#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/library/yql/providers/common/solomon_accessor/grpc/solomon_accessor_pb.pb.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

namespace NYql::NSo {

class Metric {
public:
    Metric(const NJson::TJsonValue &value);

public:
    std::map<TString, TString> Labels;
    yandex::monitoring::api::v3::MetricType Type;
    TString CreatedAt;
};

class Timeseries {
public:
    TString Name;
    yandex::monitoring::api::v3::MetricType Type;
    std::vector<int64_t> TimestampValues;
    std::vector<double> TimeseriesValues;
};

class ISolomonAccessorClient {
public:
    using TPtr = std::shared_ptr<ISolomonAccessorClient>;
    using TWeakPtr = std::weak_ptr<ISolomonAccessorClient>;

    virtual ~ISolomonAccessorClient() = default;

    static TPtr Make(
        NYql::NSo::NProto::TDqSolomonSource&& settings,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider);

public:
    virtual TMaybe<TString> ListMetrics(
        const TString &selectors,
        int pageSize,
        int page,
        TVector<Metric> &result) = 0;

    virtual TMaybe<TString> GetData(
        const std::vector<TString> &selectors,
        std::vector<Timeseries> &results) = 0;
};

} // namespace NYql