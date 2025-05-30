#pragma once

#include <library/cpp/monlib/encode/encoder.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>
#include <yql/essentials/public/udf/udf_value.h>

namespace NYql::NDq {

struct TDqSolomonReadParams {
    NSo::NProto::TDqSolomonSource Source;
};

class TMetricsEncoder {
public:
    explicit TMetricsEncoder(const NSo::NProto::TDqSolomonShardScheme& scheme, bool cloudFormat);

    void BeginNew();
    ui64 Append(const NUdf::TUnboxedValue& value);
    TString Encode();

private:
    const NSo::NProto::TDqSolomonShardScheme& Scheme;

    TString Data;
    std::optional<TStringOutput> DataOut;
    NMonitoring::IMetricEncoderPtr SolomonEncoder;
    bool UseCloudFormat;
};

} // namespace NYql::NDq
