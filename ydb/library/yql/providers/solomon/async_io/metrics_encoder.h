#pragma once

#include <ydb/library/yql/utils/actors/http_sender.h>

#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/monlib/encode/json/json.h>

#include <util/stream/str.h>


namespace NYql::NDq {

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

}
