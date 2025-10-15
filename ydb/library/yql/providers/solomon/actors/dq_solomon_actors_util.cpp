#include "dq_solomon_actors_util.h"

#include <library/cpp/monlib/encode/json/json.h>
#include <util/datetime/base.h>
#include <yql/essentials/public/udf/udf_data_type.h>

using namespace NYql;

namespace {

void BeginMetric(
    const NMonitoring::IMetricEncoderPtr& encoder,
    const NYql::NSo::NProto::TDqSolomonSchemeItem& scheme)
{
    switch (scheme.GetDataTypeId()) {
        case NUdf::TDataType<i8>::Id:
        case NUdf::TDataType<ui8>::Id:
        case NUdf::TDataType<i16>::Id:
        case NUdf::TDataType<ui16>::Id:
        case NUdf::TDataType<i32>::Id:
        case NUdf::TDataType<ui32>::Id:
        case NUdf::TDataType<i64>::Id:
        case NUdf::TDataType<ui64>::Id:
            encoder->OnMetricBegin(NMonitoring::EMetricType::IGAUGE);
            break;
        case NUdf::TDataType<float>::Id:
        case NUdf::TDataType<double>::Id:
            encoder->OnMetricBegin(NMonitoring::EMetricType::GAUGE);
            break;
        default:
            Y_ENSURE(false, "Bad type for sensor " << scheme.GetDataTypeId());
    }
}

TMaybe<TInstant> ParseTimestamp(
    const NUdf::TUnboxedValue& unboxedValue,
    const NYql::NSo::NProto::TDqSolomonSchemeItem& scheme)
{
    const NUdf::TUnboxedValue timestampValue = unboxedValue.GetElement(scheme.GetIndex());
    if (!timestampValue) {
        return {};
    }

    switch (scheme.GetDataTypeId()) {
        case NUdf::TDataType<NUdf::TDate>::Id:
        case NUdf::TDataType<NUdf::TTzDate>::Id:
        case NUdf::TDataType<NUdf::TDatetime>::Id:
        case NUdf::TDataType<NUdf::TTzDatetime>::Id:
            return TInstant::Seconds(timestampValue.Get<ui32>());
        case NUdf::TDataType<NUdf::TTimestamp>::Id:
        case NUdf::TDataType<NUdf::TTzTimestamp>::Id:
            return TInstant::MicroSeconds(timestampValue.Get<ui64>());
        default:
            Y_ENSURE(false, "Bad type for timestamp " << scheme.GetDataTypeId());
    }
}

void EncodeSensorValue(
    const NMonitoring::IMetricEncoderPtr& encoder,
    const TInstant& timestamp,
    const NUdf::TUnboxedValue& unboxedValue,
    const NYql::NSo::NProto::TDqSolomonSchemeItem& scheme)
{
    switch (scheme.GetDataTypeId()) {
        case NUdf::TDataType<i8>::Id:
            encoder->OnInt64(timestamp, unboxedValue.Get<i8>());
            break;
        case NUdf::TDataType<ui8>::Id:
            encoder->OnUint64(timestamp, unboxedValue.Get<ui8>());
            break;
        case NUdf::TDataType<i16>::Id:
            encoder->OnInt64(timestamp, unboxedValue.Get<i16>());
            break;
        case NUdf::TDataType<ui16>::Id:
            encoder->OnUint64(timestamp, unboxedValue.Get<ui16>());
            break;
        case NUdf::TDataType<i32>::Id:
            encoder->OnInt64(timestamp, unboxedValue.Get<i32>());
            break;
        case NUdf::TDataType<ui32>::Id:
            encoder->OnUint64(timestamp, unboxedValue.Get<ui32>());
            break;
        case NUdf::TDataType<i64>::Id:
            encoder->OnInt64(timestamp, unboxedValue.Get<i64>());
            break;
        case NUdf::TDataType<ui64>::Id:
            encoder->OnUint64(timestamp, unboxedValue.Get<ui64>());
            break;
        case NUdf::TDataType<float>::Id:
            encoder->OnDouble(timestamp, unboxedValue.Get<float>());
            break;
        case NUdf::TDataType<double>::Id:
            encoder->OnDouble(timestamp, unboxedValue.Get<double>());
            break;
        default:
            Y_ENSURE(false, "Bad type for sensor " << scheme.GetDataTypeId());
    }
}

} // namespace

namespace NYql::NDq {

    TMetricsEncoder::TMetricsEncoder(const NSo::NProto::TDqSolomonShardScheme& scheme, bool useCloudFormat)
    : Scheme(scheme)
    , UseCloudFormat(useCloudFormat)
{
    BeginNew();
}

void TMetricsEncoder::BeginNew() {
    Data.clear();
    DataOut.emplace(Data);
    SolomonEncoder = UseCloudFormat
        ? NMonitoring::BufferedEncoderCloudJson(&DataOut.value(), 0, "name")
        : NMonitoring::BufferedEncoderJson(&DataOut.value(), 0);

    SolomonEncoder->OnStreamBegin();
}

ui64 TMetricsEncoder::Append(const NUdf::TUnboxedValue& value)
{
    TMaybe<TInstant> timestamp = ParseTimestamp(value, Scheme.GetTimestamp());

    if (!timestamp) {
        return Scheme.GetSensors().size();
    }

    for (const auto& sensor : Scheme.GetSensors()) {
        BeginMetric(SolomonEncoder, sensor);

        SolomonEncoder->OnLabelsBegin();
        SolomonEncoder->OnLabel("name", sensor.GetKey());
        for (const auto& label : Scheme.GetLabels()) {
            const NUdf::TUnboxedValue& labelValue = value.GetElement(label.GetIndex());
            SolomonEncoder->OnLabel(label.GetKey(), TStringBuf(labelValue.AsStringRef()));
        }
        SolomonEncoder->OnLabelsEnd();

        const auto& sensorValue = value.GetElement(sensor.GetIndex());
        EncodeSensorValue(SolomonEncoder, *timestamp, sensorValue, sensor);

        SolomonEncoder->OnMetricEnd();
    }

    return Scheme.GetSensors().size();
}

TString TMetricsEncoder::Encode() {
    SolomonEncoder->OnStreamEnd();
    SolomonEncoder->Close();

    TString res = Data;
    BeginNew();
    return res;
}

} // namespace NYql::NDq
