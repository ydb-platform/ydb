#include "ut_helpers.h"

#include <ydb/library/yql/providers/solomon/actors/dq_solomon_actors_util.h>

#include <yql/essentials/public/udf/udf_data_type.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

namespace {

constexpr ui16 Days = 19000; // 2022-01-08
const TInstant Expected = TInstant::Days(Days);

TString EncodeOnePoint(NUdf::TDataTypeId timestampType, NUdf::TUnboxedValuePod timestamp, bool cloudFormat) {
    TScopedAlloc alloc(__LOCATION__);
    TMemoryUsageInfo memInfo("TMetricsEncoderTimestampTest");
    THolderFactory holderFactory(alloc.Ref(), memInfo);

    NSo::NProto::TDqSolomonShardScheme scheme;
    scheme.MutableTimestamp()->SetKey("ts");
    scheme.MutableTimestamp()->SetIndex(0);
    scheme.MutableTimestamp()->SetDataTypeId(timestampType);

    auto& sensor = *scheme.MutableSensors()->Add();
    sensor.SetKey("sensor");
    sensor.SetIndex(1);
    sensor.SetDataTypeId(NUdf::TDataType<ui32>::Id);

    TMetricsEncoder encoder(scheme, cloudFormat);
    encoder.Append(CreateStruct(holderFactory, {timestamp, NUdf::TUnboxedValuePod(ui32(42))}));
    return encoder.Encode();
}

TString EncodeAsTimestamp(bool cloudFormat) {
    return EncodeOnePoint(NUdf::TDataType<NUdf::TTimestamp>::Id, NUdf::TUnboxedValuePod(ui64(Expected.MicroSeconds())), cloudFormat);
}

void CheckSameAsTimestamp(NUdf::TDataTypeId timestampType, NUdf::TUnboxedValuePod timestamp) {
    for (const bool cloudFormat : {false, true}) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            EncodeOnePoint(timestampType, timestamp, cloudFormat),
            EncodeAsTimestamp(cloudFormat),
            "cloudFormat: " << cloudFormat);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TMetricsEncoderTimestampTest) {
    Y_UNIT_TEST(TimestampIsEncodedAsUnixSeconds) {
        UNIT_ASSERT_STRING_CONTAINS(EncodeAsTimestamp(/* cloudFormat */ false), TStringBuilder() << "\"ts\":" << Expected.Seconds());
    }

    Y_UNIT_TEST(Date) {
        CheckSameAsTimestamp(NUdf::TDataType<NUdf::TDate>::Id, NUdf::TUnboxedValuePod(Days));
    }

    Y_UNIT_TEST(TzDate) {
        NUdf::TUnboxedValuePod value(Days);
        value.SetTimezoneId(1);
        CheckSameAsTimestamp(NUdf::TDataType<NUdf::TTzDate>::Id, value);
    }

    Y_UNIT_TEST(Datetime) {
        CheckSameAsTimestamp(NUdf::TDataType<NUdf::TDatetime>::Id, NUdf::TUnboxedValuePod(ui32(Expected.Seconds())));
    }

    Y_UNIT_TEST(TzDatetime) {
        NUdf::TUnboxedValuePod value(ui32(Expected.Seconds()));
        value.SetTimezoneId(1);
        CheckSameAsTimestamp(NUdf::TDataType<NUdf::TTzDatetime>::Id, value);
    }
}

} // namespace NYql::NDq
