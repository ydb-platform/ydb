#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>

static constexpr ui64 MicroSecondsInSecond = 1000 * 1000;
static constexpr ui64 NanoSecondsInMicroSecond = 1000;

Y_UNIT_TEST_SUITE(TCastTest) {
    Y_UNIT_TEST(TimestampFromProto) {
        const ui64 now = TInstant::Now().MicroSeconds();

        google::protobuf::Timestamp timestamp;
        timestamp.set_seconds(now / MicroSecondsInSecond);
        timestamp.set_nanos((now % MicroSecondsInSecond) * NanoSecondsInMicroSecond);

        const TInstant instant = NProtoInterop::CastFromProto(timestamp);
        UNIT_ASSERT_EQUAL(instant.MicroSeconds(), now);
    }

    Y_UNIT_TEST(DurationFromProto) {
        const ui64 now = TInstant::Now().MicroSeconds();

        google::protobuf::Duration message;
        message.set_seconds(now / MicroSecondsInSecond);
        message.set_nanos((now % MicroSecondsInSecond) * NanoSecondsInMicroSecond);

        const TDuration duration = NProtoInterop::CastFromProto(message);
        UNIT_ASSERT_EQUAL(duration.MicroSeconds(), now);
    }

    Y_UNIT_TEST(TimestampToProto) {
        const TInstant instant = TInstant::Now();

        google::protobuf::Timestamp timestamp = NProtoInterop::CastToProto(instant);
        const ui64 microSeconds = timestamp.seconds() * MicroSecondsInSecond +
                                  timestamp.nanos() / NanoSecondsInMicroSecond;

        UNIT_ASSERT_EQUAL(instant.MicroSeconds(), microSeconds);
    }

    Y_UNIT_TEST(DurationToProto) {
        const TDuration duration = TDuration::Seconds(TInstant::Now().Seconds() / 2);

        google::protobuf::Duration message = NProtoInterop::CastToProto(duration);
        const ui64 microSeconds = message.seconds() * MicroSecondsInSecond +
                                  message.nanos() / NanoSecondsInMicroSecond;

        UNIT_ASSERT_EQUAL(duration.MicroSeconds(), microSeconds);
    }
}
