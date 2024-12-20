#include <ydb/core/tx/tiering/tier/object.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NColumnShard;

Y_UNIT_TEST_SUITE(S3SettingsConvertion) {
    void ValidateConversion(
        const NKikimrSchemeOp::TExternalDataSourceDescription& input, TConclusion<const NKikimrSchemeOp::TS3Settings> expectedResult) {
        NTiers::TTierConfig config;
        TConclusionStatus status = config.DeserializeFromProto(input);
        if (expectedResult.IsFail()) {
            UNIT_ASSERT(status.IsFail());
            UNIT_ASSERT_VALUES_EQUAL(expectedResult.GetErrorMessage(), status.GetErrorMessage());
        } else {
            UNIT_ASSERT(status.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(expectedResult->DebugString(), config.GetProtoConfig().DebugString());
        }
    }

    Y_UNIT_TEST(Basic) {
        NKikimrSchemeOp::TExternalDataSourceDescription input;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            SourceType: "ObjectStorage"
            Location: "http://storage.yandexcloud.net/my-bucket"
            Auth {
                Aws {
                    AwsAccessKeyIdSecretName: "access-key"
                    AwsSecretAccessKeySecretName: "secret-key"
                    AwsRegion: "ru-central1"
                }
            }
        )", &input));
        NKikimrSchemeOp::TS3Settings output;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            Scheme: HTTP
            Endpoint: "storage.yandexcloud.net"
            Bucket: "my-bucket"
            SecretKey: "SId:secret-key"
            AccessKey: "SId:access-key"
            Region: "ru-central1"
        )", &output));
        ValidateConversion(input, output);
    }

    Y_UNIT_TEST(Port) {
        NKikimrSchemeOp::TExternalDataSourceDescription input;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            SourceType: "ObjectStorage"
            Location: "http://storage.yandexcloud.net:12345/my-bucket"
            Auth {
                Aws {
                    AwsAccessKeyIdSecretName: "access-key"
                    AwsSecretAccessKeySecretName: "secret-key"
                    AwsRegion: "ru-central1"
                }
            }
        )", &input));
        NKikimrSchemeOp::TS3Settings output;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            Scheme: HTTP
            Endpoint: "storage.yandexcloud.net:12345"
            Bucket: "my-bucket"
            SecretKey: "SId:secret-key"
            AccessKey: "SId:access-key"
            Region: "ru-central1"
        )", &output));
        ValidateConversion(input, output);
    }
}

}   // namespace NKikimr
