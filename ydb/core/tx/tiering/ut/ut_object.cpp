#include <ydb/core/tx/tiering/tier/object.h>
#include <ydb/core/tx/tiering/tier/s3_uri.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NColumnShard;

Y_UNIT_TEST_SUITE(S3SettingsConversion) {
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
            UseVirtualAddressing: false
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
            UseVirtualAddressing: false
        )", &output));
        ValidateConversion(input, output);
    }

    Y_UNIT_TEST(FoldersStrictStyle) {
        std::vector<TString> uris = {
            "http://s3.yandexcloud.net:8080/my-folder/subfolder/bucket",
            "http://bucket.s3.yandexcloud.net:8080/my-folder/subfolder",
        };
        for (const auto& input : uris) {
            NTiers::TS3Uri uri = NTiers::TS3Uri::ParseUri(input).DetachResult();
            UNIT_ASSERT_STRINGS_EQUAL_C(uri.GetEndpoint(), "s3.yandexcloud.net:8080/my-folder/subfolder", input);
            UNIT_ASSERT_STRINGS_EQUAL_C(uri.GetBucket(), "bucket", input);
        }
    }

    Y_UNIT_TEST(FoldersStyleDeduction) {
        std::vector<TString> uris = {
            "http://storage.yandexcloud.net:8080/my-folder/subfolder/bucket",
            "http://storage.yandexcloud.net:8080///my-folder/subfolder/bucket//",
        };
        for (const auto& input : uris) {
            NTiers::TS3Uri uri = NTiers::TS3Uri::ParseUri(input).DetachResult();
            UNIT_ASSERT_STRINGS_EQUAL_C(uri.GetEndpoint(), "storage.yandexcloud.net:8080/my-folder/subfolder", input);
            UNIT_ASSERT_STRINGS_EQUAL_C(uri.GetBucket(), "bucket", input);
        }
    }

    Y_UNIT_TEST(StyleDeduction) {
        std::vector<TString> uris = {
            "http://storage.yandexcloud.net/bucket",
            "http://my-s3.net/bucket",
            "http://bucket.my-s3.net",
            "http://bucket.my-s3.net/",
        };
        for (const auto& input : uris) {
            NTiers::TS3Uri uri = NTiers::TS3Uri::ParseUri(input).DetachResult();
            UNIT_ASSERT_STRINGS_EQUAL_C(uri.GetBucket(), "bucket", input);
        }
    }
}

}   // namespace NKikimr
