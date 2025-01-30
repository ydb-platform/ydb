#include <library/cpp/testing/gtest/gtest.h>

#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

using namespace NKikimr;

TEST(MetadataConversion, MakeAuthTest) {
    NYql::TExternalSource externalSource;
    auto auth = NKqp::MakeAuth(externalSource);
    ASSERT_TRUE(std::holds_alternative<NExternalSource::NAuth::TNone>(auth));

    externalSource.DataSourceAuth.MutableNone();
    auth = NKqp::MakeAuth(externalSource);
    ASSERT_TRUE(std::holds_alternative<NExternalSource::NAuth::TNone>(auth));

    externalSource.DataSourceAuth.ClearNone();
    auto& serviceAccount = *externalSource.DataSourceAuth.MutableServiceAccount();
    {
        serviceAccount.SetId("sa-id");
        serviceAccount.SetSecretName("sa-name-of-secret");
        externalSource.ServiceAccountIdSignature = "sa-id-signature";
    }
    auth = NKqp::MakeAuth(externalSource);
    ASSERT_TRUE(std::holds_alternative<NExternalSource::NAuth::TServiceAccount>(auth));
    {
        auto& saAuth = std::get<NExternalSource::NAuth::TServiceAccount>(auth);
        ASSERT_EQ(saAuth.ServiceAccountId, "sa-id");
        ASSERT_EQ(saAuth.ServiceAccountIdSignature, "sa-id-signature");
    }

    externalSource.DataSourceAuth.ClearServiceAccount();
    auto& awsAccount = *externalSource.DataSourceAuth.MutableAws();
    {
        awsAccount.SetAwsRegion("aws-test");
        awsAccount.SetAwsAccessKeyIdSecretName("aws-ak-secret-name");
        awsAccount.SetAwsSecretAccessKeySecretName("aws-sak-secret-name");
        externalSource.AwsAccessKeyId = "aws-ak";
        externalSource.AwsSecretAccessKey = "aws-sak";
    }
    auth = NKqp::MakeAuth(externalSource);
    ASSERT_TRUE(std::holds_alternative<NExternalSource::NAuth::TAws>(auth));
    {
        auto& awsAuth = std::get<NExternalSource::NAuth::TAws>(auth);
        ASSERT_EQ(awsAuth.Region, "aws-test");
        ASSERT_EQ(awsAuth.AccessKey, "aws-ak");
        ASSERT_EQ(awsAuth.SecretAccessKey, "aws-sak");
    }
}

TEST(MetadataConversion, ConvertingExternalSourceMetadata) {
    NYql::TExternalSource externalSource{
        .Type = "type",
        .TableLocation = "table-loc",
        .DataSourcePath = "ds-path",
        .DataSourceLocation = "ds-loc",
    };
    THashMap<TString, TString> attributes{{"key1", "val1"}, {"key2", "val2"}};

    std::shared_ptr<NExternalSource::TMetadata> externalMetadata;
    {
        NYql::TKikimrTableMetadata tableMetadata;
        tableMetadata.ExternalSource = externalSource;
        tableMetadata.Attributes = attributes;
        externalMetadata = NKqp::ConvertToExternalSourceMetadata(tableMetadata);
    }
    ASSERT_TRUE(externalMetadata);
    ASSERT_TRUE(std::holds_alternative<NExternalSource::NAuth::TNone>(externalMetadata->Auth));

    NYql::TKikimrTableMetadata tableMetadata;
    ASSERT_TRUE(NKqp::EnrichMetadata(tableMetadata, *externalMetadata));

    ASSERT_EQ(tableMetadata.ExternalSource.Type, externalSource.Type);
    ASSERT_EQ(tableMetadata.ExternalSource.TableLocation, externalSource.TableLocation);
    ASSERT_EQ(tableMetadata.ExternalSource.DataSourcePath, externalSource.DataSourcePath);
    ASSERT_EQ(tableMetadata.ExternalSource.DataSourceLocation, externalSource.DataSourceLocation);
    ASSERT_THAT(tableMetadata.Attributes, testing::ContainerEq(attributes));
}
