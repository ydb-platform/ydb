#include <library/cpp/testing/gtest/gtest.h>

#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

using namespace NKikimr;

namespace {

NYql::TExternalDataSource MakeDataSource(const TString& type,
    const NKikimrSchemeOp::TAuth& auth, const TString& dataSourcePath = {},
    const TString& location = {}, const TString& installation = {})
{
    NKikimrSchemeOp::TExternalDataSourceDescription description;
    description.SetSourceType(type);
    description.SetLocation(location);
    description.SetInstallation(installation);
    *description.MutableAuth() = auth;
    return NYql::TExternalDataSource::CreateFromDescription(description, dataSourcePath);
}

} // anonymous namespace

TEST(MetadataConversion, MakeAuthTest) {
    NKikimrSchemeOp::TAuth noneAuthProto;
    noneAuthProto.MutableNone();
    NYql::TExternalDataSource externalSource = MakeDataSource("test", noneAuthProto);
    auto auth = externalSource.MakeExternalSourceMetadata().Auth;
    ASSERT_TRUE(std::holds_alternative<NExternalSource::NAuth::TNone>(auth));

    {
        NKikimrSchemeOp::TAuth authProto;
        auto* sa = authProto.MutableServiceAccount();
        sa->SetId("sa-id");
        sa->SetSecretName("sa-name-of-secret");
        externalSource = MakeDataSource("test", authProto);
    }
    externalSource.InitSecretValues({"sa-id-signature"});
    auth = externalSource.MakeExternalSourceMetadata().Auth;
    ASSERT_TRUE(std::holds_alternative<NExternalSource::NAuth::TServiceAccount>(auth));
    {
        auto& saAuth = std::get<NExternalSource::NAuth::TServiceAccount>(auth);
        ASSERT_EQ(saAuth.ServiceAccountId, "sa-id");
        ASSERT_EQ(saAuth.ServiceAccountIdSignature, "sa-id-signature");
    }

    {
        NKikimrSchemeOp::TAuth authProto;
        auto* aws = authProto.MutableAws();
        aws->SetAwsAccessKeyIdSecretName("aws-ak-secret-name");
        aws->SetAwsSecretAccessKeySecretName("aws-sak-secret-name");
        aws->SetAwsRegion("aws-region");
        externalSource = MakeDataSource("test", authProto);
    }
    externalSource.InitSecretValues({"aws-ak", "aws-sak"});
    auth = externalSource.MakeExternalSourceMetadata().Auth;
    ASSERT_TRUE(std::holds_alternative<NExternalSource::NAuth::TAws>(auth));
    {
        auto& awsAuth = std::get<NExternalSource::NAuth::TAws>(auth);
        ASSERT_EQ(awsAuth.Region, "aws-region");
        ASSERT_EQ(awsAuth.AccessKey, "aws-ak");
        ASSERT_EQ(awsAuth.SecretAccessKey, "aws-sak");
    }
}

TEST(MetadataConversion, ExternalDataSourceMetadataConversion) {
    NKikimrSchemeOp::TAuth auth;
    auth.MutableNone();
    auto source = MakeDataSource("type", auth, "ds-path", "ds-loc", "installation");
    auto externalMetadata = source.MakeExternalSourceMetadata();
    externalMetadata.Attributes = {{"key1", "val1"}, {"key2", "val2"}};

    EXPECT_TRUE(externalMetadata.TableLocation.empty());
    EXPECT_EQ(externalMetadata.DataSourceLocation, "ds-loc");
    EXPECT_EQ(externalMetadata.DataSourcePath, "ds-path");
    EXPECT_EQ(externalMetadata.Type, "type");
    ASSERT_TRUE(std::holds_alternative<NExternalSource::NAuth::TNone>(externalMetadata.Auth));
}

TEST(MetadataConversion, InferredMetadataUpdateIsAtomic) {
    NKikimrSchemeOp::TAuth auth;
    auth.MutableNone();
    NYql::TExternalDataSource source = MakeDataSource("ObjectStorage", auth, "original");

    EXPECT_ANY_THROW(source.ApplyInferredMetadata("", "changed"));
    EXPECT_EQ(source.GetType(), "ObjectStorage");
    EXPECT_EQ(source.GetDataSourcePath(), "original");

    source.ApplyInferredMetadata("ObjectStorage", "inferred-path");
    EXPECT_EQ(source.GetType(), "ObjectStorage");
    EXPECT_EQ(source.GetDataSourcePath(), "inferred-path");
}

TEST(MetadataConversion, YdbDataSourceCanUseDatabaseIdWithoutLocation) {
    NKikimrSchemeOp::TExternalDataSourceDescription description;
    description.SetSourceType("Ydb");
    description.MutableAuth()->MutableNone();
    (*description.MutableProperties()->mutable_properties())["database_id"] = "test-database-id";

    auto source = NYql::TExternalDataSource::CreateFromDescription(description, "source-path");
    EXPECT_TRUE(source.IsYdb());
    EXPECT_TRUE(source.GetLocation().empty());
    source.ApplyInferredMetadata("Ydb", "inferred-path");
    EXPECT_EQ(source.GetDataSourcePath(), "inferred-path");
    EXPECT_EQ(source.BuildConnectorProperties().at("database_id"), "test-database-id");
}

TEST(MetadataConversion, AuthPropertiesOverrideSourceProperties) {
    NKikimrSchemeOp::TExternalDataSourceDescription description;
    description.SetSourceType("ObjectStorage");
    auto* auth = description.MutableAuth()->MutableBasic();
    auth->SetLogin("user");
    auth->SetPasswordSecretName("password-secret");
    auto* properties = description.MutableProperties()->mutable_properties();
    (*properties)["authMethod"] = "NONE";
    (*properties)["password"] = "spoofed";

    auto source = NYql::TExternalDataSource::CreateFromDescription(description, "source-path");
    source.InitSecretValues({"resolved-password"});
    auto connectorProperties = source.BuildConnectorProperties();
    EXPECT_EQ(connectorProperties.at("authMethod"), "BASIC");
    EXPECT_EQ(connectorProperties.at("password"), "resolved-password");
}

TEST(MetadataConversion, ExternalTableEnrichmentIsOneWay) {
    NKikimrSchemeOp::TExternalTableDescription description;
    description.SetSourceType("ObjectStorage");
    description.SetLocation("table-location");
    description.SetDataSourcePath("declared-source");
    auto table = NYql::TExternalTable::CreateFromDescription(description);
    EXPECT_EQ(table.GetDataSourcePath(), "declared-source");
    NKikimrSchemeOp::TAuth auth;
    auth.MutableNone();
    auto sourceMetadata = MakeIntrusive<NYql::TKikimrTableMetadata>();
    EXPECT_ANY_THROW(table.InitExternalDataSource({}));
    EXPECT_ANY_THROW(table.InitExternalDataSource(sourceMetadata));
    EXPECT_ANY_THROW(table.GetUnderlyingDataSource());
    EXPECT_ANY_THROW(table.GetUnderlyingDataSourceMetadata());

    sourceMetadata->ExternalSource = MakeDataSource("Ydb", auth, {}, "location");
    EXPECT_ANY_THROW(table.InitExternalDataSource(sourceMetadata));

    sourceMetadata->ExternalSource = MakeDataSource("ObjectStorage", auth, "resolved-source", "source-location");
    table.InitExternalDataSource(sourceMetadata);
    EXPECT_EQ(table.GetUnderlyingDataSource().GetType(), "ObjectStorage");
    EXPECT_EQ(table.GetDataSourcePath(), "resolved-source");
    EXPECT_EQ(table.GetLocation(), "table-location");
    EXPECT_EQ(table.GetUnderlyingDataSource().GetLocation(), "source-location");
    EXPECT_EQ(table.GetUnderlyingDataSourceMetadata(), sourceMetadata);
    EXPECT_ANY_THROW(table.InitExternalDataSource(sourceMetadata));

    sourceMetadata->ExternalDataSource().ApplyInferredMetadata("ObjectStorage", "updated-source");
    EXPECT_EQ(table.GetDataSourcePath(), "updated-source");
}

TEST(MetadataConversion, ObjectKindCanOnlyBeInitializedOnceForYdbSource) {
    using EKind = NYql::TExternalDataSource::EKind;

    NKikimrSchemeOp::TAuth auth;
    auth.MutableNone();
    auto source = MakeDataSource("Ydb", auth, "source-path", "grpc://example.com");
    EXPECT_ANY_THROW(source.InitObjectKind(EKind::Unknown));
    source.InitObjectKind(EKind::Topic);
    EXPECT_EQ(source.GetType(), "Ydb");
    EXPECT_TRUE(source.IsYdbTopics());
    EXPECT_EQ(source.GetDataSourcePath(), "source-path");
    EXPECT_ANY_THROW(source.InitObjectKind(EKind::Topic));

    auto tableSource = MakeDataSource("Ydb", auth, "table-path", "grpc://example.com");
    tableSource.InitObjectKind(EKind::Table);
    EXPECT_ANY_THROW(tableSource.InitObjectKind(EKind::Table));
    EXPECT_ANY_THROW(tableSource.InitObjectKind(EKind::Topic));

    auto objectStorageSource = MakeDataSource("ObjectStorage", auth);
    EXPECT_ANY_THROW(objectStorageSource.InitObjectKind(EKind::Table));
    EXPECT_ANY_THROW(objectStorageSource.InitObjectKind(EKind::Topic));
}

TEST(MetadataConversion, SecretsCanOnlyBeSetOnce) {
    NKikimrSchemeOp::TAuth auth;
    auth.MutableServiceAccount()->SetId("sa-id");
    auto source = MakeDataSource("ObjectStorage", auth);
    EXPECT_ANY_THROW(source.BuildConnectorProperties());
    ASSERT_NO_THROW(source.InitSecretValues({"signature"}));
    EXPECT_EQ(source.BuildConnectorProperties().at("serviceAccountIdSignature"), "signature");
    EXPECT_ANY_THROW(source.InitSecretValues({"another-signature"}));

    auto invalidSource = MakeDataSource("ObjectStorage", auth);
    EXPECT_ANY_THROW(invalidSource.InitSecretValues({}));
    EXPECT_ANY_THROW(invalidSource.InitSecretValues({"signature"}));

    NKikimrSchemeOp::TAuth noneAuth;
    noneAuth.MutableNone();
    auto noSecretsSource = MakeDataSource("ObjectStorage", noneAuth);
    ASSERT_NO_THROW(noSecretsSource.InitSecretValues({}));
    EXPECT_ANY_THROW(noSecretsSource.InitSecretValues({}));
}
