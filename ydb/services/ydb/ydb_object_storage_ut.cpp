#include "ydb_common_ut.h"

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <ydb/public/api/grpc/draft/ydb_object_storage_v1.grpc.pb.h>

#include <ydb/public/lib/experimental/ydb_object_storage.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(YdbS3Internal) {

    void PrepareData(TString location) {
        TDriverConfig driverCfg;
        driverCfg.SetDatabase("/Root")
            .SetEndpoint(location);

        auto connection = NYdb::TDriver(driverCfg);

        NYdb::NTable::TTableClient client(connection);
        auto session = client.GetSession().ExtractValueSync().GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Hash", EPrimitiveType::Uint64)
                .AddNullableColumn("Name", EPrimitiveType::Utf8)
                .AddNullableColumn("Path", EPrimitiveType::Utf8)
                .AddNullableColumn("Version", EPrimitiveType::Uint64)
                .AddNullableColumn("Timestamp", EPrimitiveType::Uint64)
                .AddNullableColumn("Data", EPrimitiveType::String)
                .AddNullableColumn("ExtraData", EPrimitiveType::String)
                .AddNullableColumn("Unused1", EPrimitiveType::Uint32);
            tableBuilder.SetPrimaryKeyColumns({"Hash", "Name", "Path", "Version"});
            NYdb::NTable::TCreateTableSettings tableSettings;
            tableSettings.PartitioningPolicy(NYdb::NTable::TPartitioningPolicy().UniformPartitions(32));
            auto result = session.CreateTable("/Root/ListingObjects", tableBuilder.Build(), tableSettings).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Write some rows
        {
            auto res = session.ExecuteDataQuery(
                        "REPLACE INTO `/Root/ListingObjects` (Hash, Name, Path, Version, Timestamp, Data) VALUES\n"
                        "(50, 'bucket50', '/home/Music/Bohemian Rapshody.mp3', 1, 10, 'MP3'),\n"
                        "(50, 'bucket50', '/home/.bashrc', 1, 10, '#bashrc')\n"
                        ";",
                            NYdb::NTable::TTxControl::BeginTx().CommitTx()
                        ).ExtractValueSync();

            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(TestS3Listing) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        PrepareData(location);

        // List
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));
        NObjectStorage::TObjectStorageClient s3conn(connection);

        TValueBuilder keyPrefix;
        keyPrefix.BeginTuple()
                .AddElement().Uint64(50)
                .AddElement().Utf8("bucket50")
                .EndTuple();
        TValueBuilder suffix;
        suffix.BeginTuple().EndTuple();
        auto res = s3conn.List("/Root/ListingObjects",
                                    keyPrefix.Build(),
                                    "/home/",
                                    "/",
                                    "",
                                    suffix.Build(),
                                    100,
                                    {"Name", "Data", "Timestamp"}
            ).GetValueSync();

        Cerr << res.GetStatus() << Endl;
        UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);

        UNIT_ASSERT(!res.GetIsTruncated());
        UNIT_ASSERT(!res.GetContinuationToken());

        {
            UNIT_ASSERT_VALUES_EQUAL(res.GetCommonPrefixes().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(res.GetCommonPrefixes()[0], "/home/Music/");
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(res.GetContents().RowsCount(), 1);
            TResultSetParser parser(res.GetContents());
            UNIT_ASSERT(parser.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Name").GetOptionalUtf8().value(), "bucket50");
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Path").GetOptionalUtf8().value(), "/home/.bashrc");
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Timestamp").GetOptionalUint64().value(), 10);
        }
    }

    Y_UNIT_TEST(PgCompatibility) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableTablePgTypes(true);
        appConfig.MutableFeatureFlags()->SetDisablePgCompatibility(false);
        TKikimrWithGrpcAndRootSchema server(appConfig);
        const TString endpoint = TStringBuilder() << "localhost:" << server.GetPort();
        auto driver = TDriver(TDriverConfig().SetDatabase("/Root").SetEndpoint(endpoint));
        NYdb::NTable::TTableClient client(driver);
        auto session = client.GetSession().ExtractValueSync().GetSession();
        const auto createMixed = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Mixed` (
                Bucket Uint64, Path Utf8, NativeValue Uint64, LegacyValue pgint4,
                PRIMARY KEY (Bucket, Path)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_C(createMixed.IsSuccess(), createMixed.GetIssues().ToString());
        const auto createPgKey = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/PgKey` (
                Bucket pgint4, Path Utf8, NativeValue Uint64,
                PRIMARY KEY (Bucket, Path)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_C(createPgKey.IsSuccess(), createPgKey.GetIssues().ToString());
        const auto upsert = session.ExecuteDataQuery(
            "UPSERT INTO `/Root/Mixed` (Bucket, Path, NativeValue, LegacyValue) VALUES (50ul, \"/home/file\"u, 7ul, 42p);",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        server.GetRuntime()->GetAppData().FeatureFlags.SetDisablePgCompatibility(true);

        auto channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
        auto stub = Ydb::ObjectStorage::V1::ObjectStorageService::NewStub(channel);
        const auto list = [&](const Ydb::ObjectStorage::ListingRequest& input) {
            grpc::ClientContext context;
            Ydb::ObjectStorage::ListingResponse response;
            UNIT_ASSERT(stub->List(&context, input, &response).ok());
            return response;
        };

        Ydb::ObjectStorage::ListingRequest request;
        request.set_table_name("/Root/Mixed");
        request.set_path_column_prefix("/home/");
        request.set_max_keys(100);
        request.add_columns_to_return("NativeValue");
        request.mutable_key_prefix()->mutable_type()->mutable_tuple_type()->add_elements()->set_type_id(Ydb::Type::UINT64);
        request.mutable_key_prefix()->mutable_value()->add_items()->set_uint64_value(50);
        request.mutable_start_after_key_suffix()->mutable_type()->mutable_tuple_type();

        const auto success = list(request);
        UNIT_ASSERT_VALUES_EQUAL_C(success.status(), Ydb::StatusIds::SUCCESS, success.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(success.contents().rows_size(), 1);

        const auto expectPgRejected = [&](const Ydb::ObjectStorage::ListingRequest& input, const TString& issue) {
            const auto response = list(input);
            UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::SCHEME_ERROR, response.DebugString());
            UNIT_ASSERT_C(!response.issues().empty(), response.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(response.issues(0).message(), issue);
        };

        auto pgProjection = request;
        pgProjection.add_columns_to_return("LegacyValue");
        expectPgRejected(pgProjection, "PostgreSQL compatibility is disabled for column 'LegacyValue'");

        auto pgFilter = request;
        auto* filter = pgFilter.mutable_matching_filter();
        auto* filterType = filter->mutable_type()->mutable_tuple_type();
        filterType->add_elements()->mutable_list_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        filterType->add_elements()->mutable_list_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        filterType->add_elements()->mutable_tuple_type()->add_elements()->mutable_pg_type()->set_type_name("pgint4");
        filter->mutable_value()->add_items()->add_items()->set_text_value("LegacyValue");
        filter->mutable_value()->add_items()->add_items()->set_uint32_value(Ydb::ObjectStorage::ListingRequest::EQUAL);
        filter->mutable_value()->add_items()->add_items()->set_text_value("42");
        expectPgRejected(pgFilter, "PostgreSQL compatibility is disabled for column 'LegacyValue'");

        auto pgKey = request;
        pgKey.set_table_name("/Root/PgKey");
        pgKey.mutable_key_prefix()->mutable_type()->mutable_tuple_type()->mutable_elements(0)->mutable_pg_type()->set_type_name("pgint4");
        pgKey.mutable_key_prefix()->mutable_value()->mutable_items(0)->set_text_value("50");
        expectPgRejected(pgKey, "PostgreSQL compatibility is disabled for column 'Bucket'");

        auto pgPrefix = request;
        auto* prefixType = pgPrefix.mutable_key_prefix()->mutable_type()->mutable_tuple_type()->mutable_elements(0);
        prefixType->mutable_pg_type()->set_type_name("unrecognized_pg_type");
        expectPgRejected(pgPrefix, "PostgreSQL compatibility is disabled for KeyPrefix types");

        auto pgSuffix = request;
        auto* suffixType = pgSuffix.mutable_start_after_key_suffix()->mutable_type()->mutable_tuple_type()->add_elements();
        suffixType->mutable_optional_type()->mutable_item()->mutable_pg_type()->set_type_name("unrecognized_pg_type");
        pgSuffix.mutable_start_after_key_suffix()->mutable_value()->add_items()->set_text_value("/home/file");
        expectPgRejected(pgSuffix, "PostgreSQL compatibility is disabled for StartAfterKeySuffix types");

        filter->mutable_value()->mutable_items(0)->mutable_items(0)->set_text_value("NativeValue");
        filterType->mutable_elements(2)->mutable_tuple_type()->mutable_elements(0)->mutable_pg_type()->set_type_name("unrecognized_pg_type");
        expectPgRejected(pgFilter, "PostgreSQL compatibility is disabled for matching_filter types");
    }

    void SetPermissions(TString location) {
        TDriverConfig driverCfg;
        driverCfg.SetDatabase("/Root")
            .SetEndpoint(location);

        auto connection = NYdb::TDriver(driverCfg);
        auto scheme = NYdb::NScheme::TSchemeClient(connection);
        auto status = scheme.ModifyPermissions("/Root/ListingObjects",
                                               NYdb::NScheme::TModifyPermissionsSettings()
                                               .AddSetPermissions(
                                                   NYdb::NScheme::TPermissions("reader@builtin", {"ydb.tables.read"})
                                                   )
                                               .AddSetPermissions(
                                                   NYdb::NScheme::TPermissions("generic_reader@builtin", {"ydb.generic.read"})
                                                   )
                                               .AddSetPermissions(
                                                   NYdb::NScheme::TPermissions("writer@builtin", {"ydb.tables.modify"})
                                                   )
                                               .AddSetPermissions(
                                                   NYdb::NScheme::TPermissions("generic_writer@builtin", {"ydb.generic.write"})
                                                   )
                                               ).ExtractValueSync();
        UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
    }

    NYdb::EStatus MakeListingRequest(TString location, TString userToken) {
        TDriverConfig driverCfg;
        driverCfg.SetDatabase("/Root")
            .SetEndpoint(location)
            .SetAuthToken(userToken);

        auto connection = NYdb::TDriver(driverCfg);
        NObjectStorage::TObjectStorageClient s3conn(connection);

        TValueBuilder keyPrefix;
        keyPrefix.BeginTuple()
                .AddElement().Uint64(50)
                .AddElement().Utf8("bucket50")
                .EndTuple();
        TValueBuilder suffix;
        suffix.BeginTuple().EndTuple();
        auto res = s3conn.List("/Root/ListingObjects",
                                    keyPrefix.Build(),
                                    "/home/",
                                    "/",
                                    "",
                                    suffix.Build(),
                                    100,
                                    {"Name", "Data", "Timestamp"}
            ).GetValueSync();

        return res.GetStatus();
    }

    Y_UNIT_TEST(TestAccessCheck) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        PrepareData(location);
        SetPermissions(location);
        server.ResetSchemeCache("/Root/ListingObjects");

        UNIT_ASSERT_EQUAL(MakeListingRequest(location, ""), EStatus::SUCCESS);
        UNIT_ASSERT_EQUAL(MakeListingRequest(location, "reader@builtin"), EStatus::SUCCESS);
        UNIT_ASSERT_EQUAL(MakeListingRequest(location, "generic_reader@builtin"), EStatus::SUCCESS);
        UNIT_ASSERT_EQUAL(MakeListingRequest(location, "root@builtin"), EStatus::SUCCESS);

        UNIT_ASSERT_EQUAL(MakeListingRequest(location, "writer@builtin"), EStatus::UNAUTHORIZED);
        UNIT_ASSERT_EQUAL(MakeListingRequest(location, "generic_writer@builtin"), EStatus::UNAUTHORIZED);
        UNIT_ASSERT_EQUAL(MakeListingRequest(location, "badguy@builtin"), EStatus::UNAUTHORIZED);
    }

    NYdb::EStatus TestRequest(NObjectStorage::TObjectStorageClient s3conn, TValue&& keyPrefix, TValue&& suffix) {
        auto res = s3conn.List("/Root/ListingObjects",
                                    std::move(keyPrefix),
                                    "/home/",
                                    "/",
                                    "",
                                    std::move(suffix),
                                    100,
                                    {"Name", "Data", "Timestamp"}
            ).GetValueSync();

        return res.GetStatus();
    }

    // Test request with good suffix
    NYdb::EStatus TestKeyPrefixRequest(NObjectStorage::TObjectStorageClient s3conn, TValue&& keyPrefix) {
        return TestRequest(s3conn,
                           std::move(keyPrefix),
                           TValueBuilder().BeginTuple().EndTuple().Build());
    }

    // Test request with good keyPrefix
    NYdb::EStatus TestKeySuffixRequest(NObjectStorage::TObjectStorageClient s3conn, TValue&& keySuffix) {
        return TestRequest(s3conn,
                           TValueBuilder()
                              .BeginTuple()
                                  .AddElement().Uint64(1)
                                  .AddElement().Utf8("Bucket50")
                              .EndTuple().Build(),
                           std::move(keySuffix));
    }

    Y_UNIT_TEST(BadRequests) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        PrepareData(location);

        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));
        NObjectStorage::TObjectStorageClient s3conn(connection);

        UNIT_ASSERT_VALUES_EQUAL(TestKeyPrefixRequest(s3conn,
                                                      TValueBuilder()
                                                        .BeginTuple()
                                                            .AddElement().Uint64(1)
                                                            .AddElement().Utf8("Bucket50")
                                                        .EndTuple().Build()),
                                 EStatus::SUCCESS);


        UNIT_ASSERT_VALUES_EQUAL(TestKeyPrefixRequest(s3conn,
                                                      TValueBuilder().Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeyPrefixRequest(s3conn,
                                                      TValueBuilder()
                                                        .BeginTuple()
                                                            .AddElement().BeginList().EndList()
                                                            .AddElement().Utf8("Bucket50")
                                                        .EndTuple().Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeyPrefixRequest(s3conn,
                                             TValueBuilder().BeginStruct().EndStruct().Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeyPrefixRequest(s3conn,
                                             TValueBuilder().BeginList().EndList().Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeyPrefixRequest(s3conn,
                                             TValueBuilder()
                                                .BeginList()
                                                    .AddListItem().Uint64(1)
                                                    .AddListItem().Uint64(22)
                                                .EndList().Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeyPrefixRequest(s3conn,
                                             TValueBuilder().Uint64(50).Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeyPrefixRequest(s3conn,
                                             TValueBuilder().Uint64(50).Build()),
                                 EStatus::BAD_REQUEST);


        UNIT_ASSERT_VALUES_EQUAL(TestKeySuffixRequest(s3conn,
                                             TValueBuilder().BeginTuple().EndTuple().Build()),
                                 EStatus::SUCCESS);

        UNIT_ASSERT_VALUES_EQUAL(TestKeySuffixRequest(s3conn,
                                             TValueBuilder().Build()),
                                 EStatus::SUCCESS);

        UNIT_ASSERT_VALUES_EQUAL(TestKeySuffixRequest(s3conn,
                                             TValueBuilder().BeginStruct().EndStruct().Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeySuffixRequest(s3conn,
                                             TValueBuilder().BeginList().EndList().Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeySuffixRequest(s3conn,
                                             TValueBuilder()
                                                      .BeginList()
                                                          .AddListItem().Uint64(1)
                                                          .AddListItem().Uint64(22)
                                                      .EndList().Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeySuffixRequest(s3conn,
                                             TValueBuilder().Uint64(50).Build()),
                                 EStatus::BAD_REQUEST);

        UNIT_ASSERT_VALUES_EQUAL(TestKeySuffixRequest(s3conn,
                                             TValueBuilder().Uint64(50).Build()),
                                 EStatus::BAD_REQUEST);
    }
}
