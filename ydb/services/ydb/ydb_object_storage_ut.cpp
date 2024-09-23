#include "ydb_common_ut.h"

#include <ydb/public/lib/experimental/ydb_object_storage.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(YdbS3Internal) {

    void PrepareData(TString location) {
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

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
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Name").GetOptionalUtf8().GetRef(), "bucket50");
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Path").GetOptionalUtf8().GetRef(), "/home/.bashrc");
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Timestamp").GetOptionalUint64().GetRef(), 10);
        }
    }

    void SetPermissions(TString location) {
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));
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
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location).SetAuthToken(userToken));
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
