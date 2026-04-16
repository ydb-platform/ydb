#include <ydb/services/ydb/ut_common/ydb_ut_test_includes.h>
#include <ydb/services/ydb/ut_common/ydb_ut_common.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

Y_UNIT_TEST_SUITE(TGRpcNewClient) {
    Y_UNIT_TEST(SimpleYqlQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        //TDriver
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        auto client = NYdb::NTable::TTableClient(connection);

        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                auto session = sessionValue.GetSession();

                auto createTableHandler =
                    [session, client] (NThreading::TFuture<TStatus> future) mutable {
                        const auto& createTableResult = future.GetValue();
                        UNIT_ASSERT(!createTableResult.IsTransportError());
                        auto sqlResultHandler =
                            [](TAsyncDataQueryResult future) mutable {
                                auto sqlResultSets = future.ExtractValue();
                                UNIT_ASSERT(!sqlResultSets.IsTransportError());

                                auto resultSets = sqlResultSets.GetResultSets();
                                UNIT_ASSERT_EQUAL(resultSets.size(), 1);
                                auto& resultSet = resultSets[0];
                                UNIT_ASSERT_EQUAL(resultSet.ColumnsCount(), 1);
                                auto meta = resultSet.GetColumnsMeta();
                                UNIT_ASSERT_EQUAL(meta.size(), 1);
                                UNIT_ASSERT_EQUAL(meta[0].Name, "colName");
                                TTypeParser parser(meta[0].Type);
                                UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Primitive);
                                UNIT_ASSERT(parser.GetPrimitive() == EPrimitiveType::Int32);

                                TResultSetParser rsParser(resultSet);
                                while (rsParser.TryNextRow()) {
                                    UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetInt32(), 42);
                                }
                            };

                        session.ExecuteDataQuery(
                            "SELECT 42 as colName;", TTxControl::BeginTx(
                                TTxSettings::SerializableRW()).CommitTx()
                        ).Apply(std::move(sqlResultHandler)).Wait();
                    };

                auto tableBuilder = client.GetTableBuilder();
                tableBuilder
                    .AddNullableColumn("Key", EPrimitiveType::Int32)
                    .AddNullableColumn("Value", EPrimitiveType::String);
                tableBuilder.SetPrimaryKeyColumn("Key");
                session.CreateTable("/Root/FooTable", tableBuilder.Build()).Apply(createTableHandler).Wait();
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
    }

    Y_UNIT_TEST(TestAuth) {
        TKikimrWithGrpcAndRootSchemaWithAuthAndSsl server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetAuthToken("test_user@builtin")
                .UseSecureConnection(TKikimrTestWithAuthAndSsl::GetCaCrt())
                .SetDatabase("/Root")
                .SetEndpoint(location));

        auto client = NYdb::NTable::TTableClient(connection);
        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                UNIT_ASSERT_EQUAL(sessionValue.GetStatus(), EStatus::SUCCESS);
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
        connection.Stop(true);
    }

    Y_UNIT_TEST(YqlQueryWithParams) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        //TDriver
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        auto client = NYdb::NTable::TTableClient(connection);

        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                auto session = sessionValue.GetSession();

                auto prepareQueryHandler =
                    [session, client] (TAsyncPrepareQueryResult future) mutable {
                        const auto& prepareQueryResult = future.GetValue();
                        UNIT_ASSERT(!prepareQueryResult.IsTransportError());
                        UNIT_ASSERT_EQUAL(prepareQueryResult.GetStatus(), EStatus::SUCCESS);
                        auto query = prepareQueryResult.GetQuery();
                        auto paramsBuilder = client.GetParamsBuilder();
                        auto& param = paramsBuilder.AddParam("$paramName");
                        param.String("someString").Build();

                        auto sqlResultHandler =
                            [](TAsyncDataQueryResult future) mutable {
                                auto sqlResultSets = future.ExtractValue();
                                UNIT_ASSERT(!sqlResultSets.IsTransportError());
                                UNIT_ASSERT(sqlResultSets.IsSuccess());

                                auto resultSets = sqlResultSets.GetResultSets();
                                UNIT_ASSERT_EQUAL(resultSets.size(), 1);
                                auto& resultSet = resultSets[0];
                                UNIT_ASSERT_EQUAL(resultSet.ColumnsCount(), 1);
                                auto meta = resultSet.GetColumnsMeta();
                                UNIT_ASSERT_EQUAL(meta.size(), 1);
                                TTypeParser parser(meta[0].Type);
                                UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Primitive);
                                UNIT_ASSERT(parser.GetPrimitive() == EPrimitiveType::String);

                                TResultSetParser rsParser(resultSet);
                                while (rsParser.TryNextRow()) {
                                    UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetString(), "someString");
                                }
                            };

                        query.Execute(TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx(),
                            paramsBuilder.Build()).Apply(sqlResultHandler).Wait();
                    };

                session.PrepareDataQuery("DECLARE $paramName AS String; SELECT $paramName;").Apply(prepareQueryHandler).Wait();
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
    }

    Y_UNIT_TEST(YqlExplainDataQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        auto client = NYdb::NTable::TTableClient(connection);
        bool done = false;

        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client, &done] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                UNIT_ASSERT_EQUAL(sessionValue.GetStatus(), EStatus::SUCCESS);
                auto session = sessionValue.GetSession();

                auto schemeQueryHandler =
                    [session, &done] (const NYdb::TAsyncStatus& future) mutable {
                        const auto& schemeQueryResult = future.GetValue();
                        UNIT_ASSERT(!schemeQueryResult.IsTransportError());
                        UNIT_ASSERT_EQUAL(schemeQueryResult.GetStatus(), EStatus::SUCCESS);

                        auto sqlResultHandler =
                            [&done] (const TAsyncExplainDataQueryResult& future) mutable {
                                const auto& explainResult = future.GetValue();
                                UNIT_ASSERT(!explainResult.IsTransportError());
                                Cerr << explainResult.GetIssues().ToString() << Endl;
                                UNIT_ASSERT_EQUAL(explainResult.GetStatus(), EStatus::SUCCESS);
                                done = true;
                            };
                        const TString query = "UPSERT INTO `Root/TheTable` (Key, Value) VALUES (1, \"One\");";
                        session.ExplainDataQuery(query).Apply(std::move(sqlResultHandler)).Wait();
                    };

                const TString query = "CREATE TABLE `Root/TheTable` (Key Uint64, Value Utf8, PRIMARY KEY (Key));";
                session.ExecuteSchemeQuery(query).Apply(schemeQueryHandler).Wait();
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
        UNIT_ASSERT(done);
    }

    Y_UNIT_TEST(CreateAlterUpsertDrop) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        {

            auto asyncStatus = NYdb::NScheme::TSchemeClient(connection).MakeDirectory("/Root/TheDir");
            asyncStatus.Wait();
            UNIT_ASSERT_EQUAL(asyncStatus.GetValue().GetStatus(), EStatus::SUCCESS);
        }
        {
            auto asyncDescPath = NYdb::NScheme::TSchemeClient(connection).DescribePath("/Root/TheDir");
            asyncDescPath.Wait();
            auto entry = asyncDescPath.GetValue().GetEntry();
            UNIT_ASSERT_EQUAL(entry.Name, "TheDir");
            UNIT_ASSERT_EQUAL(entry.Type, ESchemeEntryType::Directory);
        }
        {
            auto asyncDescDir = NYdb::NScheme::TSchemeClient(connection).ListDirectory("/Root");
            asyncDescDir.Wait();
            const auto& val = asyncDescDir.GetValue();
            auto entry = val.GetEntry();
            UNIT_ASSERT_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_EQUAL(entry.Type, ESchemeEntryType::Directory);
            auto children = val.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
            for (const auto& child : children) {
                if (child.Name == ".sys" || child.Name == ".metadata") {
                    continue;
                }

                UNIT_ASSERT_EQUAL(child.Name, "TheDir");
                UNIT_ASSERT_EQUAL(child.Type, ESchemeEntryType::Directory);
            }
        }

        auto client = NYdb::NTable::TTableClient(connection);
        bool done = false;

        std::function<void(const TAsyncCreateSessionResult& future)> createSessionHandler =
            [client, &done] (const TAsyncCreateSessionResult& future) mutable {
                const auto& sessionValue = future.GetValue();
                UNIT_ASSERT(!sessionValue.IsTransportError());
                auto session = sessionValue.GetSession();

                auto createTableHandler =
                    [session, &done, client] (const NThreading::TFuture<TStatus>& future) mutable {
                        const auto& createTableResult = future.GetValue();
                        UNIT_ASSERT(!createTableResult.IsTransportError());
                        auto alterResultHandler =
                            [session, &done] (const NThreading::TFuture<TStatus>& future) mutable {
                                const auto& alterStatus = future.GetValue();
                                UNIT_ASSERT(!alterStatus.IsTransportError());
                                UNIT_ASSERT_EQUAL(alterStatus.GetStatus(), EStatus::SUCCESS);
                                auto upsertHandler =
                                    [session, &done] (const TAsyncDataQueryResult& future) mutable {
                                        const auto& sqlResultSets = future.GetValue();
                                        UNIT_ASSERT(!sqlResultSets.IsTransportError());
                                        UNIT_ASSERT_EQUAL(sqlResultSets.GetStatus(), EStatus::SUCCESS);
                                        auto describeHandler =
                                            [session, &done] (const TAsyncDescribeTableResult& future) mutable {
                                                const auto& value = future.GetValue();
                                                UNIT_ASSERT(!value.IsTransportError());
                                                UNIT_ASSERT_EQUAL(value.GetStatus(), EStatus::SUCCESS);
                                                auto desc = value.GetTableDescription();
                                                UNIT_ASSERT_EQUAL(desc.GetPrimaryKeyColumns().size(), 1);
                                                UNIT_ASSERT_EQUAL(desc.GetPrimaryKeyColumns()[0], "Key");
                                                auto columns = desc.GetColumns();
                                                UNIT_ASSERT_EQUAL(columns[0].Name, "Key");
                                                UNIT_ASSERT_EQUAL(columns[1].Name, "Value");
                                                UNIT_ASSERT_EQUAL(columns[2].Name, "NewColumn");
                                                TTypeParser column0(columns[0].Type);
                                                TTypeParser column1(columns[1].Type);
                                                TTypeParser column2(columns[2].Type);
                                                UNIT_ASSERT_EQUAL(column0.GetKind(), TTypeParser::ETypeKind::Optional);
                                                UNIT_ASSERT_EQUAL(column1.GetKind(), TTypeParser::ETypeKind::Optional);
                                                UNIT_ASSERT_EQUAL(column2.GetKind(), TTypeParser::ETypeKind::Optional);
                                                column0.OpenOptional();
                                                column1.OpenOptional();
                                                column2.OpenOptional();
                                                UNIT_ASSERT_EQUAL(column0.GetPrimitive(), EPrimitiveType::Int32);
                                                UNIT_ASSERT_EQUAL(column1.GetPrimitive(), EPrimitiveType::String);
                                                UNIT_ASSERT_EQUAL(column2.GetPrimitive(), EPrimitiveType::Utf8);
                                                UNIT_ASSERT_EQUAL( desc.GetOwner(), "root@builtin");
                                                auto dropHandler =
                                                    [&done] (const NThreading::TFuture<TStatus>& future) mutable {
                                                        const auto& dropStatus = future.GetValue();
                                                        UNIT_ASSERT(!dropStatus.IsTransportError());
                                                        UNIT_ASSERT_EQUAL(dropStatus.GetStatus(), EStatus::SUCCESS);
                                                        done = true;
                                                };
                                                session.DropTable("/Root/TheDir/FooTable")
                                                    .Apply(dropHandler).Wait();

                                            };
                                        session.DescribeTable("/Root/TheDir/FooTable")
                                            .Apply(describeHandler).Wait();
                                                                            };
                                const TString sql = "UPSERT INTO `Root/TheDir/FooTable` (Key, Value, NewColumn)"
                                   " VALUES (1, \"One\", \"йцукен\")";
                                session.ExecuteDataQuery(sql, TTxControl::
                                    BeginTx(TTxSettings::SerializableRW()).CommitTx()
                                ).Apply(upsertHandler).Wait();
                            };

                        {
                            auto type = TTypeBuilder()
                                    .BeginOptional()
                                        .Primitive(EPrimitiveType::Utf8)
                                    .EndOptional()
                                    .Build();

                            session.AlterTable("/Root/TheDir/FooTable", TAlterTableSettings()
                                .AppendAddColumns(TColumn{"NewColumn", type})).Apply(alterResultHandler).Wait();
                        }
                    };

                auto tableBuilder = client.GetTableBuilder();
                tableBuilder
                    .AddNullableColumn("Key", EPrimitiveType::Int32)
                    .AddNullableColumn("Value", EPrimitiveType::String);
                tableBuilder.SetPrimaryKeyColumn("Key");
                session.CreateTable("/Root/TheDir/FooTable", tableBuilder.Build()).Apply(createTableHandler).Wait();
            };

        client.CreateSession().Apply(createSessionHandler).Wait();
        UNIT_ASSERT(done);
    }

    Y_UNIT_TEST(InMemoryTables) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->GetAppData().FeatureFlags.SetEnablePublicApiKeepInMemory(true);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        auto client = NYdb::NTable::TTableClient(connection);
        auto createSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT(!createSessionResult.IsTransportError());
        auto session = createSessionResult.GetSession();

        auto createTableResult = session.CreateTable("/Root/Table", client.GetTableBuilder()
            .AddNullableColumn("Key", EPrimitiveType::Int32)
            .AddNullableColumn("Value", EPrimitiveType::String)
            .SetPrimaryKeyColumn("Key")
            // Note: only needed because this test doesn't initial table profiles
            .BeginStorageSettings()
                .SetTabletCommitLog0("ssd")
                .SetTabletCommitLog1("ssd")
            .EndStorageSettings()
            .BeginColumnFamily("default")
                .SetData("ssd")
                .SetKeepInMemory(true)
            .EndColumnFamily()
            .Build()).ExtractValueSync();
        UNIT_ASSERT_C(createTableResult.IsSuccess(), (NYdb::TStatus&)createTableResult);

        {
            auto describeTableResult = session.DescribeTable("/Root/Table").ExtractValueSync();
            UNIT_ASSERT_C(describeTableResult.IsSuccess(), (NYdb::TStatus&)describeTableResult);
            auto desc = describeTableResult.GetTableDescription();
            auto families = desc.GetColumnFamilies();
            UNIT_ASSERT_VALUES_EQUAL(families.size(), 1u);
            auto family = families.at(0);
            UNIT_ASSERT_VALUES_EQUAL(*family.GetKeepInMemory(), true);
        }

        {
            auto alterTableResult = session.AlterTable("/Root/Table", NYdb::NTable::TAlterTableSettings()
                .BeginAlterColumnFamily("default")
                    .SetKeepInMemory(false)
                .EndAlterColumnFamily()).ExtractValueSync();
            UNIT_ASSERT_C(alterTableResult.IsSuccess(), (NYdb::TStatus&)alterTableResult);
        }

        {
            auto describeTableResult = session.DescribeTable("/Root/Table").ExtractValueSync();
            UNIT_ASSERT_C(describeTableResult.IsSuccess(), (NYdb::TStatus&)describeTableResult);
            auto desc = describeTableResult.GetTableDescription();
            auto families = desc.GetColumnFamilies();
            UNIT_ASSERT_VALUES_EQUAL(families.size(), 1u);
            auto family = families.at(0);
            // Note: server cannot currently distinguish between implicitly
            // unset and explicitly disabled, so it returns the former.
            UNIT_ASSERT(!family.GetKeepInMemory());
        }

        {
            auto alterTableResult = session.AlterTable("/Root/Table", NYdb::NTable::TAlterTableSettings()
                .BeginAlterColumnFamily("default")
                    .SetKeepInMemory(true)
                .EndAlterColumnFamily()).ExtractValueSync();
            UNIT_ASSERT_C(alterTableResult.IsSuccess(), (NYdb::TStatus&)alterTableResult);
        }

        {
            auto describeTableResult = session.DescribeTable("/Root/Table").ExtractValueSync();
            UNIT_ASSERT_C(describeTableResult.IsSuccess(), (NYdb::TStatus&)describeTableResult);
            auto desc = describeTableResult.GetTableDescription();
            auto families = desc.GetColumnFamilies();
            UNIT_ASSERT_VALUES_EQUAL(families.size(), 1u);
            auto family = families.at(0);
            UNIT_ASSERT_VALUES_EQUAL(*family.GetKeepInMemory(), true);
        }
    }
}

} // namespace NKikimr

