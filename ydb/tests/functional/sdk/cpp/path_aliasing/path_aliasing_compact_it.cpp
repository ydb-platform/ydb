#include <ydb/public/api/protos/draft/ydb_view.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_view.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/datetime/base.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <variant>

namespace NYdb::inline Dev::NPathAliasingTests {
    namespace {

        std::string RequiredEnv(const char* name) {
            const char* value = std::getenv(name);
            if (!value || !*value) {
                throw std::runtime_error(std::string("Path-aliasing recipe did not set ") + name);
            }
            return value;
        }

        template <class T>
        T Await(NThreading::TFuture<T> future) {
            if (!future.Wait(TDuration::Seconds(60))) {
                throw std::runtime_error("SDK request did not complete within 60 seconds");
            }
            return future.ExtractValueSync();
        }

        void Check(const TStatus& status) {
            if (!status.IsSuccess()) {
                throw std::runtime_error(status.GetIssues().ToString().c_str());
            }
        }

        NTable::TTableDescription TableDescription() {
            return NTable::TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();
        }

        TValue Row(uint64_t key, const std::string& value) {
            return TValueBuilder()
                .BeginList()
                .AddListItem()
                .BeginStruct()
                .AddMember("key")
                .Uint64(key)
                .AddMember("value")
                .Utf8(value)
                .EndStruct()
                .EndList()
                .Build();
        }

        TValue Keys(uint64_t key) {
            return TValueBuilder()
                .BeginList()
                .AddListItem()
                .BeginStruct()
                .AddMember("key")
                .Uint64(key)
                .EndStruct()
                .EndList()
                .Build();
        }

        void ExpectRow(const TResultSet& result, uint64_t key, const std::string& value) {
            TResultSetParser parser(result);
            ASSERT_EQ(result.RowsCount(), 1);
            ASSERT_TRUE(parser.TryNextRow());
            EXPECT_EQ(parser.ColumnParser("key").GetOptionalUint64(), key);
            EXPECT_EQ(parser.ColumnParser("value").GetOptionalUtf8(), value);
        }

        NTable::TSession GetSession(NTable::TTableClient& client) {
            auto result = Await(client.GetSession());
            Check(result);
            return result.GetSession();
        }

        class TPathAliasing: public ::testing::Test {
        protected:
            void SetUp() override {
                AliasDatabase = RequiredEnv("YDB_DATABASE");
                CanonicalDatabase = RequiredEnv("YDB_PATH_ALIAS_CANONICAL_DATABASE");
                Name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
                Alias = MakeDriver(AliasDatabase);
                Canonical = MakeDriver(CanonicalDatabase);
                NScheme::TSchemeClient scheme(*Canonical);
                Check(Await(scheme.MakeDirectory(P())));
            }

            std::unique_ptr<TDriver> MakeDriver(const std::string& database, const std::string& token = "root@builtin") {
                return std::make_unique<TDriver>(TDriverConfig()
                                                     .SetEndpoint(RequiredEnv("YDB_ENDPOINT"))
                                                     .SetDatabase(database)
                                                     .SetAuthToken(token));
            }

            std::string A(const std::string& leaf = {}) const {
                return AliasDatabase + "/" + Name + (leaf.empty() ? "" : "/" + leaf);
            }

            std::string P(const std::string& leaf = {}) const {
                return CanonicalDatabase + "/" + Name + (leaf.empty() ? "" : "/" + leaf);
            }

            std::string Select(const std::string& path) const {
                return "SELECT key, value FROM `" + path + "` ORDER BY key;";
            }

            template <class TOp>
            TOp WaitOperation(TOp operation) {
                NOperation::TOperationClient client(*Alias);
                const TInstant deadline = TInstant::Now() + TDuration::Seconds(120);
                while (!operation.Ready()) {
                    if (TInstant::Now() >= deadline) {
                        throw std::runtime_error("Import/export operation did not become ready");
                    }
                    operation = Await(client.Get<TOp>(operation.Id()));
                }
                Check(operation.Status());
                return operation;
            }

            std::string AliasDatabase;
            std::string CanonicalDatabase;
            std::string Name;
            std::unique_ptr<TDriver> Alias;
            std::unique_ptr<TDriver> Canonical;
        };

        TEST_F(TPathAliasing, DiscoveryAndSchemeResources) {
            NDiscovery::TDiscoveryClient discovery(*Alias);
            auto endpoints = Await(discovery.ListEndpoints());
            Check(endpoints);
            ASSERT_FALSE(endpoints.GetEndpointsInfo().empty());

            NScheme::TSchemeClient alias(*Alias);
            NScheme::TSchemeClient canonical(*Canonical);
            Check(Await(alias.MakeDirectory(A("directory") + "/")));
            Check(Await(canonical.DescribePath(P("directory"))));
            Check(Await(alias.DescribePath(A("directory") + "/")));

            auto trailingSlashDriver = MakeDriver(AliasDatabase + "/");
            NDiscovery::TDiscoveryClient trailingSlashDiscovery(*trailingSlashDriver);
            Check(Await(trailingSlashDiscovery.ListEndpoints()));

            const auto aliasedRelative = Await(alias.MakeDirectory(Name + "/relative"));
            const auto canonicalRelative = Await(canonical.MakeDirectory(Name + "/relative"));
            EXPECT_FALSE(aliasedRelative.IsSuccess());
            EXPECT_EQ(aliasedRelative.GetStatus(), canonicalRelative.GetStatus());

            EXPECT_FALSE(Await(alias.DescribePath("/kfrontend/" + Name)).IsSuccess());
            // A byte-prefix-only match would incorrectly describe the existing database.
            EXPECT_FALSE(Await(alias.DescribePath("/boundaryfront")).IsSuccess());
        }

        TEST_F(TPathAliasing, TableResourcesAndRepeatedSourceDestinationOperands) {
            NTable::TTableClient alias(*Alias);
            NTable::TTableClient canonical(*Canonical);
            // GetSession carries only the database and exercises routing without a resource path.
            auto session = GetSession(alias);
            auto physicalSession = GetSession(canonical);

            Check(Await(session.CreateTable("/short-table", TableDescription())));
            Check(Await(session.CreateTable(A("table_b"), TableDescription())));
            Check(Await(physicalSession.DescribeTable(P("table"))));
            Check(Await(alias.BulkUpsert(A("table"), Row(1, "/kfront/literal"))));
            Check(Await(alias.BulkUpsert(A("table_b"), Row(1, "/kfront/literal"))));

            Check(Await(session.CopyTables({{A("table"), A("copy_a")}, {A("table_b"), A("copy_b")}})));
            Check(Await(session.RenameTables({{A("copy_a"), A("renamed_a")}, {A("copy_b"), A("renamed_b")}})));
            for (const char* leaf : {"table", "renamed_a", "renamed_b"}) {
                auto result = Await(canonical.ReadRows(P(leaf), Keys(1)));
                Check(result);
                ExpectRow(result.GetResultSet(), 1, "/kfront/literal");
            }
        }

        TEST_F(TPathAliasing, NativeViewPathIsRewrittenButSqlTextIsNot) {
            NTable::TTableClient table(*Alias);
            auto session = GetSession(table);
            Check(Await(session.CreateTable(A("table"), TableDescription())));
            Check(Await(table.BulkUpsert(A("table"), Row(1, "/kfront/literal"))));

            NQuery::TQueryClient canonicalQuery(*Canonical);
            Check(Await(canonicalQuery.ExecuteQuery(
                "CREATE VIEW `" + P("view") + "` WITH (security_invoker = TRUE) AS " + Select(P("table")),
                NQuery::TTxControl::NoTx())));

            NView::TViewClient views(*Alias);
            Check(Await(views.DescribeView(A("view"))));
            NQuery::TQueryClient aliasQuery(*Alias);
            EXPECT_FALSE(Await(aliasQuery.ExecuteQuery(
                                   Select(A("table")), NQuery::TTxControl::BeginTx().CommitTx()))
                             .IsSuccess());
            auto result = Await(aliasQuery.ExecuteQuery(
                Select(P("view")), NQuery::TTxControl::BeginTx().CommitTx()));
            Check(result);
            ExpectRow(result.GetResultSet(0), 1, "/kfront/literal");
        }

        TEST_F(TPathAliasing, CoordinationAndRateLimiterOnlyRewriteNodePaths) {
            NCoordination::TClient coordination(*Alias);
            NCoordination::TClient canonicalCoordination(*Canonical);
            Check(Await(coordination.CreateNode(A("node"))));
            Check(Await(coordination.AlterNode(A("node"), NCoordination::TAlterNodeSettings()
                                                              .SelfCheckPeriod(TDuration::Seconds(1)))));
            Check(Await(canonicalCoordination.DescribeNode(P("node"))));

            auto sessionResult = Await(coordination.StartSession(A("node")));
            Check(sessionResult);
            auto coordinationSession = sessionResult.ExtractResult();
            Check(Await(coordinationSession.Ping()));

            NRateLimiter::TRateLimiterClient limiter(*Alias);
            NRateLimiter::TRateLimiterClient canonicalLimiter(*Canonical);
            const std::string resource = "resource";
            Check(Await(limiter.CreateResource(A("node"), resource,
                                               NRateLimiter::TCreateResourceSettings().MaxUnitsPerSecond(100))));
            auto described = Await(canonicalLimiter.DescribeResource(P("node"), resource));
            Check(described);
            EXPECT_EQ(described.GetResourcePath(), resource);
            Check(Await(limiter.DropResource(A("node"), resource)));
            Check(Await(coordinationSession.Close()));
            Check(Await(coordination.DropNode(A("node"))));
        }

        TEST_F(TPathAliasing, TopicResourcesAndStreamingSessions) {
            NTopic::TTopicClient alias(*Alias);
            NTopic::TTopicClient canonical(*Canonical);
            const std::string consumer = "reader";
            Check(Await(alias.CreateTopic(A("topic"), NTopic::TCreateTopicSettings()
                                                          .PartitioningSettings(1, 1)
                                                          .BeginAddConsumer(consumer)
                                                          .EndAddConsumer())));
            Check(Await(canonical.DescribeConsumer(P("topic"), consumer)));

            auto writer = alias.CreateSimpleBlockingWriteSession(NTopic::TWriteSessionSettings()
                                                                     .Path(A("topic"))
                                                                     .ProducerId("producer")
                                                                     .MessageGroupId("producer"));
            ASSERT_TRUE(writer->Write(NTopic::TWriteMessage("payload"), nullptr, TDuration::Seconds(30)));
            ASSERT_TRUE(writer->Close(TDuration::Seconds(30)));

            auto reader = alias.CreateReadSession(NTopic::TReadSessionSettings()
                                                      .ConsumerName(consumer)
                                                      .AppendTopics(A("topic")));
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
            bool received = false;
            while (!received) {
                const auto now = TInstant::Now();
                ASSERT_LT(now, deadline);
                ASSERT_TRUE(reader->WaitEvent().Wait(deadline - now));
                auto event = reader->GetEvent(false);
                if (!event) {
                    continue;
                }
                if (auto* start = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                    EXPECT_EQ(start->GetPartitionSession()->GetTopicPath(), Name + "/topic");
                    start->Confirm();
                } else if (auto* data = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                    ASSERT_EQ(data->GetMessages().size(), 1);
                    EXPECT_EQ(data->GetMessages()[0].GetData(), "payload");
                    received = true;
                } else if (std::holds_alternative<NTopic::TSessionClosedEvent>(*event)) {
                    FAIL() << "Topic stream closed before delivering data";
                }
            }
            ASSERT_TRUE(reader->Close(TDuration::Seconds(30)));

            Check(Await(alias.DropTopic(A("topic"))));
        }

        TEST_F(TPathAliasing, ExplicitImportAndExportPaths) {
            NTable::TTableClient table(*Alias);
            auto session = GetSession(table);
            Check(Await(session.CreateTable(A("table"), TableDescription())));

            NImport::TImportClient importer(*Alias);
            Check(Await(importer.ImportData(A("table"), std::string("1,\"imported\"\n"),
                                            NImport::TImportYdbDumpDataSettings()
                                                .AppendColumns("key")
                                                .AppendColumns("value"))));

            const std::string base = RequiredEnv("YDB_PATH_ALIAS_FS_DIR");
            NExport::TExportClient exporter(*Alias);
            WaitOperation(Await(exporter.ExportToFs(NExport::TExportToFsSettings()
                                                        .BasePath(base)
                                                        .AppendItem({A("table"), "archive"}))));
            EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(base) / "archive"));

            NScheme::TSchemeClient canonicalScheme(*Canonical);
            Check(Await(canonicalScheme.MakeDirectory(P("restored"))));
            WaitOperation(Await(importer.ImportFromFs(NImport::TImportFromFsSettings()
                                                          .BasePath(base)
                                                          .DestinationPath(A("restored"))
                                                          .AppendItem({"archive", A("restored/table")}))));
            NTable::TTableClient canonicalTable(*Canonical);
            auto restored = Await(canonicalTable.ReadRows(P("restored/table"), Keys(1)));
            Check(restored);
            ExpectRow(restored.GetResultSet(), 1, "imported");
        }

        TEST_F(TPathAliasing, AuthorizationUsesTheRewrittenTarget) {
            const std::string reader = "alias_reader@builtin";
            NScheme::TSchemeClient canonicalAdmin(*Canonical);
            NScheme::TSchemeClient aliasAdmin(*Alias);
            Check(Await(canonicalAdmin.ModifyPermissions(
                CanonicalDatabase,
                NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                    {reader, {"ydb.database.connect", "ydb.granular.describe_schema"}}))));
            Check(Await(canonicalAdmin.MakeDirectory(P("protected"))));
            Check(Await(aliasAdmin.ModifyPermissions(
                A("protected"),
                NScheme::TModifyPermissionsSettings()
                    .AddInterruptInheritance(true)
                    .AddClearAcl()
                    .AddGrantPermissions({"root@builtin", {"ydb.generic.full"}}))));

            auto aliasReader = MakeDriver(AliasDatabase, reader);
            auto canonicalReader = MakeDriver(CanonicalDatabase, reader);
            NScheme::TSchemeClient aliasScheme(*aliasReader);
            NScheme::TSchemeClient canonicalScheme(*canonicalReader);
            Check(Await(aliasScheme.DescribePath(A())));
            const auto aliased = Await(aliasScheme.MakeDirectory(A("protected/denied_alias")));
            const auto canonical = Await(canonicalScheme.MakeDirectory(P("protected/denied_canonical")));
            EXPECT_EQ(aliased.GetStatus(), EStatus::UNAUTHORIZED);
            EXPECT_EQ(aliased.GetStatus(), canonical.GetStatus());
            EXPECT_FALSE(Await(canonicalAdmin.DescribePath(P("protected/denied_alias"))).IsSuccess());
        }

    } // namespace
} // namespace NYdb::inline Dev::NPathAliasingTests
