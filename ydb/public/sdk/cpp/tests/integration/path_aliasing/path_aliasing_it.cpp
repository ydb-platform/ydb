#include <ydb/public/api/protos/draft/ydb_view.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
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

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <functional>
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
            return TValueBuilder().BeginList().AddListItem().BeginStruct().AddMember("key").Uint64(key).AddMember("value").Utf8(value).EndStruct().EndList().Build();
        }

        TValue Keys(uint64_t key) {
            return TValueBuilder().BeginList().AddListItem().BeginStruct().AddMember("key").Uint64(key).EndStruct().EndList().Build();
        }

        void ExpectRow(const TResultSet& result, uint64_t key, const std::string& value) {
            ASSERT_EQ(result.RowsCount(), 1);
            TResultSetParser parser(result);
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

            std::string R(const std::string& leaf) const {
                return Name + "/" + leaf;
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

        TEST_F(TPathAliasing, DiscoveryAndScheme) {
            NDiscovery::TDiscoveryClient discovery(*Alias);
            auto endpoints = Await(discovery.ListEndpoints());
            Check(endpoints);
            ASSERT_FALSE(endpoints.GetEndpointsInfo().empty());
            NDiscovery::TDiscoveryClient canonicalDiscovery(*Canonical);
            Check(Await(canonicalDiscovery.ListEndpoints()));

            NScheme::TSchemeClient alias(*Alias);
            NScheme::TSchemeClient canonical(*Canonical);
            Check(Await(alias.MakeDirectory(A("directory"))));
            Check(Await(canonical.DescribePath(P("directory"))));
            Check(Await(alias.DescribePath(A("directory"))));
            auto listing = Await(alias.ListDirectory(A()));
            Check(listing);
            ASSERT_EQ(listing.GetChildren().size(), 1);
            EXPECT_EQ(listing.GetChildren()[0].Name, "directory");
            auto relativeAlias = Await(alias.MakeDirectory(R("relative")));
            auto relativeCanonical = Await(canonical.MakeDirectory(R("relative")));
            EXPECT_FALSE(relativeCanonical.IsSuccess());
            EXPECT_EQ(relativeAlias.GetStatus(), relativeCanonical.GetStatus());
            EXPECT_FALSE(Await(canonical.DescribePath(P("relative"))).IsSuccess());
            Check(Await(alias.RemoveDirectory(A("directory"))));
            EXPECT_FALSE(Await(canonical.DescribePath(P("directory"))).IsSuccess());
            // A component boundary is significant: /kfrontend is not /kfront.
            EXPECT_FALSE(Await(alias.DescribePath("/kfrontend/" + Name)).IsSuccess());
        }

        TEST_F(TPathAliasing, TableResourceHandlesAndRepeatedSourceDestinationPaths) {
            NTable::TTableClient alias(*Alias);
            NTable::TTableClient canonical(*Canonical);
            auto session = GetSession(alias);
            auto physicalSession = GetSession(canonical);
            // A resource alias need not share the database alias prefix.
            Check(Await(session.CreateTable("/short-table", TableDescription())));
            Check(Await(physicalSession.DescribeTable(P("table"))));
            Check(Await(session.DescribeTable("/objects/" + R("table"))));
            Check(Await(session.AlterTable(A("table"), NTable::TAlterTableSettings()
                                                           .BeginAlterAttributes()
                                                           .Add("path-looking-value", "/kfront/literal")
                                                           .EndAlterAttributes())));
            auto description = Await(physicalSession.DescribeTable(P("table")));
            Check(description);
            EXPECT_EQ(description.GetTableDescription().GetAttributes().at("path-looking-value"), "/kfront/literal");
            Check(Await(alias.BulkUpsert(A("table"), Row(1, "/kfront/literal"))));
            auto rows = Await(alias.ReadRows(A("table"), Keys(1)));
            Check(rows);
            ExpectRow(rows.GetResultSet(), 1, "/kfront/literal");

            // Enabling aliases must not give native handles new relative-path syntax.
            auto relativeRows = Await(alias.ReadRows(R("table"), Keys(1)));
            auto canonicalRelativeRows = Await(canonical.ReadRows(R("table"), Keys(1)));
            EXPECT_FALSE(canonicalRelativeRows.IsSuccess());
            EXPECT_EQ(relativeRows.GetStatus(), canonicalRelativeRows.GetStatus());
            auto relativeDrop = Await(session.DropTable("table"));
            auto canonicalRelativeDrop = Await(physicalSession.DropTable("table"));
            EXPECT_EQ(relativeDrop.GetStatus(), EStatus::BAD_REQUEST);
            EXPECT_EQ(relativeDrop.GetStatus(), canonicalRelativeDrop.GetStatus());
            Check(Await(physicalSession.DescribeTable(P("table"))));

            auto stream = Await(session.ReadTable(A("table")));
            Check(stream);
            uint64_t rowCount = 0;
            for (;;) {
                auto part = Await(stream.ReadNext());
                if (part.EOS()) {
                    break;
                }
                Check(part);
                rowCount += part.GetPart().RowsCount();
            }
            EXPECT_EQ(rowCount, 1);
            Check(Await(session.CopyTable(A("table"), A("single_copy"))));
            Check(Await(session.CopyTables({{A("table"), A("copy_a")}, {A("single_copy"), A("copy_b")}})));
            Check(Await(session.RenameTables({{A("copy_a"), A("renamed_a")}, {A("copy_b"), A("renamed_b")}})));
            for (const char* leaf : {"table", "single_copy", "renamed_a", "renamed_b"}) {
                auto result = Await(canonical.ReadRows(P(leaf), Keys(1)));
                Check(result);
                ExpectRow(result.GetResultSet(), 1, "/kfront/literal");
                Check(Await(session.DropTable(A(leaf))));
                EXPECT_FALSE(Await(physicalSession.DescribeTable(P(leaf))).IsSuccess());
            }
        }

        TEST_F(TPathAliasing, NativeViewHandleLeavesSqlOperandsUnchanged) {
            NTable::TTableClient table(*Alias);
            auto session = GetSession(table);
            Check(Await(session.CreateTable(A("table"), TableDescription())));
            Check(Await(table.BulkUpsert(A("table"), Row(1, "/kfront/literal"))));
            // SQL identifiers are not alias inputs. Use canonical SQL only to prepare
            // the object whose native DescribeView handle is exercised below.
            NQuery::TQueryClient physicalQuery(*Canonical);
            Check(Await(physicalQuery.ExecuteQuery("CREATE VIEW `" + P("view") + "` WITH (security_invoker = TRUE) AS " + Select(P("table")), NQuery::TTxControl::NoTx())));
            NView::TViewClient views(*Alias);
            Check(Await(views.DescribeView(A("view"))));
            NQuery::TQueryClient query(*Alias);
            auto fromView = Await(query.ExecuteQuery(Select(P("view")), NQuery::TTxControl::BeginTx().CommitTx()));
            Check(fromView);
            ExpectRow(fromView.GetResultSet(0), 1, "/kfront/literal");
            EXPECT_FALSE(Await(query.ExecuteQuery(Select(A("table")), NQuery::TTxControl::BeginTx().CommitTx())).IsSuccess());
            Check(Await(physicalQuery.ExecuteQuery("DROP VIEW `" + P("view") + "`;", NQuery::TTxControl::NoTx())));
        }

        TEST_F(TPathAliasing, CoordinationAndRateLimiterKeepServiceLocalNames) {
            NCoordination::TClient coordination(*Alias);
            NCoordination::TClient physicalCoordination(*Canonical);
            Check(Await(coordination.CreateNode(A("node"))));
            Check(Await(coordination.AlterNode(A("node"), NCoordination::TAlterNodeSettings()
                                                              .SelfCheckPeriod(TDuration::Seconds(1)))));
            Check(Await(physicalCoordination.DescribeNode(P("node"))));
            auto started = Await(coordination.StartSession(A("node")));
            Check(started);
            auto session = started.ExtractResult();
            const std::string semaphore = "kfront/semaphore";
            Check(Await(session.CreateSemaphore(semaphore, 1, "/kfront/literal")));
            auto acquired = Await(session.AcquireSemaphore(semaphore, NCoordination::TAcquireSemaphoreSettings()
                                                                          .Count(1)
                                                                          .Timeout(TDuration::Seconds(10))));
            Check(acquired);
            ASSERT_TRUE(acquired.GetResult());
            auto described = Await(session.DescribeSemaphore(semaphore));
            Check(described);
            EXPECT_EQ(described.GetResult().GetName(), semaphore);
            EXPECT_EQ(described.GetResult().GetData(), "/kfront/literal");
            Check(Await(session.UpdateSemaphore(semaphore, "/kfront/updated")));
            Check(Await(session.ReleaseSemaphore(semaphore)));
            Check(Await(session.Ping()));
            Check(Await(session.Reconnect()));
            Check(Await(session.DeleteSemaphore(semaphore)));
            Check(Await(session.Close()));

            NRateLimiter::TRateLimiterClient limiter(*Alias);
            NRateLimiter::TRateLimiterClient physicalLimiter(*Canonical);
            Check(Await(limiter.CreateResource(A("node"), "kfront", NRateLimiter::TCreateResourceSettings().MaxUnitsPerSecond(100))));
            Check(Await(limiter.CreateResource(A("node"), "kfront/resource")));
            Check(Await(limiter.AlterResource(A("node"), "kfront/resource", NRateLimiter::TAlterResourceSettings().MaxUnitsPerSecond(50))));
            auto resource = Await(physicalLimiter.DescribeResource(P("node"), "kfront/resource"));
            Check(resource);
            EXPECT_EQ(resource.GetResourcePath(), "kfront/resource");
            auto listed = Await(limiter.ListResources(A("node"), "kfront", NRateLimiter::TListResourcesSettings().Recursive(true)));
            Check(listed);
            EXPECT_NE(std::find(listed.GetResourcePaths().begin(), listed.GetResourcePaths().end(), "kfront/resource"),
                      listed.GetResourcePaths().end());
            Check(Await(limiter.AcquireResource(A("node"), "kfront/resource", NRateLimiter::TAcquireResourceSettings().Amount(1).OperationTimeout(TDuration::Seconds(10)).CancelAfter(TDuration::Seconds(5)))));
            Check(Await(limiter.DropResource(A("node"), "kfront/resource")));
            Check(Await(limiter.DropResource(A("node"), "kfront")));
            Check(Await(coordination.DropNode(A("node"))));
            EXPECT_FALSE(Await(physicalCoordination.DescribeNode(P("node"))).IsSuccess());
        }

        TEST_F(TPathAliasing, TopicSchemaStreamingAndTransactionalOffsets) {
            NTopic::TTopicClient topic(*Alias);
            NTopic::TTopicClient physicalTopic(*Canonical);
            const std::string consumer = "kfront";
            const std::string txConsumer = "transactional";
            // The physical topic name deliberately matches a different alias rule.
            // An independent logical name allows observing its actual target once.
            const std::string inspectionPath = "/objects/" + R("topic");
            Check(Await(physicalTopic.CreateTopic("/objects/" + R("decoy"), NTopic::TCreateTopicSettings()
                                                                                .PartitioningSettings(1, 1)
                                                                                .BeginAddConsumer(txConsumer)
                                                                                .EndAddConsumer())));
            Check(Await(topic.CreateTopic(A("topic"), NTopic::TCreateTopicSettings()
                                                          .PartitioningSettings(1, 1)
                                                          .BeginAddConsumer(consumer)
                                                          .EndAddConsumer())));
            Check(Await(topic.AlterTopic(A("topic"), NTopic::TAlterTopicSettings()
                                                         .BeginAddConsumer(txConsumer)
                                                         .EndAddConsumer())));
            auto description = Await(physicalTopic.DescribeTopic(inspectionPath));
            Check(description);
            const auto& consumers = description.GetTopicDescription().GetConsumers();
            ASSERT_EQ(consumers.size(), 2);
            EXPECT_TRUE(std::any_of(consumers.begin(), consumers.end(), [&](const auto& item) {
                return item.GetConsumerName() == consumer;
            }));
            Check(Await(topic.DescribeConsumer(A("topic"), consumer)));
            Check(Await(topic.DescribePartition(A("topic"), 0)));
            auto writer = topic.CreateSimpleBlockingWriteSession(NTopic::TWriteSessionSettings()
                                                                     .Path(A("topic"))
                                                                     .ProducerId("kfront/producer")
                                                                     .MessageGroupId("kfront/producer"));
            ASSERT_TRUE(writer->Write(NTopic::TWriteMessage("/kfront/literal"), nullptr, TDuration::Seconds(30)));
            ASSERT_TRUE(writer->Close(TDuration::Seconds(30)));

            auto readOne = [&](const std::string& consumerName, NQuery::TTransaction* transaction) {
                auto reader = topic.CreateReadSession(NTopic::TReadSessionSettings()
                                                          .ConsumerName(consumerName)
                                                          .AppendTopics(A("topic")));
                NTopic::TReadSessionGetEventSettings settings;
                if (transaction) {
                    settings.Tx(std::ref(*transaction));
                }
                const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
                for (;;) {
                    const auto now = TInstant::Now();
                    if (now >= deadline || !reader->WaitEvent().Wait(deadline - now)) {
                        throw std::runtime_error("Topic stream did not deliver its message");
                    }
                    auto event = reader->GetEvent(settings);
                    if (!event) {
                        continue;
                    }
                    if (auto* start = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                        // This path is a protocol correlation name reused by the SDK,
                        // not a canonical metadata path to rewrite on the next RPC.
                        EXPECT_EQ(start->GetPartitionSession()->GetTopicPath(), A("topic"));
                        start->Confirm();
                    } else if (auto* stop = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                        stop->Confirm();
                    } else if (auto* data = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                        EXPECT_EQ(data->GetPartitionSession()->GetTopicPath(), A("topic"));
                        if (data->GetMessages().size() != 1) {
                            throw std::runtime_error("Expected exactly one topic message");
                        }
                        const auto& message = data->GetMessages()[0];
                        EXPECT_EQ(message.GetData(), "/kfront/literal");
                        const auto offset = message.GetOffset() + 1;
                        if (transaction) {
                            // GetEvent(tx) registers offsets in the transaction. Do not
                            // also Commit() the event: that would hide a broken tx path.
                            Check(Await(transaction->Commit()));
                        } else {
                            data->Commit();
                            Check(Await(topic.CommitOffset(A("topic"), 0, consumerName, offset)));
                        }
                        if (!reader->Close(TDuration::Seconds(30))) {
                            throw std::runtime_error("Topic read session did not close");
                        }
                        return offset;
                    } else if (std::holds_alternative<NTopic::TSessionClosedEvent>(*event)) {
                        throw std::runtime_error("Topic stream closed before delivering data");
                    }
                }
            };

            const auto offset = readOne(consumer, nullptr);
            NQuery::TQueryClient query(*Alias);
            auto querySessionResult = Await(query.GetSession());
            Check(querySessionResult);
            auto querySession = querySessionResult.GetSession();
            auto begun = Await(querySession.BeginTransaction(NQuery::TTxSettings::SerializableRW()));
            Check(begun);
            auto transaction = begun.GetTransaction();
            EXPECT_EQ(readOne(txConsumer, &transaction), offset);
            for (const auto& name : {consumer, txConsumer}) {
                auto described = Await(physicalTopic.DescribeConsumer(inspectionPath, name,
                                                                      NTopic::TDescribeConsumerSettings().IncludeStats(true)));
                Check(described);
                const auto& partitions = described.GetConsumerDescription().GetPartitions();
                ASSERT_EQ(partitions.size(), 1);
                ASSERT_TRUE(partitions[0].GetPartitionConsumerStats());
                EXPECT_EQ(partitions[0].GetPartitionConsumerStats()->GetCommittedOffset(), offset);
            }
            Check(Await(topic.DropTopic(A("topic"))));
            EXPECT_FALSE(Await(physicalTopic.DescribeTopic(inspectionPath)).IsSuccess());
            Check(Await(physicalTopic.DescribeTopic("/objects/" + R("decoy"))));
            Check(Await(physicalTopic.DropTopic("/objects/" + R("decoy"))));
        }

        TEST_F(TPathAliasing, ImportDataAndFilesystemRoundTrip) {
            NTable::TTableClient table(*Alias);
            auto session = GetSession(table);
            Check(Await(session.CreateTable(A("table"), TableDescription())));
            NImport::TImportClient importer(*Alias);
            Check(Await(importer.ImportData(A("table"), std::string("1,\"/kfront/imported\"\n"),
                                            NImport::TImportYdbDumpDataSettings().AppendColumns("key").AppendColumns("value"))));
            auto rows = Await(table.ReadRows(A("table"), Keys(1)));
            Check(rows);
            ExpectRow(rows.GetResultSet(), 1, "/kfront/imported");

            const std::string base = RequiredEnv("YDB_PATH_ALIAS_FS_DIR");
            NExport::TExportClient exporter(*Alias);
            WaitOperation(Await(exporter.ExportToFs(NExport::TExportToFsSettings()
                                                        .BasePath(base)
                                                        .AppendItem({A("table"), "kfront/archive"}))));
            EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(base) / "kfront/archive"));
            EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(base) / "must-not-rewrite/archive"));
            NScheme::TSchemeClient scheme(*Canonical);
            Check(Await(scheme.MakeDirectory(P("restored"))));
            // DestinationPath and item.Dst compose one logical path before rewriting.
            WaitOperation(Await(importer.ImportFromFs(NImport::TImportFromFsSettings()
                                                          .BasePath(base)
                                                          .DestinationPath(A("restored"))
                                                          .AppendItem({"kfront/archive", "table"}))));
            NTable::TTableClient physical(*Canonical);
            auto restored = Await(physical.ReadRows(P("restored/table"), Keys(1)));
            Check(restored);
            ExpectRow(restored.GetResultSet(), 1, "/kfront/imported");
        }

        TEST_F(TPathAliasing, AuthorizationUsesTargetAndDeniedWritesDoNotMutate) {
            const std::string reader = "alias_reader@builtin";
            NScheme::TSchemeClient admin(*Canonical);
            NScheme::TSchemeClient aliasAdmin(*Alias);
            Check(Await(admin.ModifyPermissions(CanonicalDatabase, NScheme::TModifyPermissionsSettings()
                                                                       .AddGrantPermissions({reader, {"ydb.database.connect", "ydb.granular.describe_schema"}}))));
            Check(Await(admin.MakeDirectory(P("protected"))));
            Check(Await(aliasAdmin.ModifyPermissions(A("protected"), NScheme::TModifyPermissionsSettings()
                                                                         .AddInterruptInheritance(true)
                                                                         .AddClearAcl()
                                                                         .AddGrantPermissions({"root@builtin", {"ydb.generic.full"}}))));
            auto aliasReader = MakeDriver(AliasDatabase, reader);
            auto canonicalReader = MakeDriver(CanonicalDatabase, reader);
            NScheme::TSchemeClient aliasScheme(*aliasReader);
            NScheme::TSchemeClient canonicalScheme(*canonicalReader);
            // A positive control distinguishes target ACL checks from broken routing/auth.
            Check(Await(aliasScheme.DescribePath(A())));
            Check(Await(canonicalScheme.DescribePath(P())));
            auto aliasDenied = Await(aliasScheme.MakeDirectory(A("protected/denied_alias")));
            auto canonicalDenied = Await(canonicalScheme.MakeDirectory(P("protected/denied_canonical")));
            EXPECT_EQ(aliasDenied.GetStatus(), EStatus::UNAUTHORIZED);
            EXPECT_EQ(aliasDenied.GetStatus(), canonicalDenied.GetStatus());
            EXPECT_FALSE(Await(admin.DescribePath(P("protected/denied_alias"))).IsSuccess());
            EXPECT_FALSE(Await(admin.DescribePath(P("protected/denied_canonical"))).IsSuccess());
        }

        TEST_F(TPathAliasing, CrossDatabaseChecksAndMissingPathStatusAreUnchanged) {
            NTable::TTableClient alias(*Alias);
            NTable::TTableClient canonical(*Canonical);
            auto aliasSession = GetSession(alias);
            auto canonicalSession = GetSession(canonical);
            Check(Await(canonicalSession.CreateTable(P("table"), TableDescription())));
            const std::string isolation = RequiredEnv("YDB_PATH_ALIAS_ISOLATION_DATABASE");
            auto isolatedDriver = MakeDriver(isolation);
            NScheme::TSchemeClient isolated(*isolatedDriver);
            const std::string aliasTarget = isolation + "/denied_alias";
            const std::string canonicalTarget = isolation + "/denied_canonical";
            auto aliasCopy = Await(aliasSession.CopyTable(A("table"), aliasTarget));
            auto canonicalCopy = Await(canonicalSession.CopyTable(P("table"), canonicalTarget));
            EXPECT_FALSE(canonicalCopy.IsSuccess());
            EXPECT_EQ(aliasCopy.GetStatus(), canonicalCopy.GetStatus());
            EXPECT_FALSE(Await(isolated.DescribePath(aliasTarget)).IsSuccess());
            EXPECT_FALSE(Await(isolated.DescribePath(canonicalTarget)).IsSuccess());
            EXPECT_EQ(Await(aliasSession.DescribeTable(A("missing"))).GetStatus(),
                      Await(canonicalSession.DescribeTable(P("missing"))).GetStatus());
        }

        TEST_F(TPathAliasing, DataStreamsContinuationsKeepResolvedIdentity) {
            namespace NStreams = NDataStreams::V1;
            NStreams::TDataStreamsClient streams(*Alias);
            const std::string logical = A("stream");
            const std::string actual = "/objects/" + R("stream");
            const std::string decoy = "/objects/" + R("decoy");
            Check(Await(streams.CreateStream(logical, NStreams::TCreateStreamSettings().ShardCount(2))));
            Check(Await(streams.CreateStream(decoy, NStreams::TCreateStreamSettings().ShardCount(1))));
            Check(Await(streams.DescribeStream(logical)));
            Check(Await(streams.DescribeStream(actual)));
            const std::string firstData = "/kfront/data-not-a-path-1";
            const std::string secondData = "/kfront/data-not-a-path-2";
            auto firstWrite = Await(streams.PutRecord(logical, NStreams::TDataRecord{firstData, "kfront-partition", "0"}));
            Check(firstWrite);
            Check(Await(streams.PutRecord(logical, NStreams::TDataRecord{secondData, "kfront-partition", "0"})));
            Check(Await(streams.PutRecord(decoy, NStreams::TDataRecord{"decoy-data", "kfront-partition", "0"})));
            auto iterator = Await(streams.GetShardIterator(logical, firstWrite.GetResult().shard_id(),
                                                           Ydb::DataStreams::V1::ShardIteratorType::TRIM_HORIZON));
            Check(iterator);
            auto records = Await(streams.GetRecords(iterator.GetResult().shard_iterator(), NStreams::TGetRecordsSettings().Limit(1)));
            Check(records);
            ASSERT_EQ(records.GetResult().records_size(), 1);
            EXPECT_EQ(records.GetResult().records(0).data(), firstData);
            EXPECT_EQ(records.GetResult().records(0).partition_key(), "kfront-partition");
            ASSERT_FALSE(records.GetResult().next_shard_iterator().empty());
            auto nextRecords = Await(streams.GetRecords(records.GetResult().next_shard_iterator(), NStreams::TGetRecordsSettings().Limit(1)));
            Check(nextRecords);
            ASSERT_EQ(nextRecords.GetResult().records_size(), 1);
            EXPECT_EQ(nextRecords.GetResult().records(0).data(), secondData);

            auto shards = Await(streams.ListShards(logical, {}, NStreams::TListShardsSettings().MaxResults(1)));
            Check(shards);
            ASSERT_EQ(shards.GetResult().shards_size(), 1);
            ASSERT_FALSE(shards.GetResult().next_token().empty());
            auto nextShards = Await(streams.ListShards("", {}, NStreams::TListShardsSettings().NextToken(shards.GetResult().next_token())));
            Check(nextShards);
            ASSERT_EQ(nextShards.GetResult().shards_size(), 1);
            EXPECT_NE(nextShards.GetResult().shards(0).shard_id(), shards.GetResult().shards(0).shard_id());
            // Existing ListShards pagination may leave a trailing token. The
            // distinct second shard proves this continuation did not hit the decoy.

            Check(Await(streams.RegisterStreamConsumer(logical, "original-one")));
            Check(Await(streams.RegisterStreamConsumer(logical, "original-two")));
            Check(Await(streams.RegisterStreamConsumer(decoy, "decoy-only")));
            auto consumers = Await(streams.ListStreamConsumers(logical, NStreams::TListStreamConsumersSettings().MaxResults(1)));
            Check(consumers);
            ASSERT_EQ(consumers.GetResult().consumers_size(), 1);
            ASSERT_FALSE(consumers.GetResult().next_token().empty());
            auto nextConsumers = Await(streams.ListStreamConsumers("",
                                                                   NStreams::TListStreamConsumersSettings().NextToken(consumers.GetResult().next_token())));
            Check(nextConsumers);
            ASSERT_EQ(nextConsumers.GetResult().consumers_size(), 1);
            EXPECT_NE(consumers.GetResult().consumers(0).consumer_name(), nextConsumers.GetResult().consumers(0).consumer_name());
            EXPECT_NE(consumers.GetResult().consumers(0).consumer_name(), "decoy-only");
            EXPECT_NE(nextConsumers.GetResult().consumers(0).consumer_name(), "decoy-only");
            EXPECT_TRUE(nextConsumers.GetResult().next_token().empty());
            EXPECT_EQ(Await(streams.ListStreamConsumers("", NStreams::TListStreamConsumersSettings()
                                                                .NextToken("malformed-token")))
                          .GetStatus(), EStatus::BAD_REQUEST);
            Check(Await(streams.DeleteStream(logical, NStreams::TDeleteStreamSettings().EnforceConsumerDeletion(true))));
            EXPECT_FALSE(Await(streams.DescribeStream(actual)).IsSuccess());
            Check(Await(streams.DescribeStream(decoy)));
            Check(Await(streams.DeleteStream(decoy, NStreams::TDeleteStreamSettings().EnforceConsumerDeletion(true))));
        }

    } // namespace
} // namespace NYdb::inline Dev::NPathAliasingTests
