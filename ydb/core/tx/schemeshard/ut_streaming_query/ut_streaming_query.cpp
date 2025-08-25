#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TStreamingQueryTest) {
    Y_UNIT_TEST(CreateStreamingQuery) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyStreamingQuery", false, NLs::PathExist);
    }

    void CompareProperties(const NKikimrSchemeOp::TStreamingQueryProperties& expected, const NKikimrSchemeOp::TStreamingQueryProperties& actual) {
        const auto& expectedProperties = expected.properties();
        const auto& actualProperties = actual.properties();
        UNIT_ASSERT_EQUAL(expectedProperties.size(), actualProperties.size());
        for (const auto& [expectedKey, expectedValue] : expectedProperties) {
            const auto it = actualProperties.find(expectedKey);
            UNIT_ASSERT(it != actualProperties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, expectedValue);
        }
    }

    Y_UNIT_TEST(CreateStreamingQueryWithProperties) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO Output SELECT * FROM Input"
                    }
                    Properties {
                        key: "run",
                        value: "true"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        NKikimrSchemeOp::TStreamingQueryProperties expectedProperties;
        {
            auto& properties = *expectedProperties.MutableProperties();
            properties.emplace("query_text", "INSERT INTO Output SELECT * FROM Input");
            properties.emplace("run", "true");
        }

        const auto describeResult =  DescribePath(runtime, "/MyRoot/MyStreamingQuery");
        TestDescribeResult(describeResult, {NLs::PathExist});
        UNIT_ASSERT(describeResult.GetPathDescription().HasStreamingQueryDescription());
        const auto& streamingQueryDescription = describeResult.GetPathDescription().GetStreamingQueryDescription();
        UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
        UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetVersion(), 1);
        CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
    }

    Y_UNIT_TEST(ParallelCreateStreamingQuery) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreateStreamingQuery(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "MyStreamingQuery1"
            )");
        AsyncCreateStreamingQuery(runtime, ++txId, "/MyRoot/DirA", R"(
                Name: "MyStreamingQuery2"
            )");
        TestModificationResult(runtime, txId - 2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId - 1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId - 1, txId - 2});

        TestDescribe(runtime, "/MyRoot/DirA/MyStreamingQuery1");
        TestDescribe(runtime, "/MyRoot/DirA/MyStreamingQuery2");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"), {NLs::PathVersionEqual(7)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/MyStreamingQuery1"), {NLs::PathVersionEqual(2)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/MyStreamingQuery2"), {NLs::PathVersionEqual(2)});
    }

    Y_UNIT_TEST(ParallelCreateSameStreamingQuery) {
        using ESts = NKikimrScheme::EStatus;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const TString streamingQueryConfig = R"(
                Name: "NilNoviSubLuna"
            )";

        AsyncCreateStreamingQuery(runtime, ++txId, "/MyRoot", streamingQueryConfig);
        AsyncCreateStreamingQuery(runtime, ++txId, "/MyRoot", streamingQueryConfig);
        AsyncCreateStreamingQuery(runtime, ++txId, "/MyRoot", streamingQueryConfig);

        const TVector<TExpectedResult> expectedResults = {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists};
        const ui64 sts[3] = {
            TestModificationResults(runtime, txId - 2, expectedResults),
            TestModificationResults(runtime, txId - 1, expectedResults),
            TestModificationResults(runtime, txId, expectedResults)
        };

        for (const auto st : sts) {
            if (st == ESts::StatusAlreadyExists) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"), {
                    NLs::Finished,
                    NLs::IsStreamingQuery
                });
            } else if (st == ESts::StatusMultipleModifications) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"), {
                    NLs::Finished,
                    NLs::IsStreamingQuery
                });
            }
        }

        env.TestWaitNotification(runtime, {txId - 2, txId - 1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"), {
            NLs::Finished,
            NLs::IsStreamingQuery,
            NLs::PathVersionEqual(2)
        });

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", streamingQueryConfig, {ESts::StatusAlreadyExists});
    }

    Y_UNIT_TEST(CreateStreamingQueryOrReplace) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO Output SELECT * FROM Input"
                    }
                    Properties {
                        key: "run",
                        value: "true"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        {
            NKikimrSchemeOp::TStreamingQueryProperties expectedProperties;
            {
                auto& properties = *expectedProperties.MutableProperties();
                properties.emplace("query_text", "INSERT INTO Output SELECT * FROM Input");
                properties.emplace("run", "true");
            }

            const auto describeResult =  DescribePath(runtime, "/MyRoot/MyStreamingQuery");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasStreamingQueryDescription());
            const auto& streamingQueryDescription = describeResult.GetPathDescription().GetStreamingQueryDescription();
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetVersion(), 1);
            CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
        }

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO OtherSink SELECT * FROM OtherSource"
                    }
                    Properties {
                        key: "resource_pool",
                        value: "my_pool"
                    }
                }
                ReplaceIfExists: true
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        {
            NKikimrSchemeOp::TStreamingQueryProperties expectedProperties;
            {
                auto& properties = *expectedProperties.MutableProperties();
                properties.emplace("query_text", "INSERT INTO OtherSink SELECT * FROM OtherSource");
                properties.emplace("resource_pool", "my_pool");
            }

            const auto describeResult =  DescribePath(runtime, "/MyRoot/MyStreamingQuery");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasStreamingQueryDescription());
            const auto& streamingQueryDescription = describeResult.GetPathDescription().GetStreamingQueryDescription();
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetVersion(), 2);
            CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
        }
    }

    Y_UNIT_TEST(ReadOnlyModeAndCreateStreamingQuery) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "SubDirA");
        AsyncCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
            )");

        // Set ReadOnly
        SetSchemeshardReadOnlyMode(runtime, true);
        const TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Verify that table creation successfully finished
        env.TestWaitNotification(runtime, txId);

        // Check that describe works
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SubDirA"), {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/MyStreamingQuery"), {
            NLs::Finished,
            NLs::IsStreamingQuery
        });

        // Check that new modifications fail
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB", {NKikimrScheme::StatusReadOnly});
        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery2"
            )", {NKikimrScheme::StatusReadOnly});

        // Disable ReadOnly
        SetSchemeshardReadOnlyMode(runtime, false);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Check that modifications now work again
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB");
    }

    Y_UNIT_TEST(CreateStreamingQueryFailAlreadyExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO Output SELECT * FROM Input"
                    }
                    Properties {
                        key: "run",
                        value: "true"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        NKikimrSchemeOp::TStreamingQueryProperties expectedProperties;
        {
            auto& properties = *expectedProperties.MutableProperties();
            properties.emplace("query_text", "INSERT INTO Output SELECT * FROM Input");
            properties.emplace("run", "true");
        }

        {
            const auto describeResult =  DescribePath(runtime, "/MyRoot/MyStreamingQuery");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasStreamingQueryDescription());
            const auto& streamingQueryDescription = describeResult.GetPathDescription().GetStreamingQueryDescription();
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetVersion(), 1);
            CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
        }

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO OtherSink SELECT * FROM OtherSource"
                    }
                    Properties {
                        key: "resource_pool",
                        value: "my_pool"
                    }
                }
            )", {NKikimrScheme::StatusAlreadyExists});

        env.TestWaitNotification(runtime, txId);

        {
            const auto describeResult =  DescribePath(runtime, "/MyRoot/MyStreamingQuery");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasStreamingQueryDescription());
            const auto& streamingQueryDescription = describeResult.GetPathDescription().GetStreamingQueryDescription();
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetVersion(), 1);
            CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
        }
    }

    Y_UNIT_TEST(CreateStreamingQueryOrReplaceFailNameConflict) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
                Name: "UniqueName"
                QueryText: "Some query"
            )", {NKikimrScheme::StatusAccepted}
        );

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/UniqueName", false, NLs::PathExist);

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "UniqueName"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO Output SELECT * FROM Input"
                    }
                    Properties {
                        key: "run",
                        value: "true"
                    }
                }
                ReplaceIfExists: true
            )", {{NKikimrScheme::StatusNameConflict, "error: unexpected path type"}});

        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateStreamingQuerySchemeErrors) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: ""
            )", {{NKikimrScheme::StatusSchemeError, "error: path part shouldn't be empty"}});
    }

    Y_UNIT_TEST(AlterStreamingQuery) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO Output SELECT * FROM Input"
                    }
                    Properties {
                        key: "run",
                        value: "true"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        {
            NKikimrSchemeOp::TStreamingQueryProperties expectedProperties;
            {
                auto& properties = *expectedProperties.MutableProperties();
                properties.emplace("query_text", "INSERT INTO Output SELECT * FROM Input");
                properties.emplace("run", "true");
            }

            const auto describeResult =  DescribePath(runtime, "/MyRoot/MyStreamingQuery");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasStreamingQueryDescription());
            const auto& streamingQueryDescription = describeResult.GetPathDescription().GetStreamingQueryDescription();
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetVersion(), 1);
            CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
        }

        TestAlterStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO OtherSink SELECT * FROM OtherSource"
                    }
                    Properties {
                        key: "resource_pool",
                        value: "my_pool"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        {
            NKikimrSchemeOp::TStreamingQueryProperties expectedProperties;
            {
                auto& properties = *expectedProperties.MutableProperties();
                properties.emplace("query_text", "INSERT INTO OtherSink SELECT * FROM OtherSource");
                properties.emplace("run", "true");
                properties.emplace("resource_pool", "my_pool");
            }

            const auto describeResult =  DescribePath(runtime, "/MyRoot/MyStreamingQuery");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasStreamingQueryDescription());
            const auto& streamingQueryDescription = describeResult.GetPathDescription().GetStreamingQueryDescription();
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetVersion(), 2);
            CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
        }
    }

    Y_UNIT_TEST(ParallelAlterStreamingQuery) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO Output SELECT * FROM Input"
                    }
                    Properties {
                        key: "run",
                        value: "true"
                    }
                }
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        constexpr ui32 TEST_RUNS = 30;
        TSet<ui64> txIds;
        for (ui32 i = 0; i < TEST_RUNS; ++i) {
            AsyncAlterStreamingQuery(runtime, ++txId, "/MyRoot",R"(
                    Name: "MyStreamingQuery"
                    Properties {
                        Properties {
                            key: "query_text",
                            value: "INSERT INTO OtherSink SELECT * FROM OtherSource"
                        }
                        Properties {
                            key: "resource_pool",
                            value: "my_pool"
                        }
                    }
                )");

            txIds.insert(txId);
        }

        ui32 acceptedCount = 0;
        for (const auto testTx : txIds) {
            const auto result = TestModificationResults(runtime, testTx, {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications});
            acceptedCount += result == NKikimrScheme::StatusAccepted;
        }
        UNIT_ASSERT_GE(acceptedCount, 1);

        env.TestWaitNotification(runtime, txIds);

        NKikimrSchemeOp::TStreamingQueryProperties expectedProperties;
        {
            auto& properties = *expectedProperties.MutableProperties();
            properties.emplace("query_text", "INSERT INTO OtherSink SELECT * FROM OtherSource");
            properties.emplace("run", "true");
            properties.emplace("resource_pool", "my_pool");
        }

        {
            const auto describeResult =  DescribePath(runtime, "/MyRoot/MyStreamingQuery");
            TestDescribeResult(describeResult, {NLs::PathExist});
            UNIT_ASSERT(describeResult.GetPathDescription().HasStreamingQueryDescription());
            const auto& streamingQueryDescription = describeResult.GetPathDescription().GetStreamingQueryDescription();
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
            UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetVersion(), acceptedCount + 1);
            CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
        }
    }

    Y_UNIT_TEST(AlterStreamingQueryFailOnNotExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestAlterStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO OtherSink SELECT * FROM OtherSource"
                    }
                }
            )", {{NKikimrScheme::StatusPathDoesNotExist, "error: path hasn't been resolved"}});

        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AlterStreamingQueryFailNameConflict) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateView(runtime, ++txId, "/MyRoot", R"(
                Name: "UniqueName"
                QueryText: "Some query"
            )", {NKikimrScheme::StatusAccepted}
        );

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/UniqueName", false, NLs::PathExist);

        TestAlterStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "UniqueName"
                Properties {
                    Properties {
                        key: "query_text",
                        value: "INSERT INTO OtherSink SELECT * FROM OtherSource"
                    }
                }
            )", {{NKikimrScheme::StatusNameConflict, "error: unexpected path type"}});

        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(DropStreamingQuery) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
                Name: "MyStreamingQuery"
            )", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyStreamingQuery", false, NLs::PathExist);

        TestDropStreamingQuery(runtime, ++txId, "/MyRoot", "MyStreamingQuery");
        env.TestWaitNotification(runtime, txId);

        TestLs(runtime, "/MyRoot/MyStreamingQuery", false, NLs::PathNotExist);
    }

    Y_UNIT_TEST(DropStreamingQueryTwice) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateStreamingQuery(runtime, ++txId, "/MyRoot", R"(
            Name: "MyStreamingQuery"
        )");
        env.TestWaitNotification(runtime, txId);

        AsyncDropStreamingQuery(runtime, ++txId, "/MyRoot", "MyStreamingQuery");
        AsyncDropStreamingQuery(runtime, ++txId, "/MyRoot", "MyStreamingQuery");
        TestModificationResult(runtime, txId - 1);

        const auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
        UNIT_ASSERT(ev);

        const auto& record = ev->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusMultipleModifications);
        UNIT_ASSERT_VALUES_EQUAL(record.GetPathDropTxId(), txId - 1);

        env.TestWaitNotification(runtime, txId - 1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/MyStreamingQuery"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(DropStreamingQueryFailOnNotExists) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestDropStreamingQuery(runtime, ++txId, "/MyRoot", "MyStreamingQuery", {{NKikimrScheme::StatusPathDoesNotExist, "error: path hasn't been resolved"}});
        env.TestWaitNotification(runtime, txId);
    }
}
