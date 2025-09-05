#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TStreamingQueryTestReboots) {
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

    Y_UNIT_TEST(CreateStreamingQuery) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncMkDir(runtime, ++t.TxId, "/MyRoot", "DirStreamingQuery");

            AsyncCreateStreamingQuery(runtime, ++t.TxId, "/MyRoot/DirStreamingQuery", R"(
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
                )");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            NKikimrSchemeOp::TStreamingQueryProperties expectedProperties;
            {
                auto& properties = *expectedProperties.MutableProperties();
                properties.emplace("query_text", "INSERT INTO Output SELECT * FROM Input");
                properties.emplace("run", "true");
            }

            {
                TInactiveZone inactive(activeZone);
                auto describeResult =  DescribePath(runtime, "/MyRoot/DirStreamingQuery/MyStreamingQuery");
                TestDescribeResult(describeResult, {NLs::Finished});
                const auto& pathDescription = describeResult.GetPathDescription();
                UNIT_ASSERT(pathDescription.HasStreamingQueryDescription());
                const auto& streamingQueryDescription = pathDescription.GetStreamingQueryDescription();
                UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
                UNIT_ASSERT_VALUES_EQUAL(pathDescription.GetSelf().GetVersion().GetStreamingQueryVersion(), 1);
                CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
            }
        });
    }

    Y_UNIT_TEST(ParallelCreateDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncCreateStreamingQuery(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "DropMe"
                )");

            AsyncDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);

            TestDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DropMe"), {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropStreamingQuery) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateStreamingQuery(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "StreamingQuery"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "StreamingQuery");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/StreamingQuery"), {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropStreamingQuery2) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateStreamingQuery(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "StreamingQuery"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "StreamingQuery");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/StreamingQuery"), {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(DropStreamingQuery) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateStreamingQuery(runtime, t.TxId, "/MyRoot", R"(
                        Name: "StreamingQuery"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "StreamingQuery");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/StreamingQuery"), {NLs::PathNotExist});

                TestCreateStreamingQuery(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "StreamingQuery"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "StreamingQuery");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/StreamingQuery"), {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedStreamingQuery) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateStreamingQuery(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "StreamingQuery"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "StreamingQuery");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateStreamingQuery(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "StreamingQuery"
                )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "StreamingQuery");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedStreamingQueryAndDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateStreamingQuery(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "StreamingQuery"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "StreamingQuery");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateStreamingQuery(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "StreamingQuery"
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropStreamingQuery(runtime, ++t.TxId, "/MyRoot", "StreamingQuery");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/StreamingQuery"), {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(AlterStreamingQuery) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                TestCreateStreamingQuery(runtime, t.TxId, "/MyRoot", R"(
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
                    )");

                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterStreamingQuery(runtime, t.TxId, "/MyRoot", R"(
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

            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            NKikimrSchemeOp::TStreamingQueryProperties expectedProperties;
            {
                auto& properties = *expectedProperties.MutableProperties();
                properties.emplace("query_text", "INSERT INTO OtherSink SELECT * FROM OtherSource");
                properties.emplace("run", "true");
                properties.emplace("resource_pool", "my_pool");
            }

            {
                TInactiveZone inactive(activeZone);
                auto describeResult =  DescribePath(runtime, "/MyRoot/MyStreamingQuery");
                TestDescribeResult(describeResult, {NLs::Finished});
                const auto& pathDescription = describeResult.GetPathDescription();
                UNIT_ASSERT(pathDescription.HasStreamingQueryDescription());
                const auto& streamingQueryDescription = pathDescription.GetStreamingQueryDescription();
                UNIT_ASSERT_VALUES_EQUAL(streamingQueryDescription.GetName(), "MyStreamingQuery");
                UNIT_ASSERT_VALUES_EQUAL(pathDescription.GetSelf().GetVersion().GetStreamingQueryVersion(), 2);
                CompareProperties(expectedProperties, streamingQueryDescription.GetProperties());
            }
        });
    }
}
