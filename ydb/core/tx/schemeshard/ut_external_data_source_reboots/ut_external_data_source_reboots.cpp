#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TExternalDataSourceTestReboots) {
    Y_UNIT_TEST(CreateExternalDataSourceWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncMkDir(runtime, ++t.TxId, "/MyRoot", "DirExternalDataSource");

            AsyncCreateExternalDataSource(runtime, ++t.TxId, "/MyRoot/DirExternalDataSource", R"(
                    Name: "MyExternalDataSource"
                    SourceType: "ObjectStorage"
                    Location: "https://s3.cloud.net/my_bucket"
                    Auth {
                        None {
                        }
                    }
                )");

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            {
                TInactiveZone inactive(activeZone);
                auto describeResult =  DescribePath(runtime, "/MyRoot/DirExternalDataSource/MyExternalDataSource");
                TestDescribeResult(describeResult, {NLs::Finished});

                UNIT_ASSERT(describeResult.GetPathDescription().HasExternalDataSourceDescription());
                const auto& externalDataSourceDescription = describeResult.GetPathDescription().GetExternalDataSourceDescription();
                UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetName(), "MyExternalDataSource");
                UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetSourceType(), "ObjectStorage");
                UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetVersion(), 1);
                UNIT_ASSERT_VALUES_EQUAL(externalDataSourceDescription.GetLocation(), "https://s3.cloud.net/my_bucket");
                UNIT_ASSERT_EQUAL(externalDataSourceDescription.GetAuth().identity_case(), NKikimrSchemeOp::TAuth::kNone);
            }
        });
    }

    Y_UNIT_TEST(ParallelCreateDrop) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            AsyncCreateExternalDataSource(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "DropMe"
                    SourceType: "ObjectStorage"
                    Location: "https://s3.cloud.net/my_bucket"
                    Auth {
                        None {
                        }
                    }
                )");
            AsyncDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId-1);


            TestDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "DropMe");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DropMe"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropExternalDataSourceWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalDataSource(runtime, ++t.TxId, "/MyRoot",R"(
                        Name: "ExternalDataSource"
                        SourceType: "ObjectStorage"
                        Location: "https://s3.cloud.net/my_bucket"
                        Auth {
                            None {
                            }
                        }
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "ExternalDataSource");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalDataSource"),
                                   {NLs::PathNotExist});
            }
        });
    }

    Y_UNIT_TEST(SimpleDropExternalDataSourceWithReboots2) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalDataSource(runtime, ++t.TxId, "/MyRoot",R"(
                        Name: "ExternalDataSource"
                        SourceType: "ObjectStorage"
                        Location: "https://s3.cloud.net/my_bucket"
                        Auth {
                            None {
                            }
                        }
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "ExternalDataSource");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalDataSource"),
                                   {NLs::PathNotExist});
            }
        });
    }


    Y_UNIT_TEST(DropExternalDataSourceWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalDataSource(runtime, t.TxId, "/MyRoot",R"(
                        Name: "ExternalDataSource"
                        SourceType: "ObjectStorage"
                        Location: "https://s3.cloud.net/my_bucket"
                        Auth {
                            None {
                            }
                        }
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "ExternalDataSource");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalDataSource"),
                                   {NLs::PathNotExist});

                TestCreateExternalDataSource(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "ExternalDataSource"
                        SourceType: "ObjectStorage"
                        Location: "https://s3.cloud.net/my_bucket"
                        Auth {
                            None {
                            }
                        }
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "ExternalDataSource");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalDataSource"),
                                   {NLs::PathNotExist});
            }
        });
    }


    Y_UNIT_TEST(CreateDroppedExternalDataSourceWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalDataSource(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "ExternalDataSource"
                        SourceType: "ObjectStorage"
                        Location: "https://s3.cloud.net/my_bucket"
                        Auth {
                            None {
                            }
                        }
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "ExternalDataSource");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateExternalDataSource(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "ExternalDataSource"
                    SourceType: "ObjectStorage"
                    Location: "https://s3.cloud.net/my_bucket"
                    Auth {
                        None {
                        }
                    }
                )");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                TestDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "ExternalDataSource");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }
        });
    }

    Y_UNIT_TEST(CreateDroppedExternalDataSourceAndDropWithReboots) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestCreateExternalDataSource(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "ExternalDataSource"
                        SourceType: "ObjectStorage"
                        Location: "https://s3.cloud.net/my_bucket"
                        Auth {
                            None {
                            }
                        }
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "ExternalDataSource");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateExternalDataSource(runtime, ++t.TxId, "/MyRoot", R"(
                        Name: "ExternalDataSource"
                        SourceType: "ObjectStorage"
                        Location: "https://s3.cloud.net/my_bucket"
                        Auth {
                            None {
                            }
                        }
                    )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestDropExternalDataSource(runtime, ++t.TxId, "/MyRoot", "ExternalDataSource");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ExternalDataSource"),
                                   {NLs::PathNotExist});
            }
        });
    }
}
