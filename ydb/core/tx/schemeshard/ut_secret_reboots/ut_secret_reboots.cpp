#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

namespace {
    using namespace NSchemeShardUT_Private;

    void ExpectEqualSecretDescription(
        const NKikimrScheme::TEvDescribeSchemeResult& describeResult,
        const TString& name,
        const TString& value,
        const ui64 version
    ) {
        UNIT_ASSERT(describeResult.HasPathDescription());
        UNIT_ASSERT(describeResult.GetPathDescription().HasSecretDescription());
        const auto& secretDescription = describeResult.GetPathDescription().GetSecretDescription();
        UNIT_ASSERT_VALUES_EQUAL(secretDescription.GetName(), name);
        UNIT_ASSERT_VALUES_EQUAL(secretDescription.GetValue(), value);
        UNIT_ASSERT_VALUES_EQUAL(secretDescription.GetVersion(), version);
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribePathWithSecretValue(
        TTestActorRuntime& runtime,
        const TString& path
    ) {
        NKikimrSchemeOp::TDescribeOptions opts;
        opts.SetReturnSecretValue(true);
        return DescribePath(runtime, path, opts);
    }
}

Y_UNIT_TEST_SUITE(TSchemeShardSecretTestReboots) {
    Y_UNIT_TEST(CreateSecret) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "dir");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestCreateSecret(runtime, ++t.TxId, "/MyRoot/dir",
                R"(
                    Name: "test-secret"
                    Value: "test-value"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
                TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
            }
        });
    }

    Y_UNIT_TEST(AlterSecret) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "dir");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                TestCreateSecret(runtime, ++t.TxId, "/MyRoot/dir",
                    R"(
                        Name: "test-secret"
                        Value: "test-value-0"
                    )"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
            }

            TestAlterSecret(runtime, ++t.TxId, "/MyRoot/dir",
                R"(
                    Name: "test-secret"
                    Value: "test-value-1"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                const auto describeResult = DescribePathWithSecretValue(runtime, "/MyRoot/dir/test-secret");
                TestDescribeResult(describeResult, {NLs::Finished, NLs::IsSecret});
                ExpectEqualSecretDescription(describeResult, "test-secret", "test-value-1", 1);
            }
        });
    }

    Y_UNIT_TEST(DropSecret) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
                TestMkDir(runtime, ++t.TxId, "/MyRoot", "dir");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestCreateSecret(runtime, ++t.TxId, "/MyRoot/dir",
                    R"(
                        Name: "test-secret"
                        Value: "test-value"
                    )"
                );
                t.TestEnv->TestWaitNotification(runtime, t.TxId);
                TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathExist);
            }

            TestDropSecret(runtime, ++t.TxId, "/MyRoot/dir", "test-secret");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestLs(runtime, "/MyRoot/dir/test-secret", false, NLs::PathNotExist);
            }
        });
    }
}
