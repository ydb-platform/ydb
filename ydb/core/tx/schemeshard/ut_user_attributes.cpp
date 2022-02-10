#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardUserAttrsTest) {
    Y_UNIT_TEST(Boot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
    }

    Y_UNIT_TEST(MkDir) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TUserAttrs dirAAttrs{{"AttrA1", "ValA1"}, {"AttrA2", "ValA2"}};
        TestMkDir(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(dirAAttrs));
        TUserAttrs dirBAttrs{{"AttrB1", "ValB1"}, {"AttrB2", "ValB2"}};
        TestMkDir(runtime, txId++, "/MyRoot", "DirB", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(dirBAttrs));
        TUserAttrs subdirAAttrs{{"AttrAA1", "ValAA1"}, {"AttrAA2", "ValAA2"}};
        TestMkDir(runtime, txId++, "/MyRoot/DirA", "SubDirA", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(subdirAAttrs));
        TUserAttrs subdirBAttrs{{"AttrAB1", "ValAB1"}, {"AttrAB2", "ValAB2"}};
        TestMkDir(runtime, txId++, "/MyRoot/DirA/SubDirA", "DirB", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(subdirBAttrs));

        env.TestWaitNotification(runtime, {100, 101, 102, 103});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
            {NLs::Finished, NLs::UserAttrsEqual(dirAAttrs), NLs::PathVersionEqual(5)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
            {NLs::Finished, NLs::UserAttrsEqual(dirBAttrs), NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA"),
            {NLs::Finished, NLs::UserAttrsEqual(subdirAAttrs), NLs::PathVersionEqual(5)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA/DirB"),
            {NLs::Finished
             , NLs::UserAttrsEqual(subdirBAttrs)
             , NLs::UserAttrsHas({{"AttrAB1", "ValAB1"}})
             , NLs::UserAttrsHas({{"AttrAB2", "ValAB2"}})
             , NLs::PathVersionEqual(3)});
    }

    Y_UNIT_TEST(SetAttrs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, txId++, "/MyRoot", "DirA");

        AsyncUserAttrs(runtime, txId++, "/MyRoot", "DirA", AlterUserAttrs({{"AttrA1", "ValA1"}}));
        AsyncUserAttrs(runtime, txId++, "/MyRoot", "DirA", AlterUserAttrs({{"AttrAX", "ValAX"}}));
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);

        env.TestWaitNotification(runtime, {txId-2, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
            {NLs::UserAttrsEqual({{"AttrA1", "ValA1"}}), NLs::PathVersionEqual(4)});

        TestUserAttrs(runtime, txId++, "", "MyRoot", AlterUserAttrs({{"AttrRoot", "ValRoot"}}));
        env.TestWaitNotification(runtime, txId-1);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::UserAttrsEqual({{"AttrRoot", "ValRoot"}})});
    }

    Y_UNIT_TEST(UserConditionsAtAlter) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"AttrA1", "ValA1"}}));
        env.TestWaitNotification(runtime, {txId-2, txId-1});

        auto dirVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
            {NLs::UserAttrsEqual({{"AttrA1", "ValA1"}}), NLs::PathVersionEqual(3)});

        TestUserAttrs(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusAccepted},
                      AlterUserAttrs({}, {"AttrA1"}), {dirVer});
        env.TestWaitNotification(runtime, txId-1);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                    {NLs::UserAttrsEqual({}), NLs::PathVersionEqual(4)});

        TestUserAttrs(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusPreconditionFailed},
                      AlterUserAttrs({{"AttrA2", "ValA2"}}), {dirVer});
        env.TestWaitNotification(runtime, txId-1);
        dirVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                    {NLs::UserAttrsEqual({}), NLs::PathVersionEqual(4)});

        TestUserAttrs(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusAccepted},
                      AlterUserAttrs({{"AttrA2", "ValA2"}}), {dirVer});
        env.TestWaitNotification(runtime, txId-1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                            {NLs::UserAttrsEqual({{"AttrA2", "ValA2"}}), NLs::PathVersionEqual(5)});
    }

    Y_UNIT_TEST(UserConditionsAtCreateDropOps) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"AttrA1", "ValA1"}}));
        env.TestWaitNotification(runtime, {txId-2, txId-1});

        auto dirAVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
            {NLs::UserAttrsEqual({{"AttrA1", "ValA1"}}), NLs::PathVersionEqual(3)});
        auto rootVer = TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::Finished});

        TestMkDir(runtime, txId++, "/MyRoot", "DirB", {NKikimrScheme::StatusAccepted}, {}, {dirAVer, rootVer});

        auto dirBVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
            {NLs::Finished, NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
            {NLs::Finished, NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::Finished});

        TestMkDir(runtime, txId++, "/MyRoot", "DirC", {NKikimrScheme::StatusPreconditionFailed}, {}, {dirBVer, dirAVer, rootVer});
        rootVer = TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::Finished});
        TestMkDir(runtime, txId++, "/MyRoot", "DirC", {NKikimrScheme::StatusAccepted}, {}, {dirBVer, dirAVer, rootVer});
        env.TestWaitNotification(runtime, txId-1);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::Finished});
        auto dirCVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirC"),
            {NLs::Finished, NLs::PathVersionEqual(3)});

        TestRmDir(runtime, txId++, "/MyRoot", "DirC", {NKikimrScheme::StatusPreconditionFailed}, {dirCVer, dirBVer, dirAVer, rootVer});
        rootVer = TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::Finished});
        TestRmDir(runtime, txId++, "/MyRoot", "DirC", {NKikimrScheme::StatusAccepted}, {dirCVer, dirBVer, dirAVer, rootVer});
        env.TestWaitNotification(runtime, txId-1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirC"),
                    {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
            {NLs::Finished});
    }

    Y_UNIT_TEST(VariousUse) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusAccepted});

        env.TestWaitNotification(runtime, txId-1);

        AsyncUserAttrs(runtime, txId++, "/MyRoot", "DirA", AlterUserAttrs({{"AttrA1", "ValA1"}}));
        AsyncUserAttrs(runtime, txId++, "/MyRoot", "DirA", AlterUserAttrs({{"AttrA2", "ValA2"}}));
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);

        env.TestWaitNotification(runtime, {txId-2, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
            {NLs::UserAttrsEqual({{"AttrA1", "ValA1"}}), NLs::PathVersionEqual(4)});

        TestUserAttrs(runtime, txId++, "/MyRoot", "DirA", AlterUserAttrs({{"AttrA3", "ValA3"}}, {"AttrA1"}));
        env.TestWaitNotification(runtime, txId-1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
            {NLs::UserAttrsEqual({{"AttrA3", "ValA3"}}), NLs::PathVersionEqual(5)});

        TestUserAttrs(runtime, txId++, "/MyRoot", "DirA", AlterUserAttrs({}, {"AttrA3"}));
        env.TestWaitNotification(runtime, txId-1);
        auto dirAVer =TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                          {NLs::UserAttrsEqual({}), NLs::PathVersionEqual(6)});

        TestUserAttrs(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusAccepted},
                      AlterUserAttrs({{"AttrA6", "ValA6"}}), {dirAVer});
        env.TestWaitNotification(runtime, txId-1);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                    {NLs::Finished, NLs::UserAttrsEqual({{"AttrA6", "ValA6"}}), NLs::PathVersionEqual(7)});

        TestUserAttrs(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusPreconditionFailed},
                      AlterUserAttrs({}, {"AttrA6"}), {dirAVer});

        dirAVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                    {NLs::Finished, NLs::UserAttrsEqual({{"AttrA6", "ValA6"}}), NLs::PathVersionEqual(7)});

        TestUserAttrs(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusAccepted},
                      AlterUserAttrs({}, {"AttrA6"}), {dirAVer});
        env.TestWaitNotification(runtime, txId-1);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                            {NLs::Finished, NLs::UserAttrsEqual({}), NLs::PathVersionEqual(8)});

        TUserAttrs dirAAttrs{{"AttrB1", "ValB1"}};
        TestMkDir(runtime, txId++, "/MyRoot", "DirB", {NKikimrScheme::StatusPreconditionFailed}, AlterUserAttrs(dirAAttrs), {dirAVer});

        ++dirAVer.Version;
        TestMkDir(runtime, txId++, "/MyRoot", "DirB", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(dirAAttrs), {dirAVer});
        env.TestWaitNotification(runtime, txId-1);
        auto dirBVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                            {NLs::Finished, NLs::UserAttrsEqual(dirAAttrs), NLs::PathVersionEqual(3)});

        TestMkDir(runtime, txId++, "/MyRoot/DirB", "DirBB", {NKikimrScheme::StatusAccepted}, {}, {dirAVer, dirBVer});
        env.TestWaitNotification(runtime, txId-1);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                            {NLs::Finished, NLs::PathVersionEqual(5)});
        auto dirBBVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/DirBB"),
                            {NLs::Finished, NLs::PathVersionEqual(3)});

        TestRmDir(runtime, txId++, "/MyRoot/DirB", "DirBB", {NKikimrScheme::StatusPreconditionFailed}, {dirAVer, dirBVer});
        dirBVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                            {NLs::Finished, NLs::PathVersionEqual(5)});
        TestRmDir(runtime, txId++, "/MyRoot/DirB", "DirBB", {NKikimrScheme::StatusAccepted}, {dirAVer, dirBVer});
        env.TestWaitNotification(runtime, txId-1);
        dirBVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                    {NLs::Finished, NLs::NoChildren, NLs::PathVersionEqual(7)});

        TestRmDir(runtime, txId++, "/MyRoot", "DirB", {NKikimrScheme::StatusPreconditionFailed}, {dirAVer, dirBVer, dirBBVer});
    }

    Y_UNIT_TEST(SpecialAttributes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TUserAttrs dirAAttrs{{"__reserved_attribute", "Val1"}};
        TestMkDir(runtime, txId++, "/MyRoot", "DirA", {NKikimrScheme::StatusInvalidParameter}, AlterUserAttrs(dirAAttrs));

        TUserAttrs dirBAttrs{{"__volume_space_limit", "not a number"}};
        TestMkDir(runtime, txId++, "/MyRoot", "DirB", {NKikimrScheme::StatusInvalidParameter}, AlterUserAttrs(dirBAttrs));

        TUserAttrs dirCAttrs{{"__volume_space_limit", "4096"}};
        TestMkDir(runtime, txId++, "/MyRoot", "DirC", {NKikimrScheme::StatusAccepted}, AlterUserAttrs(dirCAttrs));

        TUserAttrs dirDAttrs{{"__extra_path_symbols_allowed", "./_"}};
        TestMkDir(runtime, txId++, "/MyRoot", "DirD", {NKikimrScheme::StatusInvalidParameter}, AlterUserAttrs(dirDAttrs));
    }
}
