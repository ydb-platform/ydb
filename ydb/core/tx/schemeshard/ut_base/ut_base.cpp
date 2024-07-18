#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

#include <util/generic/size_literals.h>
#include <util/string/cast.h>

#include <locale>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardTest) {
    Y_UNIT_TEST(Boot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
    }

    Y_UNIT_TEST(InitRootAgain) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);


        auto result = env.InitRoot(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), "MyRoot");
        UNIT_ASSERT_VALUES_EQUAL((ui32)result, (ui32)TEvSchemeShard::TEvInitRootShardResult::StatusAlreadyInitialized);
    }

    Y_UNIT_TEST(InitRootWithOwner) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TString newOwner = "something@builtin";

        auto result = env.InitRoot(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), "MyRoot", {}, newOwner);
        UNIT_ASSERT_VALUES_EQUAL((ui32)result, (ui32)TEvSchemeShard::TEvInitRootShardResult::StatusSuccess);

        auto checkOwner = [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
            const auto& self = record.GetPathDescription().GetSelf();
            UNIT_ASSERT_EQUAL(self.GetOwner(), newOwner);
        };

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {checkOwner});
    }

    Y_UNIT_TEST(MkRmDir) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/", "DirA", {NKikimrScheme::StatusPathDoesNotExist});
        TestMkDir(runtime, ++txId, "/", "MyRoot", {NKikimrScheme::StatusPathDoesNotExist});
        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        TestMkDir(runtime, ++txId, "/MyRoot", "DirB");
        TestMkDir(runtime, ++txId, "/MyRoot/DirA", "SubDirA");
        TestMkDir(runtime, ++txId, "/MyRoot/DirA/SubDirA", "DirB");

        env.TestWaitNotification(runtime, xrange(txId - 5, txId + 1));

        TestDescribeResult(DescribePath(runtime, "/"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished,
                            NLs::PathsInsideDomain(4)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA/DirB"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3)});

        TestRmDir(runtime, ++txId, "/MyRoot/DirA", "SubDirA", {NKikimrScheme::StatusNameConflict});
        TestRmDir(runtime, ++txId, "/MyRoot/DirA/SubDirA", "DirB");
        env.TestWaitNotification(runtime, txId);
        TestRmDir(runtime, ++txId, "/MyRoot/DirA", "SubDirA");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA/DirB"), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA"), {NLs::PathNotExist});

        //

        TestMkDir(runtime, ++txId, "/MyRoot/DirA/SubDirA", "DirB", {NKikimrScheme::StatusPathDoesNotExist});

        //

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA", {NKikimrScheme::StatusAlreadyExists});
        TestRmDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished,
                            NLs::PathsInsideDomain(2)});

        //

        TestRmDir(runtime, ++txId, "/MyRoot", "DirA");
        TestRmDir(runtime, ++txId, "/MyRoot", "DirB");
        env.TestWaitNotification(runtime, {txId-1, txId});
        TestRmDir(runtime, ++txId, "/", "MyRoot", {NKikimrScheme::StatusNameConflict});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::NoChildren,
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(PathName) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "NameInEnglish");
        TestMkDir(runtime, ++txId, "/MyRoot", "НазваниеНаРусском", {NKikimrScheme::StatusSchemeError});

        env.TestWaitNotification(runtime, xrange(txId - 1, txId + 1));

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/NameInEnglish"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/НазваниеНаРусском"),
                           {NLs::PathNotExist});
    }

    class TLocaleGuard {
    public:
        explicit TLocaleGuard(const std::locale& targetLocale)
            : OriginalLocale_(std::locale::global(targetLocale))
        {
        }
        ~TLocaleGuard() {
            std::locale::global(OriginalLocale_);
        }

    private:
        const std::locale OriginalLocale_;
    };

    Y_UNIT_TEST(PathName_SetLocale) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        try {
            TLocaleGuard localeGuard(std::locale("C.UTF-8"));
            TestMkDir(runtime, ++txId, "/MyRoot", "НазваниеНаРусском", {NKikimrScheme::StatusSchemeError});

            env.TestWaitNotification(runtime, txId);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/НазваниеНаРусском"),
                            {NLs::PathNotExist});
        } catch (std::runtime_error) {
            // basic utf-8 locale is absent in the system, abort the test
        }
    }

    using TRuntimeTxFn = std::function<void(TTestBasicRuntime&, ui64)>;

    void DropTwice(const TString& path, TRuntimeTxFn createFn, TRuntimeTxFn dropFn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        createFn(runtime, ++txId);
        env.TestWaitNotification(runtime, txId);

        dropFn(runtime, ++txId);
        dropFn(runtime, ++txId);
        TestModificationResult(runtime, txId - 1);

        auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
        UNIT_ASSERT(ev);

        const auto& record = ev->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusMultipleModifications);
        UNIT_ASSERT_VALUES_EQUAL(record.GetPathDropTxId(), txId - 1);

        env.TestWaitNotification(runtime, txId - 1);
        TestDescribeResult(DescribePath(runtime, path), {
            NLs::PathNotExist
        });
    }

    Y_UNIT_TEST(RmDirTwice) {
        auto createFn = [](TTestBasicRuntime& runtime, ui64 txId) {
            TestMkDir(runtime, txId, "/MyRoot", "Dir");
        };

        auto dropFn = [](TTestBasicRuntime& runtime, ui64 txId) {
            AsyncRmDir(runtime, txId, "/MyRoot", "Dir");
        };

        DropTwice("/MyRoot/Dir", createFn, dropFn);
    }

    Y_UNIT_TEST(DropTableTwice) {
        auto createFn = [](TTestBasicRuntime& runtime, ui64 txId) {
            TestCreateTable(runtime, txId, "/MyRoot", R"(
                  Name: "Table"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value" Type: "Utf8" }
                  KeyColumnNames: ["key"]
            )");
        };

        auto dropFn = [](TTestBasicRuntime& runtime, ui64 txId) {
            AsyncDropTable(runtime, txId, "/MyRoot", "Table");
        };

        DropTwice("/MyRoot/Table", createFn, dropFn);
    }

    Y_UNIT_TEST(CacheEffectiveACL) {

        TEffectiveACL firstACL;

        {
            NACLib::TDiffACL diff;
            diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff");
            diff.AddAccess(NACLib::EAccessType::Allow, NACLib::ConnectDatabase, "1@staff", NACLib::InheritNone);
            diff.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "2@staff", NACLib::InheritObject);
            diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "3@staff", NACLib::InheritContainer);
            diff.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "4@staff", NACLib::InheritOnly);
            diff.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "5@staff", NACLib::InheritObject | NACLib::InheritOnly);
            diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "6@staff", NACLib::InheritContainer | NACLib::InheritOnly);
            diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer | NACLib::InheritOnly);

            NACLib::TACL input;
            input.ApplyDiff(diff);

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "2@staff", NACLib::InheritObject);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "4@staff", NACLib::InheritOnly);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "5@staff", NACLib::InheritObject | NACLib::InheritOnly);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff");
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::ConnectDatabase, "1@staff", NACLib::InheritNone);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "3@staff", NACLib::InheritContainer);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "6@staff", NACLib::InheritContainer | NACLib::InheritOnly);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer | NACLib::InheritOnly);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << input.DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), input.DebugString());
            }

            Y_ASSERT(!firstACL);
            firstACL.Init(input.SerializeAsString());
            Y_ASSERT(firstACL);
        }

        {
            // InheritOnly is not filtered from self effective.
            // Record with InheritOnly doesn't counted at CheckAccess
            // InheritOnly flag is eliminated on inheriting
            NACLib::TACL canonic;
            canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "2@staff", NACLib::InheritObject);
            canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "4@staff", NACLib::InheritOnly);
            canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "5@staff", NACLib::InheritObject | NACLib::InheritOnly);

            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff");
            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::ConnectDatabase, "1@staff", NACLib::InheritNone);
            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "3@staff", NACLib::InheritContainer);
            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "6@staff", NACLib::InheritContainer | NACLib::InheritOnly);
            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer | NACLib::InheritOnly);


            Cerr << "canonic: " << canonic.DebugString() << Endl;
            Cerr << "result: " << NACLib::TACL(firstACL.GetForSelf()).DebugString() << Endl;
            UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(firstACL.GetForSelf()).DebugString());
        }

        {
            NACLib::TACL canonic;
            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "3@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "6@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

            Cerr << "canonic: " << canonic.DebugString() << Endl;
            Cerr << "result: " << NACLib::TACL(firstACL.GetForChildren(/*isContainer=*/ true)).DebugString() << Endl;
            UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(firstACL.GetForChildren(/*isContainer=*/ true)).DebugString());
        }

        {
            NACLib::TACL canonic;
            canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "2@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);
            canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "5@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);

            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
            canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

            Cerr << "canonic: " << canonic.DebugString() << Endl;
            Cerr << "result: " << NACLib::TACL(firstACL.GetForChildren(/*isContainer=*/ false)).DebugString() << Endl;
            UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(firstACL.GetForChildren(/*isContainer=*/ false)).DebugString());
        }

        {
            TEffectiveACL secondACL;

            {
                NACLib::TDiffACL diff;
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "00@staff");
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::ConnectDatabase, "11@staff", NACLib::InheritNone);
                diff.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "22@staff", NACLib::InheritObject);
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "33@staff", NACLib::InheritContainer);
                diff.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "44@staff", NACLib::InheritOnly);
                diff.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "55@staff", NACLib::InheritObject | NACLib::InheritOnly);
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "66@staff", NACLib::InheritContainer | NACLib::InheritOnly);
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "77@staff", NACLib::InheritObject | NACLib::InheritContainer | NACLib::InheritOnly);

                NACLib::TACL input;
                input.ApplyDiff(diff);

                Y_ASSERT(!secondACL);
                secondACL.Update(firstACL, input.SerializeAsString(), /*isContainer=*/ true);
                Y_ASSERT(secondACL);
            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "22@staff", NACLib::InheritObject);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "44@staff", NACLib::InheritOnly);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "55@staff", NACLib::InheritObject | NACLib::InheritOnly);


                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "3@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "6@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "00@staff");
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::ConnectDatabase, "11@staff", NACLib::InheritNone);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "33@staff", NACLib::InheritContainer);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "66@staff", NACLib::InheritContainer | NACLib::InheritOnly);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "77@staff", NACLib::InheritObject | NACLib::InheritContainer | NACLib::InheritOnly);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForSelf()).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForSelf()).DebugString());
            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "3@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "6@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "00@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "33@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "66@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "77@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ true)).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ true)).DebugString());
            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "22@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "55@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "00@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "77@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ false)).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ false)).DebugString());
            }
        }

        {
            TEffectiveACL secondACL;

            {
                NACLib::TDiffACL diff;
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "00@staff");
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::ConnectDatabase, "11@staff", NACLib::InheritNone);
                diff.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "22@staff", NACLib::InheritObject);
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "33@staff", NACLib::InheritContainer);
                diff.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "44@staff", NACLib::InheritOnly);
                diff.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "55@staff", NACLib::InheritObject | NACLib::InheritOnly);
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "66@staff", NACLib::InheritContainer | NACLib::InheritOnly);
                diff.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "77@staff", NACLib::InheritObject | NACLib::InheritContainer | NACLib::InheritOnly);

                NACLib::TACL input;
                input.ApplyDiff(diff);

                Y_ASSERT(!secondACL);
                secondACL.Update(firstACL, input.SerializeAsString(),  /*isContainer=*/ false);
                Y_ASSERT(secondACL);
            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "2@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "5@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "22@staff", NACLib::InheritObject);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "44@staff", NACLib::InheritOnly);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "55@staff", NACLib::InheritObject | NACLib::InheritOnly);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "00@staff");
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::ConnectDatabase, "11@staff", NACLib::InheritNone);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "33@staff", NACLib::InheritContainer);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "66@staff", NACLib::InheritContainer | NACLib::InheritOnly);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "77@staff", NACLib::InheritObject | NACLib::InheritContainer | NACLib::InheritOnly);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForSelf()).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForSelf()).DebugString());
            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "00@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "33@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "66@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "77@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ true)).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ true)).DebugString());
            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "2@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "5@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "22@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "55@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "00@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "77@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ false)).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ false)).DebugString());
            }
        }

        {
            TEffectiveACL secondACL;

            NACLib::TACL input; // empty

            Y_ASSERT(!secondACL);
            secondACL.Update(firstACL, input.SerializeAsString(),  /*isContainer=*/ true);
            Y_ASSERT(secondACL);

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "3@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "6@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForSelf()).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForSelf()).DebugString());
            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "3@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "6@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ true)).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ true)).DebugString());
            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "2@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "5@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ false)).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ false)).DebugString());
            }
        }

        {
            TEffectiveACL secondACL;

            NACLib::TACL input; // empty

            Y_ASSERT(!secondACL);
            secondACL.Update(firstACL, input.SerializeAsString(),  /*isContainer=*/ false);
            Y_ASSERT(secondACL);

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "2@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "5@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForSelf()).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForSelf()).DebugString());            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "3@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "6@staff", NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ true)).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ true)).DebugString());
            }

            {
                NACLib::TACL canonic;
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "2@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericRead, "5@staff", NACLib::InheritObject); canonic.MutableACE()->rbegin()->SetInherited(true);

                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "0@staff"); canonic.MutableACE()->rbegin()->SetInherited(true);
                canonic.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "7@staff", NACLib::InheritObject | NACLib::InheritContainer); canonic.MutableACE()->rbegin()->SetInherited(true);

                Cerr << "canonic: " << canonic.DebugString() << Endl;
                Cerr << "result: " << NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ false)).DebugString() << Endl;
                UNIT_ASSERT_NO_DIFF(canonic.DebugString(), NACLib::TACL(secondACL.GetForChildren(/*isContainer=*/ false)).DebugString());
            }
        }

    }

    Y_UNIT_TEST(ModifyACL) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::NoChildren,
                            NLs::PathVersionEqual(3)});

        {
            AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");

            NACLib::TDiffACL diffACL;
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user0@builtin");

            AsyncModifyACL(runtime, ++txId, "/MyRoot", "DirA", diffACL.SerializeAsString(), "svc@staff");

            TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
            TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);

            env.TestWaitNotification(runtime, {txId, txId-1});
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(1)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(5),
                            NLs::HasEffectiveRight("+U:user0@builtin")});

        {
            NACLib::TDiffACL diffACL;
            diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1@builtin", NACLib::InheritNone);

            TestModifyACL(runtime, ++txId, "/", "MyRoot", diffACL.SerializeAsString(), "");
            env.TestWaitNotification(runtime, txId);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(1),
                            NLs::HasEffectiveRight("+U:user1@builtin:-")});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(6),
                            NLs::HasEffectiveRight("+U:user0@builtin"),
                            NLs::HasNotEffectiveRight("+U:user1@builtin:-")});

        {
            TestMkDir(runtime, ++txId, "/MyRoot/DirA", "DirB");
            env.TestWaitNotification(runtime, txId);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/DirB"),
                           {NLs::PathVersionEqual(5),
                            NLs::HasEffectiveRight("+U:user0@builtin"),
                            NLs::HasNotEffectiveRight("+U:user1@builtin:-")});

    }

    Y_UNIT_TEST(NameFormat) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\""
                            "Columns { Name: \"RowId!\"      Type: \"Yson\"}"
                            "KeyColumnNames: [\"RowId\"]",
                        {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\""
                            "Columns { Name: \"RowId?\"      Type: \"Yson\"}"
                            "KeyColumnNames: [\"RowId\"]",
                        {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\""
                            "Columns { Name: \"RowId@\"      Type: \"Yson\"}"
                            "KeyColumnNames: [\"RowId\"]",
                        {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\""
                            "Columns { Name: \"RowId:\"      Type: \"Yson\"}"
                            "KeyColumnNames: [\"RowId\"]",
                        {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\""
                            "Columns { Name: \"RowId:\"      Type: \"Yson\"}"
                            "KeyColumnNames: [\"RowId\"]",
                        {NKikimrScheme::StatusSchemeError});

        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir0!");
        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir0?");
        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir0@");
        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir0:");

        TestModificationResult(runtime, txId-3, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1, txId-2, txId-3});

        TSchemeLimits lowLimits;
        lowLimits.ExtraPathSymbolsAllowed = "_.-";
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1!", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1?", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1@", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir1:", {NKikimrScheme::StatusSchemeError});

        lowLimits.ExtraPathSymbolsAllowed = "!?@:";
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1!");
        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir1@");

        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1});

        lowLimits.ExtraPathSymbolsAllowed = "!";
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1!"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir1@"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3)});

        AsyncMkDir(runtime, ++txId, "/MyRoot/Dir1!", "Ok");
        AsyncMkDir(runtime, ++txId, "/MyRoot/Dir1@", "Ok");

        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(8),
                            NLs::ChildrenCount(6)});
    }

    Y_UNIT_TEST(CreateTable) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");

        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table2\""
                            "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                            "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "Columns { Name: \"YaValue\"    Type: \"Yson\"}"
                            "Columns { Name: \"MoreValue\"  Type: \"Json\"}"
                            "KeyColumnNames: [\"RowId\", \"key1\", \"key2\"]"
                        );
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"x/y\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]"
                        );
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table3\""
                            "Columns { Name: \"RowId\"      Type: \"Yson\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table3\""
                            "Columns { Name: \"RowId\"      Type: \"Json\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table3\""
                            "Columns { Name: \"RowId\"      Type: \"Json\"}"
                            "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\", \"key1\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA/Table1",
                        "Name: \"x/y\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Name with spaces\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"TableN\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "Columns { Name: \"column with spaces\" Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"TableN\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "Columns { Name: \"кокошник\"  Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"*.#\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "Columns { Name: \"!a~\" Type: \"Uint32\"}"
                            "Columns { Name: \"(a+b)*c=5\" Type: \"Uint32\"}"
                            "Columns { Name: \"1-x/y+q^6\" Type: \"Uint32\"}"
                            "Columns { Name: \"!@#$%^&*()_-+=~:;,./?{}|\" Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]");
        AsyncCreateTable(runtime, ++txId, "/NotMyRoot/DirA",
                        "Name: \"Table3\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table3\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\", \"key\"]");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table3\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}");

        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table3\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]"
                            "PartitionConfig {ChannelProfileId: 42}");

        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table3\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]"
                            "PartitionConfig {ChannelProfileId: 1}");

        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table4\""
                            "Columns { Name: \"key\"       Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]"
                            "PartitionConfig {ChannelProfileId: 0}");

        TestModificationResult(runtime, txId-18, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-17, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-16, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-15, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-14, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-13, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-12, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-11, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-10, NKikimrScheme::StatusPathIsNotDirectory);
        TestModificationResult(runtime, txId-9, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-8, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-7, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-6, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-5, NKikimrScheme::StatusPathDoesNotExist);
        TestModificationResult(runtime, txId-4, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-3, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusInvalidParameter);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, xrange(txId - 18, txId + 1));

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::ChildrenCount(1)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(13),
                            NLs::ChildrenCount(5)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table1"),
                           {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table2"),
                           {NLs::PathExist});
    }

    Y_UNIT_TEST(CreateTableWithDate) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "DateInColumns"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Date" }
            KeyColumnNames: ["key"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Uint64 : 200 } }
              }
            }
        )");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "DateInKeys"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Date" }
            KeyColumnNames: ["key", "value"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Uint64 : 200 } }
                Tuple { Optional { Uint32 : 678 } }
              }
            }
        )");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TzDateInKeys"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "TzDate" }
            KeyColumnNames: ["key", "value"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Uint64 : 200 } }
                Tuple { Optional { Uint32 : 678 } }
              }
            }
        )", {NKikimrScheme::StatusSchemeError}); // Type 'TzDate' specified for column 'value' is not supported by storage

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TzDateInColumns"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "TzDate" }
            KeyColumnNames: ["key"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Uint64 : 200 } }
              }
            }
        )", {NKikimrScheme::StatusSchemeError}); // Type 'TzDate' specified for column 'value' is not supported by storage

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "DatetimeInKeys"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Datetime" }
            KeyColumnNames: ["key", "value"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Uint64 : 200 } }
                Tuple { Optional { Uint32 : 678 } }
              }
            }
        )");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TzDatetimeInKeys"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "TzDatetime" }
            KeyColumnNames: ["key", "value"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Uint64 : 200 } }
                Tuple { Optional { Uint32 : 678 } }
              }
            }
        )", {NKikimrScheme::StatusSchemeError});

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TimestampInKeys"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Timestamp" }
            KeyColumnNames: ["key", "value"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Uint64 : 200 } }
                Tuple { Optional { Uint64 : 18446744073709551606 } }
              }
            }
        )");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TzTimestampInKeys"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "TzTimestamp" }
            KeyColumnNames: ["key", "value"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Uint64 : 200 } }
                Tuple { Optional { Uint64 : 678 } }
              }
            }
        )", {NKikimrScheme::StatusSchemeError});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "TzTimestampInIndex"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "TzTimestamp" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "TimestampInIndex"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Timestamp" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
        )");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "IntervalInKeys"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Interval" }
            KeyColumnNames: ["key", "value"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Uint64 : 200 } }
                Tuple { Optional { Int64 : -9223372036854775807 } }
              }
            }
        )");

        env.TestWaitNotification(runtime, {100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111});

        TestDescribeResult(DescribePath(runtime, "/MyRoot", true, true),
                           {NLs::PathExist,
                            NLs::Finished,
                            NLs::ShardsInsideDomain(12)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DateInColumns", true, true),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DateInKeys", true, true),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TzDateInKeys", true, true),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TzDateInColumns", true, true),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DatetimeInKeys", true, true),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TzDatetimeInKeys", true, true),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TimestampInKeys", true, true),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                                UNIT_ASSERT_EQUAL(record.GetPathDescription()
                                                      .GetTable()
                                                      .GetSplitBoundary(0)
                                                      .GetKeyPrefix()
                                                      .GetTuple(1)
                                                      .GetOptional()
                                                      .GetUint64()
                                                      , ui64(18446744073709551606ul));
                            }});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TzTimestampInIndex", true, true),
                           {NLs::PathNotExist});


        TestDescribeResult(DescribePath(runtime, "/MyRoot/TimestampInIndex", true, true),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::IndexesCount(1)
                            });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/TzTimestampInKeys", true, true),
                           {NLs::PathNotExist});


        TestDescribeResult(DescribePath(runtime, "/MyRoot/IntervalInKeys", true, true),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                                UNIT_ASSERT_EQUAL(record.GetPathDescription()
                                                      .GetTable()
                                                      .GetSplitBoundary(0)
                                                      .GetKeyPrefix()
                                                      .GetTuple(1)
                                                      .GetOptional()
                                                      .GetInt64()
                                                      , i64(-9223372036854775807l));
                            }});
    }

    Y_UNIT_TEST(ConsistentCopyTable) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");

        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        AsyncCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "src2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValues"
              KeyColumnNames: ["value0", "value1"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0CoveringValue1"
              KeyColumnNames: ["value0"]
              DataColumnNames: ["value1"]
            }
        )");

        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1 , txId-2});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(7),
                            NLs::Finished,
                            NLs::PathExist,
                            NLs::PathsInsideDomain(9),
                            NLs::ShardsInsideDomain(5),
                            NLs::ChildrenCount(2)
                           });

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirA/dst2"
                      })");
        env.TestWaitNotification(runtime, txId);

        auto dst1Version = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/dst1"),
                                              {NLs::PathVersionEqual(3),
                                               NLs::PathExist,
                                               NLs::Finished});

        auto dst2Version = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/dst2"),
                                              {NLs::PathVersionEqual(3),
                                               NLs::PathExist,
                                               NLs::Finished});

        auto dirAVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                              {NLs::PathVersionEqual(11),
                                               NLs::PathExist,
                                               NLs::Finished,
                                               NLs::PathExist,
                                               NLs::PathsInsideDomain(17),
                                               NLs::ShardsInsideDomain(10)});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/dst1"
                         DstPath: "/MyRoot/DirA/seconddst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/dst2"
                        DstPath: "/MyRoot/DirA/seconddst2"
                      }
         )", {NKikimrScheme::StatusAccepted}, {dst1Version, dst2Version, dirAVersion});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(15),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::PathsInsideDomain(25),
                            NLs::ShardsInsideDomain(15),
                            NLs::ChildrenCount(6)
                           });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/src1"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::CreatedAt(txId-3)
                           });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/src2"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::CreatedAt(txId-2)
                           });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/dst1"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::CreatedAt(txId-1)
                           });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/dst2"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::CreatedAt(txId-1)
                           });

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/dst2/UserDefinedIndexByValue0CoveringValue1"),
                           {NLs::Finished,
                            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value0"}),
                            NLs::IndexDataColumns({"value1"})});

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/dst2/UserDefinedIndexByValue0CoveringValue1/indexImplTable"),
                           {NLs::Finished});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/seconddst1"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::IndexesCount(0),
                            NLs::CreatedAt(txId)
                           });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/seconddst2"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::IndexesCount(3),
                            NLs::CreatedAt(txId)
                           });

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/seconddst1"
                         DstPath: "/MyRoot/DirA/thirddst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/seconddst2"
                        DstPath: "/MyRoot/DirA/thirddst2"
                        OmitIndexes: true
                      }
         )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(19),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::PathsInsideDomain(27),
                            NLs::ShardsInsideDomain(17),
                            NLs::ChildrenCount(8)
                           });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/thirddst2"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::IndexesCount(0), //Omit Indexes
                            NLs::CreatedAt(txId)
                           });
    }

    Y_UNIT_TEST(ConsistentCopyTableAwait) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");

        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(7),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::PathsInsideDomain(3),
                            NLs::ChildrenCount(2),
                            NLs::ShardsInsideDomain(2)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/src1"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/src2"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished});

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirB");
        AsyncConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirB/await1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirB/await2"
                      }
                )");
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-1, txId});

        AsyncConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirB/await1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirB/await2"
                      }
                )");
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(7),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::ChildrenCount(2),
                            NLs::PathsInsideDomain(6),
                            NLs::ShardsInsideDomain(4)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                           {NLs::PathVersionEqual(7),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::ChildrenCount(2)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/await1"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::CreatedAt(txId)
                           });

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB/await2"),
                           {NLs::PathVersionEqual(3),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::CreatedAt(txId)
                           });
    }

    Y_UNIT_TEST(ConsistentCopyTableRejects) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src3"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        TestModificationResult(runtime, txId-3, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId-3, txId-2, txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(9),
                            NLs::PathExist,
                            NLs::ChildrenCount(3)
                           });

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src1"
                        DstPath: "/MyRoot/DirA/dst2"
                      }
         )", {NKikimrScheme::StatusInvalidParameter});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
         )", {NKikimrScheme::StatusInvalidParameter});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/src1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirA/dst2"
                      }
         )", {NKikimrScheme::StatusSchemeError});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirA/dst2"
                      }
         )", {NKikimrScheme::StatusNameConflict});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: ""
                        DstPath: "/MyRoot/DirA/dst2"
                      }
         )", {NKikimrScheme::StatusPathDoesNotExist});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/notExist"
                        DstPath: "/MyRoot/DirA/dst2"
                      }
         )", {NKikimrScheme::StatusPathDoesNotExist});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/src2"
                       }
         )", {NKikimrScheme::StatusSchemeError});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/notExist/dst2"
                      }
         )", {NKikimrScheme::StatusPathDoesNotExist});

        {
            TSchemeLimits lowLimits;
            lowLimits.ExtraPathSymbolsAllowed = "-_.";
            SetSchemeshardSchemaLimits(runtime, lowLimits);

            TestConsistentCopyTables(runtime, ++txId, "/", R"(
                           CopyTableDescriptions {
                             SrcPath: "/MyRoot/DirA/src1"
                             DstPath: "/MyRoot/DirA/dst1{not*allowed$symbols!"
                           }
                          CopyTableDescriptions {
                            SrcPath: "/MyRoot/DirA/src2"
                            DstPath: "/MyRoot/DirA/dst2"
                          }
             )", {NKikimrScheme::StatusSchemeError});
        }

        auto dirAVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                              {NLs::PathVersionEqual(9),
                                               NLs::PathExist,
                                               NLs::Finished});

        AsyncConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirA/dst2"
                      }
         )");
        ui64 copyingTx = txId;
        //Still in fly checks

        //retry, must never happen with the same txId, but check that behavior just in case
        AsyncConsistentCopyTables(runtime, copyingTx, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src2"
                         DstPath: "/MyRoot/DirA/dst2"
                       }
         )");

        //retry with different body, just in case
        AsyncConsistentCopyTables(runtime, copyingTx, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src2"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst2"
                       })");

        //simultaneous
        AsyncConsistentCopyTables(runtime, ++txId, "/", R"(
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src1"
                        DstPath: "/MyRoot/DirA/dst1"
                      }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirA/dst2"
                      })");

        AsyncConsistentCopyTables(runtime, ++txId, "/", R"(
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src3"
                        DstPath: "/MyRoot/DirA/dst1"
                      })");

        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);

        env.TestWaitNotification(runtime, copyingTx);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(13),
                            NLs::PathExist,
                            NLs::Finished,
                            NLs::ChildrenCount(5)});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/x1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirA/x2"
                      }
         )", {NKikimrScheme::StatusPreconditionFailed}, {dirAVersion});

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "WithEnabledExternalBlobs"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
              PartitionConfig {
                ColumnFamilies {
                  Id: 0
                  ColumnCodec: ColumnCodecLZ4
                  ColumnCache: ColumnCacheEver
                  StorageConfig {
                    SysLog {
                      PreferredPoolKind: "hdd-1"
                    }
                    Log {
                      PreferredPoolKind: "hdd-1"
                    }
                    Data {
                      PreferredPoolKind: "hdd-1"
                    }
                    External {
                      PreferredPoolKind: "hdd-2"
                    }
                    ExternalThreshold: 5604288
                  }
                }
              })");
        env.TestWaitNotification(runtime, txId);

        TestCopyTable(runtime, ++txId, "/MyRoot/DirA", "NotOk", "/MyRoot/DirA/WithEnabledExternalBlobs",
                      NKikimrScheme::StatusPreconditionFailed);
        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/IsOk"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/WithEnabledExternalBlobs"
                        DstPath: "/MyRoot/DirA/NotOk"
                      }
         )", {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "IndexedTableWithEnabledExternalBlobs"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
              PartitionConfig {
                ColumnFamilies {
                  Id: 0
                  ColumnCodec: ColumnCodecLZ4
                  ColumnCache: ColumnCacheEver
                  StorageConfig {
                    SysLog {
                      PreferredPoolKind: "hdd-1"
                    }
                    Log {
                      PreferredPoolKind: "hdd-1"
                    }
                    Data {
                      PreferredPoolKind: "hdd-1"
                    }
                    External {
                      PreferredPoolKind: "hdd-2"
                    }
                    ExternalThreshold: 5604288
                  }
                }
              }
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCopyTable(runtime, ++txId, "/MyRoot/DirA", "NotOk", "/MyRoot/DirA/IndexedTableWithEnabledExternalBlobs",
                      NKikimrScheme::StatusPreconditionFailed);
        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/IsOk"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/IndexedTableWithEnabledExternalBlobs"
                        DstPath: "/MyRoot/DirA/NotOk"
                      }
         )", {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestAlterTable(runtime, ++txId, "/MyRoot/DirA", R"(
                            Name: "IndexedTableWithEnabledExternalBlobs"
                            PartitionConfig {
                             ColumnFamilies {
                               Id: 0
                               ColumnCodec: ColumnCodecLZ4
                               ColumnCache: ColumnCacheEver
                               StorageConfig {
                                 ExternalThreshold: 0
                               }
                             }
                            })");
        env.TestWaitNotification(runtime, txId);

        TestCopyTable(runtime, ++txId, "/MyRoot/DirA", "NotOk", "/MyRoot/DirA/IndexedTableWithEnabledExternalBlobs",
                      NKikimrScheme::StatusPreconditionFailed);

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/IsOk"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/IndexedTableWithEnabledExternalBlobs"
                        DstPath: "/MyRoot/DirA/NotOk"
                      }
         )", {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, {txId-1, txId});
    }

    Y_UNIT_TEST(ConsistentCopyTableToDeletedPath) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");

        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "src2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(7),
                            NLs::PathExist,
                            NLs::ChildrenCount(2)
                           });

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/DirA/src1"
                         DstPath: "/MyRoot/DirA/dst1"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirA/dst2"
                      }
         )");
        env.TestWaitNotification(runtime, txId);

        AsyncDropTable(runtime, ++txId, "/MyRoot/DirA", "dst1");
        AsyncDropTable(runtime, ++txId, "/MyRoot/DirA", "dst2");
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId, txId-1});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src1"
                        DstPath: "/MyRoot/DirA/dst1"
                      }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirA/dst2"
                      }
         )");
        env.TestWaitNotification(runtime, txId);

        //simultaneously
        AsyncDropTable(runtime, ++txId, "/MyRoot/DirA", "dst1");
        AsyncDropTable(runtime, ++txId, "/MyRoot/DirA", "dst2");
        AsyncConsistentCopyTables(runtime, ++txId, "/", R"(
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src1"
                        DstPath: "/MyRoot/DirA/dst1"
                      }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/DirA/src2"
                        DstPath: "/MyRoot/DirA/dst2"
                      }
         )");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});
    }

    Y_UNIT_TEST(CreateIndexedTable) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue1"
              KeyColumnNames: ["value1"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValues"
              KeyColumnNames: ["value0", "value1"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0CoveringValue1"
              KeyColumnNames: ["value0"]
              DataColumnNames: ["value1"]
            }
        )");
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table1"),
                           {NLs::NotFinished,
                            NLs::PathVersionEqual(1),
                            NLs::IndexesCount(0)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/Table1/UserDefinedIndexByValue1"),
                           {NLs::NotFinished});
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::ChildrenCount(1)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table1"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3),
                            NLs::IndexesCount(4)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0"),
                           {NLs::Finished,
                            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value0"})});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0/indexImplTable"),
                           {NLs::Finished});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/Table1/UserDefinedIndexByValue1"),
                           {NLs::Finished,
                            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value1"})});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/Table1/UserDefinedIndexByValue1/indexImplTable", true, true),
                           {NLs::Finished,
                            NLs::NoMaxPartitionsCount,
                            NLs::SizeToSplitEqual(2<<30)}); // 2G
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/Table1/UserDefinedIndexByValues"),
                           {NLs::Finished,
                            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value0", "value1"})});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/Table1/UserDefinedIndexByValues/indexImplTable"),
                           {NLs::Finished});


        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0CoveringValue1"),
                           {NLs::Finished,
                            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value0"}),
                            NLs::IndexDataColumns({"value1"})});

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0CoveringValue1/indexImplTable"),
                           {NLs::Finished});

        TestDropTable(runtime, ++txId, "/MyRoot/DirA", "Table1");
        env.TestWaitNotification(runtime, 103);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathExist,
                            NLs::ChildrenCount(0)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table1"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 10));
    }

    Y_UNIT_TEST(CopyIndexedTable) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue1"
              KeyColumnNames: ["value1"]
            }
            IndexDescription {
             Name: "UserDefinedIndexByValue0CoveringValue1"
             KeyColumnNames: ["value0"]
             DataColumnNames: ["value1"]
           }
        )");
        env.TestWaitNotification(runtime, {txId, txId-1});


        TestCopyTable(runtime, ++txId, "/MyRoot/DirA", "copy", "/MyRoot/DirA/Table1");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/copy"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3),
                            NLs::IndexesCount(3)});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/copy/UserDefinedIndexByValue0"),
                           {NLs::Finished,
                            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value0"})});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/copy/UserDefinedIndexByValue0/indexImplTable"),
                           {NLs::Finished});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/copy/UserDefinedIndexByValue1"),
                           {NLs::Finished,
                            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value1"})});
        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/copy/UserDefinedIndexByValue1/indexImplTable"),
                           {NLs::Finished});

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/copy/UserDefinedIndexByValue0CoveringValue1"),
                           {NLs::Finished,
                            NLs::IndexType(NKikimrSchemeOp::EIndexTypeGlobal),
                            NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
                            NLs::IndexKeys({"value0"}),
                            NLs::IndexDataColumns({"value1"})});

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/DirA/copy/UserDefinedIndexByValue0CoveringValue1/indexImplTable"),
                           {NLs::Finished});
    }

    Y_UNIT_TEST(CreateIndexedTableRejects) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue1"
              KeyColumnNames: ["value1"]
            }
        )");
        env.TestWaitNotification(runtime, {100, 101, 102});

        //the same table
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue1"
              KeyColumnNames: ["value1"]
            }
        )", {TEvSchemeShard::EStatus::StatusAlreadyExists});

        //the same index name
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value1"]
            }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});

        //value_not_exist
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value_not_exist"]
            }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});

        //no directory
        TestCreateIndexedTable(runtime, txId++, "/MyRoot/USER_0", R"(
                TableDescription {
                  Name: "Table2"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value0" Type: "Utf8" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0"
                }
            )", {NKikimrScheme::StatusPathDoesNotExist});

        //no key in index descr
        TestCreateIndexedTable(runtime, txId++, "/MyRoot/DirA", R"(
                TableDescription {
                  Name: "Table2"
                  Columns { Name: "key"   Type: "Uint64" }
                  Columns { Name: "value0" Type: "Utf8" }
                  KeyColumnNames: ["key"]
                }
                IndexDescription {
                  Name: "UserDefinedIndexByValue0"
                }
            )", {NKikimrScheme::StatusInvalidParameter});

        //not uniq keys
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0", "value1", "value0"]
            }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});

        //too many keys in index table
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key1"   Type: "Uint64" }
              Columns { Name: "key2"   Type: "Uint64" }
              Columns { Name: "key3"   Type: "Uint64" }
              Columns { Name: "key4"   Type: "Uint64" }
              Columns { Name: "key5"   Type: "Uint64" }
              Columns { Name: "key6"   Type: "Uint64" }
              Columns { Name: "key7"   Type: "Uint64" }
              Columns { Name: "key8"   Type: "Uint64" }
              Columns { Name: "key9"   Type: "Uint64" }
              Columns { Name: "key10"   Type: "Uint64" }
              Columns { Name: "key11"   Type: "Uint64" }
              Columns { Name: "key12"   Type: "Uint64" }
              Columns { Name: "key13"   Type: "Uint64" }
              Columns { Name: "key14"   Type: "Uint64" }
              Columns { Name: "key15"   Type: "Uint64" }
              Columns { Name: "key16"   Type: "Uint64" }
              Columns { Name: "key17"   Type: "Uint64" }
              Columns { Name: "key18"   Type: "Uint64" }
              Columns { Name: "key19"   Type: "Uint64" }
              Columns { Name: "key20"   Type: "Uint64" }

              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10",
                               "key11", "key12", "key13", "key14", "key15", "key16", "key17", "key18", "key19", "key20"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
        )", {TEvSchemeShard::EStatus::StatusSchemeError});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Float" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0", "value0"]
            }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Uint64" }
              Columns { Name: "value1" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0CoveringValue1"
              KeyColumnNames: ["value0"]
              DataColumnNames: ["value0"]
            }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Uint64" }
              Columns { Name: "value1" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0CoveringValue1"
              KeyColumnNames: ["value0"]
              DataColumnNames: ["value0", "value1"]
            }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Uint64" }
              Columns { Name: "value1" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0CoveringValue1"
              KeyColumnNames: ["value0"]
              DataColumnNames: ["key"]
            }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Uint64" }
              Columns { Name: "value1" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0CoveringValue1"
              KeyColumnNames: ["value0"]
              DataColumnNames: ["key", "value1"]
            }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Uint64" }
              Columns { Name: "value1" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0CoveringValue1"
              KeyColumnNames: ["value0"]
              DataColumnNames: ["blabla"]
            }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA/Table1", R"(
              Name: "inside_table"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )", {TEvSchemeShard::EStatus::StatusPathIsNotDirectory});

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0", R"(
              Name: "inside_index"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )", {TEvSchemeShard::EStatus::StatusNameConflict});

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0/indexImplTable", R"(
              Name: "inside_impl_table"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )", {TEvSchemeShard::EStatus::StatusNameConflict});

        TestAlterTable(runtime, ++txId, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0", R"(
              Name: "indexImplTable"
              Columns { Name: "add_1"  Type: "Uint32"}
              Columns { Name: "add_2"  Type: "Uint64"}
        )", {TEvSchemeShard::EStatus::StatusNameConflict});

        TestAlterTable(runtime, ++txId, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0", R"(
              Name: "indexImplTable"
              DropColumns { Name: "key"  Type: "Uint64"}
        )", {TEvSchemeShard::EStatus::StatusNameConflict});

        TestCopyTable(runtime, ++txId, "/MyRoot/DirA", "copy_is_ok", "/MyRoot/DirA/Table1");
        env.TestWaitNotification(runtime, txId);

        TestCopyTable(runtime, ++txId, "/MyRoot/DirA", "copy", "/MyRoot/DirA/Table1/UserDefinedIndexByValue0", TEvSchemeShard::EStatus::StatusNameConflict);
        TestCopyTable(runtime, ++txId, "/MyRoot/DirA", "copy", "/MyRoot/DirA/Table1/UserDefinedIndexByValue0/indexImplTable", TEvSchemeShard::EStatus::StatusNameConflict);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "WithFollowerGroup"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Uint64" }
              KeyColumnNames: ["key"]
              PartitionConfig {
               FollowerGroups {
                 FollowerCount: 1
               }
              }
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "WithFollowerCount"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Uint64" }
              KeyColumnNames: ["key"]
              PartitionConfig {
               FollowerCount: 1
              }
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "WithNoFollowers"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Uint64" }
              KeyColumnNames: ["key"]
              PartitionConfig {
               FollowerGroups {
               }
              }
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "WithNoFollowers"
              PartitionConfig {
                CrossDataCenterFollowerCount: 1
              }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "WithNoFollowers"
              PartitionConfig {
                FollowerCount: 1
              }
        )", {TEvSchemeShard::EStatus::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot/DirA", R"(
              Name: "WithNoFollowers"
              PartitionConfig {
                FollowerGroups {
                  FollowerCount: 1
                }
              }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDropTable(runtime, ++txId, "/MyRoot/DirA", "WithFollowerGroup");
        TestDropTable(runtime, ++txId, "/MyRoot/DirA", "WithFollowerCount");
        TestDropTable(runtime, ++txId, "/MyRoot/DirA", "WithNoFollowers");
        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDropTable(runtime, ++txId, "/", "Table1", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});
        TestDropTable(runtime, ++txId, "", "Table1", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});
        TestDropTable(runtime, ++txId, "", "", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});
        TestDropTable(runtime, ++txId, "/MyRoot/DirA", "", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});
        TestDropTable(runtime, ++txId, "/MyRoot/DirA/Table", "", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});

        TestDropTable(runtime, ++txId, "/MyRoot/not_exist", "Table1", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});
        TestDropTable(runtime, ++txId, "/not_exist/DirA", "Table1", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});
        TestDropTable(runtime, ++txId, "/MyRoot/DirA", "not_exist", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});

        TestDropTable(runtime, ++txId, "/MyRoot/DirA/Table_not_exist", "UserDefinedIndexByValue0", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});
        TestDropTable(runtime, ++txId, "/MyRoot/DirA/Table1", "UserDefinedIndexByValue0_not_exist", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});
        TestDropTable(runtime, ++txId, "/MyRoot/DirA/Table1", "UserDefinedIndexByValue0", {TEvSchemeShard::EStatus::StatusNameConflict});
        TestDropTable(runtime, ++txId, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0", "indexImplTable_not_exist", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});
        TestDropTable(runtime, ++txId, "/MyRoot/DirA/Table1/UserDefinedIndexByValue0", "indexImplTable", {TEvSchemeShard::EStatus::StatusNameConflict});

        AsyncDropTable(runtime, ++txId, "/MyRoot/DirA", "Table1");
        AsyncDropTable(runtime, ++txId, "/MyRoot/DirA", "copy_is_ok");
        AsyncDropTable(runtime, ++txId, "/MyRoot/DirA", "Table1");

        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);

        {
            auto ev = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
            UNIT_ASSERT(ev);
            const auto& record = ev->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusMultipleModifications);
            UNIT_ASSERT_VALUES_EQUAL(record.GetPathDropTxId(), txId - 2);
        }

        env.TestWaitNotification(runtime, {txId-1, txId-2});

        TestDropTable(runtime, ++txId, "/MyRoot/DirA", "Table1", {TEvSchemeShard::EStatus::StatusPathDoesNotExist});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 20));
    }

    Y_UNIT_TEST(CreateIndexedTableAndForceDrop) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue1"
              KeyColumnNames: ["value1"]
            }
        )");
        env.TestWaitNotification(runtime, {txId, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::ChildrenCount(1)});

        auto dirAVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                              {NLs::Finished,
                                               NLs::PathVersionEqual(5),
                                               NLs::ChildrenCount(1),
                                               NLs::ShardsInsideDomain(3)});

        TestForceDropUnsafe(runtime, ++txId, dirAVersion.PathId.LocalPathId);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(7),
                            NLs::ChildrenCount(0)});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 5));
    }

    Y_UNIT_TEST(CreateIndexedTableAndForceDropSimultaneously) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        auto dirAVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                              {NLs::Finished,
                                               NLs::PathVersionEqual(3),
                                               NLs::ChildrenCount(0)});

        AsyncCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue1"
              KeyColumnNames: ["value1"]
            }
        )");
        AsyncForceDropUnsafe(runtime, ++txId, dirAVersion.PathId.LocalPathId);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished,
                            NLs::ChildrenCount(0)});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 5));
    }

    Y_UNIT_TEST(DropIndexedTableAndForceDropSimultaneously) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot/DirA", R"(
            TableDescription {
              Name: "Table1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value0" Type: "Utf8" }
              Columns { Name: "value1" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue0"
              KeyColumnNames: ["value0"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue1"
              KeyColumnNames: ["value1"]
            }
        )");
        env.TestWaitNotification(runtime, {txId, txId-1});


        auto dirAVersion = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                              {NLs::Finished,
                                               NLs::PathVersionEqual(5),
                                               NLs::ChildrenCount(1)});

        AsyncDropTable(runtime, ++txId, "/MyRoot/DirA", "Table1");
        AsyncForceDropUnsafe(runtime, ++txId, dirAVersion.PathId.LocalPathId);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished,
                            NLs::ChildrenCount(0)});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 5));
    }

    Y_UNIT_TEST(IgnoreUserColumnIds) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        TestCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"TableIgnoreIds\""
                            "Columns { Name: \"key\"       Type: \"Uint32\" Id: 42}"
                            "Columns { Name: \"col3\"       Type: \"Uint32\" Id: 100500}"
                            "Columns { Name: \"col1\"       Type: \"Uint32\" Id: 100}"
                            "KeyColumnNames: [\"key\"]"
                            "KeyColumnIds: 100500");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/TableIgnoreIds"),
                           {NLs::Finished,
                            NLs::IsTable,
                            [](const NKikimrScheme::TEvDescribeSchemeResult& record) {
                                Cerr << record.ShortDebugString();
                                THashMap<TString, ui32> expectedIds = {{"key", 1}, {"col3", 2}, {"col1", 3}};
                                auto& table = record.GetPathDescription().GetTable();
                                UNIT_ASSERT_VALUES_EQUAL(3, table.ColumnsSize());
                                for (auto& col : table.GetColumns()) {
                                    UNIT_ASSERT_VALUES_EQUAL(col.GetId(), expectedIds[col.GetName()]);
                                }
                                UNIT_ASSERT_VALUES_EQUAL(1, table.KeyColumnIdsSize());
                                UNIT_ASSERT_VALUES_EQUAL(1, table.GetKeyColumnIds(0));
                            }});
    }

#if 0 // KIKIMR-1452
    Y_UNIT_TEST(CreateSameTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusAlreadyExists});

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["value"]
        )", {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "diff"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["diff"]
        )", {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            PartitionConfig {ChannelProfileId: 0}
        )", {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 10
        )", {NKikimrScheme::StatusSchemeError});
    }
#endif

    Y_UNIT_TEST(CreateTableWithUniformPartitioning) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"PartitionedTable1\""
                            "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                            "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                            "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                            "UniformPartitionsCount: 10"
                        );
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PartitionedTable1"),
                           {NLs::IsTable,
                            NLs::ShardsInsideDomain(10)});

        TestCreateTable(runtime, ++txId, "/MyRoot",
                        R"(Name: "PartitionedTable2"
                            Columns { Name: "key1"       Type: "Uint64"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key1"]
                            UniformPartitionsCount: 10
                            PartitionConfig {
                                TxReadSizeLimit: 1000
                            })");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PartitionedTable2"),
                           {NLs::IsTable,
                            NLs::ShardsInsideDomain(20)});
    }

    Y_UNIT_TEST(CreateTableWithSplitBoundaries) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;
        ++txId;
        TestCreateTable(runtime, txId, "/MyRoot", R"(
                        Name: "PartitionedTable1"
                            Columns { Name: "key1"       Type: "Uint32"}
                            Columns { Name: "key2"       Type: "Utf8"}
                            Columns { Name: "key3"       Type: "Uint64"}
                            Columns { Name: "key4"       Type: "Int32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key1", "key2", "key3", "key4"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                                Tuple { }                  # NULL
                                Tuple { Optional { Uint64 : 100500 } }
                                Tuple { Optional { Int32 : -100500 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                                Tuple { Optional { Text : "Tinky Winky" } }
                                Tuple { Optional { Uint64 : 200 } }
                            }}
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot//PartitionedTable1", true, true),
                           {NLs::IsTable,
                            NLs::CheckBoundaries});
    }

    Y_UNIT_TEST(CreateTableWithConfig) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"___(
                        Name: "Table1"
                        Columns { Name: "key1"       Type: "Uint32"}
                        Columns { Name: "Value"      Type: "Utf8"}
                        KeyColumnNames: ["key1"]
                        UniformPartitionsCount: 2
                        PartitionConfig {
                            CompactionPolicy {
                                InMemSizeToSnapshot: 1000
                                InMemStepsToSnapshot : 10
                                InMemCompactionBrokerQueue: 0
                                Generation {
                                    GenerationId: 0
                                    SizeToCompact: 10000
                                    CountToCompact: 3
                                    ForceCountToCompact: 5
                                    ForceSizeToCompact: 20000
                                    CompactionBrokerQueue: 1
                                    KeepInCache: true
                                }
                            }
                        })___");
        env.TestWaitNotification(runtime, txId);

        auto t1 = DescribePath(runtime, "/MyRoot/Table1");

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto t2 = DescribePath(runtime, "/MyRoot/Table1");

        UNIT_ASSERT_VALUES_EQUAL(t1.DebugString(), t2.DebugString());
    }

    Y_UNIT_TEST(CreateTableWithNamedConfig) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"___(
                        Name: "Table2"
                        Columns { Name: "key1"       Type: "Uint32"}
                        Columns { Name: "Value"      Type: "Utf8"}
                        KeyColumnNames: ["key1"]
                        UniformPartitionsCount: 2
                        PartitionConfig {
                            NamedCompactionPolicy : "UserTableDefault"
                        })___");
        env.TestWaitNotification(runtime, txId);

        auto t1 = DescribePath(runtime, "/MyRoot/Table2");

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto t2 = DescribePath(runtime, "/MyRoot/Table2");

        UNIT_ASSERT_VALUES_EQUAL(t1.DebugString(), t2.DebugString());
    }

    Y_UNIT_TEST(CreateTableWithUnknownNamedConfig) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"___(
                        Name: "Table2"
                        Columns { Name: "key1"       Type: "Uint32"}
                        Columns { Name: "Value"      Type: "Utf8"}
                        KeyColumnNames: ["key1"]
                        UniformPartitionsCount: 2
                        PartitionConfig {
                            NamedCompactionPolicy : "Default"
                        })___",
                        {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(CreateAlterTableWithCodec) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"_(
                        Name: "Table1"
                        Columns { Name: "key1"       Type: "Uint32"}
                        Columns { Name: "Value"      Type: "Utf8"}
                        KeyColumnNames: ["key1"]
                        UniformPartitionsCount: 2
                        PartitionConfig {
                            NamedCompactionPolicy : "UserTableDefault"
                            ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecLZ4
                                ColumnCache: ColumnCacheNone
                            }
                        })_");
        env.TestWaitNotification(runtime, txId);

        auto t1 = DescribePath(runtime, "/MyRoot/Table1");

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto t2 = DescribePath(runtime, "/MyRoot/Table1");

        UNIT_ASSERT_VALUES_EQUAL(t1.DebugString(), t2.DebugString());

        TestAlterTable(runtime, ++txId, "/MyRoot", R"_(
                        Name: "Table1"
                        PartitionConfig {
                            ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecZSTD
                                ColumnCache: ColumnCacheNone
                            }
                        })_", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"_(
                        Name: "Table1"
                        PartitionConfig {
                            ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                            }
                        })_");
        env.TestWaitNotification(runtime, txId);

        auto t3 = DescribePath(runtime, "/MyRoot/Table1");
        UNIT_ASSERT(t2.DebugString() != t3.DebugString());

        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto t4 = DescribePath(runtime, "/MyRoot/Table1");
        UNIT_ASSERT_VALUES_EQUAL(t3.DebugString(), t4.DebugString());

        TestCreateTable(runtime, ++txId, "/MyRoot", R"_(
                        Name: "Table2"
                        Columns { Name: "key1"       Type: "Uint32"}
                        Columns { Name: "Value"      Type: "Utf8"}
                        KeyColumnNames: ["key1"]
                        UniformPartitionsCount: 2
                        PartitionConfig {
                            NamedCompactionPolicy : "UserTableDefault"
                            ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecZSTD
                                ColumnCache: ColumnCacheNone
                            }
                        })_", {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(DependentOps) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        ++txId;
        AsyncMkDir(runtime, txId, "/MyRoot", "DirA");
        ++txId;
        AsyncMkDir(runtime, txId, "/MyRoot/DirA", "SubDirA");
        ++txId;
        AsyncMkDir(runtime, txId, "/MyRoot/DirA/SubDirA", "AAA");
        ++txId;
        AsyncMkDir(runtime, txId, "/MyRoot", "DirB");
        ++txId;
        AsyncMkDir(runtime, txId, "/MyRoot/DirA/SubDirA/AAA", "aaa");

        env.TestWaitNotification(runtime, {txId, txId-1, txId-2, txId-3, txId-4});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/"),
                           {NLs::Finished,
                            NLs::ChildrenCount(2)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3),
                            NLs::NoChildren});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::ChildrenCount(1)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::ChildrenCount(1)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA/AAA"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(5),
                            NLs::ChildrenCount(1)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA/AAA/aaa"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3),
                            NLs::NoChildren});
    }

    Y_UNIT_TEST(ParallelCreateTable) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table1\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]"
                        );
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table2\""
                            "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                            "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\", \"key1\", \"key2\"]"
                        );
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDescribe(runtime, "/MyRoot/DirA/Table1");
        TestDescribe(runtime, "/MyRoot/DirA/Table2");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathVersionEqual(7)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table1"),
                           {NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table2"),
                           {NLs::PathVersionEqual(3)});
    }

    Y_UNIT_TEST(ParallelCreateSameTable) { //+
        using ESts = NKikimrScheme::EStatus;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TString tableConfig = "Name: \"NilNoviSubLuna\""
            "Columns { Name: \"key\"        Type: \"Uint64\"}"
            "Columns { Name: \"value\"      Type: \"Uint64\"}"
            "KeyColumnNames: [\"key\"]"
            "UniformPartitionsCount: 16";

        AsyncCreateTable(runtime, ++txId, "/MyRoot", tableConfig);
        AsyncCreateTable(runtime, ++txId, "/MyRoot", tableConfig);
        AsyncCreateTable(runtime, ++txId, "/MyRoot", tableConfig);

        ui64 sts[3];
        sts[0] = TestModificationResults(runtime, txId-2, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[1] = TestModificationResults(runtime, txId-1, {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});
        sts[2] = TestModificationResults(runtime, txId,   {ESts::StatusAccepted, ESts::StatusMultipleModifications, ESts::StatusAlreadyExists});

        for (ui32 i=0; i<3; ++i) {
            if (sts[i] == ESts::StatusAlreadyExists) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                                   {NLs::Finished,
                                    NLs::IsTable});
            }

            if (sts[i] == ESts::StatusMultipleModifications) {
                TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                                   {NLs::NotFinished,
                                    NLs::IsTable});
            }
        }

        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/NilNoviSubLuna"),
                           {NLs::Finished,
                            NLs::IsTable,
                            NLs::PathVersionEqual(3)});

        TestCreateTable(runtime, ++txId, "/MyRoot", tableConfig, {ESts::StatusAlreadyExists});
    }

    Y_UNIT_TEST(CopyTable) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\""
                            "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                            "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                            "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                            "UniformPartitionsCount: 2"
                        );
        env.TestWaitNotification(runtime, txId);

        TestCopyTable(runtime, ++txId, "/MyRoot", "NewTable", "/MyRoot/Table");
        env.TestWaitNotification(runtime, txId);

        TestDescribe(runtime, "/MyRoot/NewTable");

        // Try to Copy over existing table
        TestCopyTable(runtime, ++txId, "/MyRoot", "Table", "/MyRoot/NewTable", NKikimrScheme::StatusAlreadyExists);

        // Try to Copy over existing dir
        AsyncMkDir(runtime, ++txId, "/MyRoot", "Dir");
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "Dir", "/MyRoot/Table");
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusNameConflict);
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished,
                            NLs::IsTable,
                            NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable"),
                           {NLs::Finished,
                            NLs::IsTable,
                            NLs::PathVersionEqual(3)});
    }

    Y_UNIT_TEST(CopyTableTwiceSimultaneously) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\""
                            "Columns { Name: \"key\"        Type: \"Uint32\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"key\"]"
                            "UniformPartitionsCount: 2"
                        );
        env.TestWaitNotification(runtime, txId);

        // Write some data to the shards in order to prevent their deletion right after merge
        auto fnWriteRow = [&] (ui64 tabletId, ui32 key) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key '( '('key (Uint32 '%u)) ) )
                    (let value '('('Value (Utf8 'aaaaaaaa)) ) )
                    (return (AsList (UpdateRow '__user__Table key value) ))
                )
            )", key);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        };
        fnWriteRow(TTestTxConfig::FakeHiveTablets, 0);
        fnWriteRow(TTestTxConfig::FakeHiveTablets+1, 0x80000000u);

        AsyncCopyTable(runtime, ++txId, "/MyRoot", "NewTable", "/MyRoot/Table");
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "NewTable2", "/MyRoot/NewTable");
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable"),
                           {NLs::Finished,
                            NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable2"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished,
                            NLs::ChildrenCount(2)});
    }

    Y_UNIT_TEST(CopyTableAndConcurrentChanges) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestCreateTable(runtime, ++txId, "/MyRoot", //124
                        "Name: \"Table\""
                            "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                            "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                            "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                            "UniformPartitionsCount: 2"
                        );
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathVersionEqual(3)});

        // Copy & Drop
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "Copy1", "/MyRoot/Table"); //125
        AsyncDropTable(runtime, ++txId, "/MyRoot", "Table"); //126
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy1"),
                           {NLs::PathVersionEqual(3)});

        // Copy & Alter
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "Copy2", "/MyRoot/Table"); //127
        AsyncAlterTable(runtime, ++txId, "/MyRoot", //128
                "Name: \"Table\""
                    "Columns { Name: \"add_1\"  Type: \"Uint32\"}"
                    "Columns { Name: \"add_2\"  Type: \"Uint64\"}"
                );
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy2"),
                           {NLs::PathVersionEqual(3)});
        // Alter & Copy
        AsyncAlterTable(runtime, ++txId, "/MyRoot", //129
                "Name: \"Table\""
                    "Columns { Name: \"add_1\"  Type: \"Uint32\"}"
                    "Columns { Name: \"add_2\"  Type: \"Uint64\"}"
                );
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "Copy3", "/MyRoot/Table"); //130
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathVersionEqual(4)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy3"),
                           {NLs::PathNotExist});

        // Copy & Copy
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "Copy4", "/MyRoot/Table"); //131
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "Copy5", "/MyRoot/Table"); //132
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathVersionEqual(4)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy4"),
                           {NLs::PathVersionEqual(3)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy5"),
                           {NLs::PathNotExist});

        // Drop & Copy
        AsyncDropTable(runtime, ++txId, "/MyRoot", "Table"); //133
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "Copy6", "/MyRoot/Table"); //134
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy6"),
                           {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CopyTableAndConcurrentSplit) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            UniformPartitionsCount: 2)");
        env.TestWaitNotification(runtime, txId);

        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409547
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint32: 3000000000 } }
                                    }
                                })");
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "NewTable", "/MyRoot/Table");
        // New split must be rejected while CopyTable is in progress
        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409546
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint32: 1000000000 } }
                                    }
                                })");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-1, txId-2});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable", true),
                           {NLs::PartitionCount(3),
                            NLs::PathVersionEqual(4)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(3),
                            NLs::PathVersionEqual(4)});

        // Delete all tables and wait for everything to be cleaned up
        AsyncDropTable(runtime, ++txId, "/MyRoot", "Table");
        AsyncDropTable(runtime, ++txId, "/MyRoot", "NewTable");
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId-1, txId});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+10));
    }

    Y_UNIT_TEST(CopyTableAndConcurrentMerge) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            UniformPartitionsCount: 3
                            PartitionConfig {
                                PartitioningPolicy {
                                    MinPartitionsCount: 0
                                }
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table",
                            R"(
                                SourceTabletId: 72075186233409547
                                SourceTabletId: 72075186233409548
                            )");
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "NewTable", "/MyRoot/Table");
        // New split must be rejected while CopyTable is in progress
        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table",
                            R"(
                                SourceTabletId: 72075186233409549
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint32: 300 } }
                                    }
                                }
                            )");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);

        env.TestWaitNotification(runtime, {txId-1, txId-2});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable", true),
                           {NLs::PartitionCount(2),
                            NLs::PathVersionEqual(4)});

        // Delete all tables and wait for everything to be cleaned up
        AsyncDropTable(runtime, ++txId, "/MyRoot", "Table");
        AsyncDropTable(runtime, ++txId, "/MyRoot", "NewTable");
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId-1, txId});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+10));
    }

    Y_UNIT_TEST(CopyTableAndConcurrentSplitMerge) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                                PartitioningPolicy {
                                    MinPartitionsCount: 0
                                }
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        // Merge and split so that overall partition count stays the same but shard boundaries change
        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table",
                            R"(
                                SourceTabletId: 72075186233409546
                                SourceTabletId: 72075186233409547
                            )");
        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table",
                            R"(
                                SourceTabletId: 72075186233409548
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint32: 4000 } }
                                    }
                                }
                            )");
        AsyncCopyTable(runtime, ++txId, "/MyRoot", "NewTable", "/MyRoot/Table"); //104
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        auto fnCheckSingleColumnKey = [](TString keyBuf, ui32 val) {
            TSerializedCellVec cells(keyBuf);
            UNIT_ASSERT_VALUES_EQUAL(cells.GetCells().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(*(const ui32*)cells.GetCells()[0].Data(), val);
        };

        TVector<ui32> expectedBoundaries = {200, 4000};

        TestDescribeResult(DescribePath(runtime, "/MyRoot/NewTable", true),
                           {NLs::PartitionCount(expectedBoundaries.size()+1),
                            NLs::PathVersionEqual(4),
                            [&] (auto describeRec) {
                                for (ui32 i = 0; i < expectedBoundaries.size(); ++i) {
                                    fnCheckSingleColumnKey(describeRec.GetPathDescription().GetTablePartitions(i).GetEndOfRangeKeyPrefix(), expectedBoundaries[i]);
                                }
                            }});

        // Delete all tables and wait for everything to be cleaned up
        AsyncDropTable(runtime, ++txId, "/MyRoot", "Table");
        AsyncDropTable(runtime, ++txId, "/MyRoot", "NewTable");
        env.TestWaitNotification(runtime, {txId-1, txId});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 10));
    }

    Y_UNIT_TEST(CopyTableWithAlterConfig) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                        )");
        env.TestWaitNotification(runtime, txId);

        // Cannot have multiple column families without StorageConfig
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                                 Name: "Table"
                                                 PartitionConfig {
                                                   ColumnFamilies {
                                                     Id: 1
                                                     ColumnCodec: ColumnCodecLZ4
                                                     ColumnCache: ColumnCacheNone
                                                   }
                                                 }
                                             )", {NKikimrScheme::StatusInvalidParameter});

        // Cannot have multiple changes for the same column family 0
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                                 Name: "Table"
                                                 PartitionConfig {
                                                   ColumnFamilies {
                                                     Id: 0
                                                     ColumnCodec: ColumnCodecLZ4
                                                     ColumnCache: ColumnCacheNone
                                                   }
                                                   ColumnFamilies {
                                                     Id: 0
                                                     ColumnCodec: ColumnCodecLZ4
                                                     ColumnCache: ColumnCacheNone
                                                   }
                                                 }
                                             )", {NKikimrScheme::StatusInvalidParameter});

        // Cannot switch from legacy table to StorageConfig
        TestAlterTable(runtime, ++txId, "/MyRoot",
                       R"(
                       Name: "Table"
                       PartitionConfig {
                         ColumnFamilies {
                           Id: 0
                           ColumnCodec: ColumnCodecLZ4
                           ColumnCache: ColumnCacheNone
                           StorageConfig {
                             SysLog {}
                             Log {}
                           }
                         }
                       })", {NKikimrScheme::StatusInvalidParameter});

        // Changing column family settings should be OK
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                                 Name: "Table"
                                                 PartitionConfig {
                                                   ColumnFamilies {
                                                     Id: 0
                                                     ColumnCodec: ColumnCodecLZ4
                                                     ColumnCache: ColumnCacheEver
                                                   }
                                                 }
                                             )");
        env.TestWaitNotification(runtime, txId);

        // Copying table with changed column family settings should be OK
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "CopyTable"
                            CopyFromTable: "/MyRoot/Table"
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecLZ4
                                ColumnCache: ColumnCacheOnce
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        TestAlterSubDomain(runtime, ++txId,  "/",
                           "StoragePools { "
                           "  Name: \"pool-1\" "
                           "  Kind: \"pool-kind-1\" "
                           "} "
                           "StoragePools { "
                           "  Name: \"pool-2\" "
                           "  Kind: \"pool-kind-2\" "
                           "} "
                           "StoragePools { "
                           "  Name: \"name_USER_0_kind_hdd-1\" "
                           "  Kind: \"hdd-1\" "
                           "} "
                           "StoragePools { "
                           "  Name: \"name_USER_0_kind_hdd-2\" "
                           "  Kind: \"hdd-2\" "
                           "} "
                           "Name: \"MyRoot\"");
        env.TestWaitNotification(runtime, txId);

        // Copy table while changing StorageConfig is OK
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "CopyTable2"
                            CopyFromTable: "/MyRoot/Table"
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecLZ4
                                ColumnCache: ColumnCacheOnce
                                StorageConfig {
                                  SysLog {}
                                  Log {}
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        // Changing codecs doesn't change StorageConfig, so it's OK
        TestAlterTable(runtime, ++txId, "/MyRoot",
                       R"(
                       Name: "CopyTable2"
                       PartitionConfig {
                         ColumnFamilies {
                           Id: 0
                           ColumnCodec: ColumnCodecLZ4
                           ColumnCache: ColumnCacheNone
                         }
                       })");
        env.TestWaitNotification(runtime, txId);

        // Adding column families to StorageConfig based tables is OK
        TestAlterTable(runtime, ++txId, "/MyRoot",
                       R"(
                       Name: "CopyTable2"
                       Columns { Name: "Value"  Family: 1 }
                       PartitionConfig {
                         ColumnFamilies {
                           Id: 1
                           ColumnCodec: ColumnCodecLZ4
                           ColumnCache: ColumnCacheNone
                         }
                       })");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "CopyTable3"
                            CopyFromTable: "/MyRoot/Table")");
        env.TestWaitNotification(runtime, txId);

        auto checker = [] (NKikimrSchemeOp::EColumnCache cacheType, size_t families = 1) {
            return [=] (const NKikimrSchemeOp::TTableDescription& tableDescription) {
                auto partConfig = tableDescription.GetPartitionConfig();
                Cdbg << "-----------" << Endl << partConfig.DebugString() << "\n~~~~~~\n" << Endl;

                UNIT_ASSERT_VALUES_EQUAL(tableDescription.ColumnsSize(), 2);
                UNIT_ASSERT_VALUES_EQUAL(partConfig.ColumnFamiliesSize(), families);

                const auto& otherFamily = partConfig.GetColumnFamilies(0);
                UNIT_ASSERT_VALUES_EQUAL(otherFamily.GetId(), 0);
                UNIT_ASSERT_EQUAL(otherFamily.GetColumnCache(), cacheType);

                UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(1).GetFamily(), families - 1);
            };
        };

        // /Root/Table
        for (ui64 tabletId : {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2}) {
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 2);
            checker(NKikimrSchemeOp::EColumnCache::ColumnCacheEver)(tableDescription);
        }

        // /Root/CopyTable
        for (ui64 tabletId : {TTestTxConfig::FakeHiveTablets+3, TTestTxConfig::FakeHiveTablets+4, TTestTxConfig::FakeHiveTablets+5}) {
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 3);
            checker(NKikimrSchemeOp::EColumnCache::ColumnCacheOnce)(tableDescription);
        }

        // /Root/CopyTable2
        for (ui64 tabletId : {TTestTxConfig::FakeHiveTablets+6, TTestTxConfig::FakeHiveTablets+7, TTestTxConfig::FakeHiveTablets+8}) {
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 4);
            checker(NKikimrSchemeOp::EColumnCache::ColumnCacheNone, 2)(tableDescription);
        }

        // /Root/CopyTable3
        for (ui64 tabletId : {TTestTxConfig::FakeHiveTablets+9, TTestTxConfig::FakeHiveTablets+10, TTestTxConfig::FakeHiveTablets+11}) {
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 5);
            checker(NKikimrSchemeOp::EColumnCache::ColumnCacheEver)(tableDescription);
        }

        auto descrChecker = [] (size_t families) {
            return [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                //Cerr << record.DebugString() << Endl;
                UNIT_ASSERT_EQUAL(record.GetStatus(), NKikimrScheme::StatusSuccess);
                UNIT_ASSERT_EQUAL(families, record.GetPathDescription().GetTable().GetPartitionConfig().ColumnFamiliesSize());
            };
        };

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {descrChecker(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/CopyTable", true),
                           {descrChecker(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/CopyTable2", true),
                           {descrChecker(2)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/CopyTable3", true),
                           {descrChecker(1)});
    }

    Y_UNIT_TEST(CopyTableOmitFollowers) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // create src table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "Value" Type: "Utf8"}
            KeyColumnNames: ["key"]
            PartitionConfig {
                FollowerCount: 1
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true), {
            NLs::Finished,
            NLs::IsTable,
            NLs::FollowerCount(1)
        });

        // simple copy table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            CopyFromTable: "/MyRoot/Table"
            OmitFollowers: true
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/CopyTable", true), {
            NLs::Finished,
            NLs::IsTable,
            NLs::FollowerCount(0),
            NLs::CrossDataCenterFollowerCount(0),
            NLs::FollowerGroups({NKikimrHive::TFollowerGroup()})
        });

        // consistent copy table
        TestConsistentCopyTables(runtime, ++txId, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/ConsistentCopyTable"
                OmitFollowers: true
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ConsistentCopyTable", true), {
            NLs::Finished,
            NLs::IsTable,
            NLs::FollowerCount(0),
            NLs::CrossDataCenterFollowerCount(0),
            NLs::FollowerGroups({NKikimrHive::TFollowerGroup()})
        });
    }

    Y_UNIT_TEST(CopyTableForBackup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        bool splitStarted = false;
        THashSet<ui64> deletedShardIdxs; // used to sanity check at the end of the test

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvHive::EvDeleteTabletReply:
                for (const ui64 shardIdx : ev->Get<TEvHive::TEvDeleteTabletReply>()->Record.GetShardLocalIdx()) {
                    deletedShardIdxs.insert(shardIdx);
                }
                return TTestActorRuntime::EEventAction::PROCESS;

            case TEvDataShard::EvGetTableStats:
                splitStarted = true;
                return TTestActorRuntime::EEventAction::DROP; // prevent splitting

            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        // these limits should have no effect on backup tables
        TSchemeLimits limits;
        limits.MaxPaths = 4;
        limits.MaxShards = 4;
        limits.MaxChildrenInDir = 3;
        SetSchemeshardSchemaLimits(runtime, limits);

        // create src table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
            PartitionConfig {
                PartitioningPolicy {
                    SizeToSplit: 100
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true), {
            NLs::IsBackupTable(false),
        });

        // simple copy table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            CopyFromTable: "/MyRoot/Table"
            IsBackup: true
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/CopyTable", true), {
            NLs::IsBackupTable(true),
        });

        // consistent copy table
        TestConsistentCopyTables(runtime, ++txId, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table"
                DstPath: "/MyRoot/ConsistentCopyTable"
                IsBackup: true
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/ConsistentCopyTable", true), {
            NLs::IsBackupTable(true),
        });

        // write some data...
        for (ui32 i = 0; i < 100; ++i) {
            const auto query = Sprintf(R"(
                (
                    (let key '('('key (Uint32 '%u)) ) )
                    (let value '('('value (Utf8 'foobar)) ) )
                    (return (AsList (UpdateRow '__user__Table key value) ))
                )
            )", i);
            NKikimrMiniKQL::TResult result;
            TString err;
            const auto status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, query, result, err);
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        }

        // ... and wait for the split to start
        if (!splitStarted) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&splitStarted](IEventHandle&) {
                return splitStarted;
            });
            runtime.DispatchEvents(opts);
        }

        // negative tests

        // shards limit
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 4
        )", {NKikimrScheme::StatusResourceExhausted});

        // ok
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir");
        env.TestWaitNotification(runtime, txId);

        // children limit
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table3"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});

        // ok
        TestCreateTable(runtime, ++txId, "/MyRoot/Dir", R"(
            Name: "Table3"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // paths limit
        TestCreateTable(runtime, ++txId, "/MyRoot/Dir", R"(
            Name: "Table4"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusResourceExhausted});

        // free quota
        TestDropTable(runtime, ++txId, "/MyRoot", "Table2");
        env.TestWaitNotification(runtime, txId);

        // ok
        TestCreateTable(runtime, ++txId, "/MyRoot/Dir", R"(
            Name: "Table4"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        // reset limits to default
        SetSchemeshardSchemaLimits(runtime, TSchemeLimits());

        // cannot create new table with 'IsBackup'
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "TableForBackup"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
            IsBackup: true
        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "TableForBackup"
              Columns { Name: "key" Type: "Uint32"}
              Columns { Name: "value" Type: "Utf8"}
              KeyColumnNames: ["key"]
              IsBackup: true
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue"
              KeyColumnNames: ["value"]
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        // cannot add 'IsBackup' property to existent table
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            IsBackup: true
        )", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            IsBackup: true
            DropColumns { Name: "value" }
        )", {NKikimrScheme::StatusInvalidParameter});

        // cannot remove 'IsBackup' property from existent table
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            IsBackup: false
        )", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            IsBackup: false
            DropColumns { Name: "value" }
        )", {NKikimrScheme::StatusInvalidParameter});

        // sanity check

        // drop all tables
        TVector<ui64> dropTxIds;
        for (const auto& table : {"Table", "CopyTable", "ConsistentCopyTable"}) {
            TestDropTable(runtime, dropTxIds.emplace_back(++txId), "/MyRoot", table);
        }
        for (const auto& table : {"Table3", "Table4"}) {
            TestDropTable(runtime, dropTxIds.emplace_back(++txId), "/MyRoot/Dir", table);
        }
        // Table2 has already been dropped
        env.TestWaitNotification(runtime, dropTxIds);

        if (deletedShardIdxs.size() != 6) { // 6 tables with one shard each
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&deletedShardIdxs](IEventHandle&) {
                return deletedShardIdxs.size() == 6;
            });
            runtime.DispatchEvents(opts);
        }

        // ok
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(ConsistentCopyTablesForBackup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TSchemeLimits limits;
        limits.MaxConsistentCopyTargets = 1; // should not affect
        SetSchemeshardSchemaLimits(runtime, limits);

        // create two tables
        for (int i = 1; i <= 2; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "Table%i"
                Columns { Name: "key" Type: "Uint32"}
                Columns { Name: "value" Type: "Utf8"}
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        // negative
        TestConsistentCopyTables(runtime, ++txId, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table1"
                DstPath: "/MyRoot/CopyTable1"
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table2"
                DstPath: "/MyRoot/CopyTable2"
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        // positive
        TestConsistentCopyTables(runtime, ++txId, "/", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table1"
                DstPath: "/MyRoot/CopyTable1"
                IsBackup: true
            }
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Table2"
                DstPath: "/MyRoot/CopyTable2"
                IsBackup: true
            }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateIndexedTableAfterBackup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TSchemeLimits limits;
        limits.MaxShards = 3; // for table + table + index
        SetSchemeshardSchemaLimits(runtime, limits);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
            PartitionConfig {
                PartitioningPolicy {
                    SizeToSplit: 100
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        for (int i = 1; i <= 2; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "CopyTable%i"
                CopyFromTable: "/MyRoot/Table"
                IsBackup: true
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table2"
              Columns { Name: "key" Type: "Uint32"}
              Columns { Name: "value" Type: "Utf8"}
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndexByValue"
              KeyColumnNames: ["value"]
            }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CopyLockedTableForBackup) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // create src table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        THolder<IEventHandle> delayed;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvSchemeShard::EvModifySchemeTransaction) {
                const auto& record = ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record;
                if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexBuild) {
                    delayed.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // build index
        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "Sync", {"value"});
        const auto buildIndexId = txId;

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) {
                return bool(delayed);
            });
            runtime.DispatchEvents(opts);
        }

        // table is locked at this point
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            CopyFromTable: "/MyRoot/Table"
        )", {NKikimrScheme::StatusMultipleModifications});

        // copy table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            CopyFromTable: "/MyRoot/Table"
            IsBackup: true
        )");
        env.TestWaitNotification(runtime, txId);

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(delayed.Release(), 0, true);
        env.TestWaitNotification(runtime, buildIndexId);
    }

    Y_UNIT_TEST(AlterTableAndConcurrentSplit) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            UniformPartitionsCount: 3
                            PartitionConfig {
                              PartitioningPolicy {
                                MinPartitionsCount: 0
                              }
                              CompactionPolicy {
                                InMemSizeToSnapshot: 4194304
                                InMemStepsToSnapshot: 300
                                InMemForceStepsToSnapshot: 500
                                InMemForceSizeToSnapshot: 16777216
                                InMemCompactionBrokerQueue: 0
                                ReadAheadHiThreshold: 67108864
                                ReadAheadLoThreshold: 16777216
                                MinDataPageSize: 7168
                                SnapBrokerQueue: 0
                                Generation {
                                  GenerationId: 0
                                  SizeToCompact: 0
                                  CountToCompact: 8
                                  ForceCountToCompact: 8
                                  ForceSizeToCompact: 134217728
                                  CompactionBrokerQueue: 1
                                  KeepInCache: true
                                }
                            }}
                        )");
        env.TestWaitNotification(runtime, txId);

        // Write some data to the shards in order to prevent their deletion right after merge
        auto fnWriteRow = [&] (ui64 tabletId, ui32 key) {
            TString writeQuery = Sprintf(R"(
                (
                    (let key '( '('key (Uint32 '%u)) ) )
                    (let value '('('Value (Utf8 'aaaaaaaa)) ) )
                    (return (AsList (UpdateRow '__user__Table key value) ))
                )
            )", key);
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
        };
        fnWriteRow(TTestTxConfig::FakeHiveTablets, 0);
        fnWriteRow(TTestTxConfig::FakeHiveTablets+1, 0x80000000u);

        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409546
                                SourceTabletId: 72075186233409547
                            )");
        AsyncAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                ExecutorCacheSize: 12121212
                                CompactionPolicy {
                                    InMemSizeToSnapshot: 4194304
                                    InMemStepsToSnapshot: 300
                                    InMemForceStepsToSnapshot: 500
                                    InMemForceSizeToSnapshot: 16777216
                                    InMemCompactionBrokerQueue: 0
                                    ReadAheadHiThreshold: 67108864
                                    ReadAheadLoThreshold: 16777216
                                    MinDataPageSize: 7168
                                    SnapBrokerQueue: 0
                                    Generation {
                                      GenerationId: 0
                                      SizeToCompact: 0
                                      CountToCompact: 8
                                      ForceCountToCompact: 8
                                      ForceSizeToCompact: 134217728
                                      CompactionBrokerQueue: 1
                                      KeepInCache: true
                                    }
                                    Generation {
                                      GenerationId: 1
                                      SizeToCompact: 41943040
                                      CountToCompact: 5
                                      ForceCountToCompact: 8
                                      ForceSizeToCompact: 536870912
                                      CompactionBrokerQueue: 2
                                      KeepInCache: false
                                    }
                                 }
                            }
                        )");
        // New split must be rejected while CopyTable is in progress
        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409548
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint32: 300 } }
                                    }
                                }
                            )");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::PartitionCount(2)});

        NTabletFlatScheme::TSchemeChanges scheme;
        TString errStr;

        // Check local scheme on datashards
        LocalSchemeTx(runtime, TTestTxConfig::FakeHiveTablets, "", true, scheme, errStr);
        UNIT_ASSERT_VALUES_EQUAL(errStr, "");
        UNIT_ASSERT_C(!ToString(scheme).Contains("ExecutorCacheSize: 12121212"), "Old shard must not participate in ALTER");

        LocalSchemeTx(runtime, TTestTxConfig::FakeHiveTablets+1, "", true, scheme, errStr);
        UNIT_ASSERT_VALUES_EQUAL(errStr, "");
        UNIT_ASSERT_C(!ToString(scheme).Contains("ExecutorCacheSize: 12121212"), "Old shard must not participate in ALTER");

        LocalSchemeTx(runtime, TTestTxConfig::FakeHiveTablets+2, "", true, scheme, errStr);
        UNIT_ASSERT_VALUES_EQUAL(errStr, "");
        UNIT_ASSERT_STRING_CONTAINS_C(ToString(scheme), "ExecutorCacheSize: 12121212", "New shard must participate in ALTER");
        {
            // Read user table schema from new shard;
            NKikimrMiniKQL::TResult result;
            TString err;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets+2, R"(
                                    (
                                        (let range '('('Tid (Uint64 '0) (Void))))
                                        (let select '('LocalTid 'Schema))
                                        (let options '('('ItemsLimit (Uint64 '1))) )
                                        (let result (SelectRange 'UserTables range select options))
                                        (return (AsList (SetResult 'TableInfo result) ))
                                    )
                )", result, err);
            UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(err, "");
//            Cerr << result << Endl;
            //  Value { Struct { Optional { Struct { List { Struct { Optional { Uint32: 1001 } } Struct { Optional { Bytes: ".." } } } } } } }
            ui32 localTid = result.GetValue().GetStruct(0).GetOptional().GetStruct(0).GetList(0).GetStruct(0).GetOptional().GetUint32();
            TString schemaStr = result.GetValue().GetStruct(0).GetOptional().GetStruct(0).GetList(0).GetStruct(1).GetOptional().GetBytes();
            UNIT_ASSERT_VALUES_EQUAL(localTid, 1001);
            UNIT_ASSERT(!schemaStr.empty());
            NKikimrSchemeOp::TTableDescription tableDescr;
            bool ok = tableDescr.ParseFromArray(schemaStr.data(), schemaStr.size());
            UNIT_ASSERT(ok);
//            Cerr << tableDescr << Endl;
            UNIT_ASSERT_VALUES_EQUAL(tableDescr.GetPartitionConfig().GetExecutorCacheSize(), 12121212);
            UNIT_ASSERT_VALUES_EQUAL(tableDescr.GetPartitionConfig().GetCompactionPolicy().GenerationSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(tableDescr.GetPartitionConfig().GetCompactionPolicy().GetGeneration(1).GetForceSizeToCompact(), 536870912);
        }

        LocalSchemeTx(runtime, TTestTxConfig::FakeHiveTablets+3, "", true, scheme, errStr);
        UNIT_ASSERT_VALUES_EQUAL(errStr, "");
        UNIT_ASSERT_STRING_CONTAINS_C(ToString(scheme), "ExecutorCacheSize: 12121212", "Non-splitted shard must participate in ALTER");

        // Drop the table and wait for everything to be cleaned up
        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+10));
    }

    Y_UNIT_TEST(DropTableAndConcurrentSplit) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            UniformPartitionsCount: 3
                            PartitionConfig {
                                PartitioningPolicy {
                                    MinPartitionsCount: 0
                                }
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409547
                                SourceTabletId: 72075186233409548
                            )");
        AsyncDropTable(runtime, ++txId, "/MyRoot", "Table");
        // New split must be rejected while DropTable is in progress
        AsyncSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409549
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint32: 300 } }
                                    }
                                }
                            )");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        // Wait for everything to be cleaned up
        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+10));
    }

    Y_UNIT_TEST(AlterTable) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TSet<TString> cols = {"key1", "key2", "value"};
        TSet<TString> keyCol = {"key1", "key2"};
        TSet<TString> dropCols;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key2"   Type: "Uint32"}
                    Columns { Name: "key1"   Type: "Uint64"}
                    Columns { Name: "value"  Type: "Utf8"}
                    KeyColumnNames: ["key1", "key2"]
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: add column" << Endl;
        cols.insert("add_1");
        cols.insert("add_2");
        cols.insert("add_50");
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "add_1"  Type: "Uint32"}
                    Columns { Name: "add_2"  Type: "Uint64"}
                    Columns { Name: "add_50" Type: "Utf8" Id: 100500}
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: add existed column" << Endl;
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" Columns { Name: "value"  Type: "Utf8"})",
                {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" Columns { Name: "add_1"  Type: "Uint64"})",
                {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: add wrong column" << Endl;
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" Columns { Name: "кокошник" Type: "Utf8"})",
                {NKikimrScheme::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" Columns { Name: "name with spaces" Type: "Uint64"})",
                {NKikimrScheme::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" Columns { Name: "\x10xyz" Type: "Uint64"})",
                {NKikimrScheme::StatusInvalidParameter});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: add column ignores id" << Endl;
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" Columns { Name: "add_100" Type: "Utf8" Id: 1 })");
        cols.insert("add_100");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: drop column" << Endl;
        cols.erase("value");
        cols.erase("add_2");
        dropCols.insert("value");
        dropCols.insert("add_2");
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" DropColumns { Name: "value" } DropColumns { Name: "add_2" })");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: drop column ignores id" << Endl;
        dropCols.insert("add_100");
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" DropColumns { Name: "add_100" Id: 500100 })");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: drop dropped column" << Endl;
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" DropColumns { Name: "add_2"})",
                {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        Cdbg << "AlterTable: drop key column" << Endl;
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" DropColumns { Name: "key1" })",
                {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        Cdbg << "AlterTable: drop without column name " << Endl;
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table" DropColumns { Id: 3 })",
                {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        Cdbg << "AlterTable: add + drop different column" << Endl;
        cols.insert("add_3");
        dropCols.insert("add_1");
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "add_3"  Type: "Uint32" }
                    DropColumns { Name: "add_1" }
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: add + drop same column (exist)" << Endl;
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "add_3"  Type: "Uint32"}
                    DropColumns { Name: "add_3" }
                )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        Cdbg << "AlterTable: add + drop same column (not exist)" << Endl;
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "add_4"  Type: "Uint32"}
                    DropColumns { Name: "add_4" }
                )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});
    }

    Y_UNIT_TEST(AlterTableDropColumnReCreateSplit) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key"    Type: "Uint32"}
                    Columns { Name: "col1"   Type: "Uint32"}
                    Columns { Name: "col2"   Type: "Uint32"}
                    KeyColumnNames: ["key"]
                )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    DropColumns { Name: "col1" }
                )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "col1"   Type: "Utf8"}
                )");
        env.TestWaitNotification(runtime, txId);

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409546
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint32: 3000000000 } }
                                    }
                                })");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AlterTableDropColumnSplitThenReCreate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key"    Type: "Uint32"}
                    Columns { Name: "col1"   Type: "Uint32"}
                    Columns { Name: "col2"   Type: "Uint32"}
                    KeyColumnNames: ["key"]
                )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    DropColumns { Name: "col1" }
                )");
        env.TestWaitNotification(runtime, txId);

        TestSplitTable(runtime, ++txId, "/MyRoot/Table", R"(
                                SourceTabletId: 72075186233409546
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint32: 3000000000 } }
                                    }
                                })");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "col1"   Type: "Utf8"}
                )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AlterTableKeyColumns) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TSet<TString> cols = {"key1", "value"};
        TSet<TString> keyCol = {"key1"};
        TSet<TString> dropCols;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key1" Type: "Uint64" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key1"]
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: add key column (errors)" << Endl;
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key2" Type: "Uint32" }
                    KeyColumnNames: ["key2", "key1"]
                )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key1" Type: "Uint64" }
                    KeyColumnNames: ["key1", "key1"]
                )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "some" Type: "Uint32" }
                    KeyColumnNames: ["key1", "value"]
                )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    KeyColumnNames: ["key1"]
                )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key2" Type: "Uint32" }
                    KeyColumnNames: ["key1", "key2"]
                    PartitionConfig { ChannelProfileId: 1 }
                )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        Cdbg << "AlterTable: add key column" << Endl;
        cols.insert("key2");
        keyCol.insert("key2");
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key2" Type: "Uint32" }
                    KeyColumnNames: ["key1", "key2"]
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        cols.insert("key3");
        cols.insert("value2");
        keyCol.insert("key3");
        dropCols.insert("value");
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key3"  Type: "Utf8"}
                    Columns { Name: "value2"  Type: "Utf8"}
                    DropColumns { Name: "value" }
                    KeyColumnNames: ["key1", "key2", "key3"]
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        cols.insert("value3");
        dropCols.insert("value");
        TestAlterTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "value3"  Type: "Utf8"}
                    KeyColumnNames: ["key1", "key2", "key3"]
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});
    }

    Y_UNIT_TEST(AlterTableById) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TSet<TString> cols = {"key1", "key2", "value"};
        TSet<TString> keyCol = {"key1", "key2"};
        TSet<TString> dropCols;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                R"(Name: "Table"
                    Columns { Name: "key2"   Type: "Uint32" }
                    Columns { Name: "key1"   Type: "Uint64" }
                    Columns { Name: "value"  Type: "Utf8" }
                    KeyColumnNames: ["key1", "key2"]
                )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});

        Cdbg << "AlterTable: add column" << Endl;
        cols.insert("add_1");
        cols.insert("add_2");
        TestAlterTable(runtime, ++txId, "not used",
                R"(Id_Deprecated: 2
                    Columns { Name: "add_1"  Type: "Uint32"}
                    Columns { Name: "add_2"  Type: "Uint64"})"
                );
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::CheckColumns("Table", cols, dropCols, keyCol)});
    }

    Y_UNIT_TEST(AlterTableConfig) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key2"   Type: "Uint32"}
                            Columns { Name: "key1"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key1", "key2"]
                            PartitionConfig {
                                TxReadSizeLimit: 100
                                ExecutorCacheSize: 42
                                ChannelProfileId: 1
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        ui64 datashardTabletId = TTestTxConfig::FakeHiveTablets;
        UNIT_ASSERT_VALUES_EQUAL(GetTxReadSizeLimit(runtime, datashardTabletId), 100);
        UNIT_ASSERT_VALUES_EQUAL(GetExecutorCacheSize(runtime, datashardTabletId), 42);

        { // ChannelProfileId
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, datashardTabletId, 2);
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetPartitionConfig().GetChannelProfileId(), 1);
        }

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                TxReadSizeLimit: 2000
                            }
                       )");
        env.TestWaitNotification(runtime, txId);
        UNIT_ASSERT_VALUES_EQUAL(GetTxReadSizeLimit(runtime, datashardTabletId), 2000);
        UNIT_ASSERT_VALUES_EQUAL(GetExecutorCacheSize(runtime, datashardTabletId), 42);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "add_2"  Type: "Uint64"}
                            PartitionConfig {
                                ExecutorCacheSize: 100500
                            }
                       )");
        env.TestWaitNotification(runtime, txId);
        UNIT_ASSERT_VALUES_EQUAL(GetTxReadSizeLimit(runtime, datashardTabletId), 2000);
        UNIT_ASSERT_VALUES_EQUAL(GetExecutorCacheSize(runtime, datashardTabletId), 100500);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            DropColumns { Name: "add_2" }
                            PartitionConfig {
                                TxReadSizeLimit: 1000
                            }
                       )");
        env.TestWaitNotification(runtime, txId);
        UNIT_ASSERT_VALUES_EQUAL(GetTxReadSizeLimit(runtime, datashardTabletId), 1000);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "add_1" Type: "Uint64" }
                    DropColumns { Name: "value" }
                    PartitionConfig {
                        PipelineConfig {
                            NumActiveTx: 8
                            EnableOutOfOrder: 1
                        }
                    }
                )");

        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "noPartConfig"  Type: "Uint64"}
            )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table"
                PartitionConfig {
                    CrossDataCenterFollowerCount: 1
                }
            )");

        env.TestWaitNotification(runtime, txId);

        { // ChannelProfileId
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, datashardTabletId, 2);
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetPartitionConfig().GetChannelProfileId(), 1);
        }

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table"
                PartitionConfig {
                    ChannelProfileId: 0
                }
            )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table"
                PartitionConfig {
                    DisableStatisticsCalculation: true
                }
            )");
        env.TestWaitNotification(runtime, txId);
        UNIT_ASSERT_VALUES_EQUAL(GetStatDisabled(runtime, datashardTabletId), 1);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table"
                PartitionConfig {
                    DisableStatisticsCalculation: false
                }
            )");
        env.TestWaitNotification(runtime, txId);
        UNIT_ASSERT_VALUES_EQUAL(GetStatDisabled(runtime, datashardTabletId), 0);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::IsTable,
                            NLs::PathVersionEqual(11)});
    }

    Y_UNIT_TEST(ConfigColumnFamily) {
        using NKikimrSchemeOp::EColumnCodec;
        using NKikimrSchemeOp::EColumnCache;
        using NKikimrSchemeOp::EColumnStorage;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;
        TVector<ui64> tabletIds = {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1};

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key1" Type: "Uint64" }
            Columns { Name: "c1" Type: "Uint32" }
            Columns { Name: "c2" Type: "Uint32" Family: 0 }
            Columns { Name: "c3" Type: "Uint32" Family: 0 }
            KeyColumnNames: ["key1"]
            UniformPartitionsCount: 2
            PartitionConfig {
                ColumnFamilies { Id: 0 Name: "default"
                    Storage: ColumnStorage1
                    ColumnCache: ColumnCacheNone
                }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        for (ui64 tabletId : tabletIds) {
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 2);
            auto partConfig = tableDescription.GetPartitionConfig();
            Cdbg << ToString(partConfig) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.ColumnsSize(), 4);
            UNIT_ASSERT_VALUES_EQUAL(partConfig.ColumnFamiliesSize(), 1);

            const auto& defaultFamily = partConfig.GetColumnFamilies(0);
            UNIT_ASSERT_VALUES_EQUAL(defaultFamily.GetId(), 0);
            UNIT_ASSERT_VALUES_EQUAL(defaultFamily.GetName(), "default");
            UNIT_ASSERT_EQUAL(defaultFamily.GetStorage(), EColumnStorage::ColumnStorage1);
            UNIT_ASSERT_EQUAL(defaultFamily.GetColumnCache(), EColumnCache::ColumnCacheNone);

            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(0).GetName(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(1).GetName(), "c1");
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(2).GetName(), "c2");
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(3).GetName(), "c3");

            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(0).GetFamily(), 0);
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(1).GetFamily(), 0);
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(2).GetFamily(), 0);
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(3).GetFamily(), 0);
        }

        // add family (fails for now)

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            PartitionConfig {
                ColumnFamilies { Id: 1 Name: "other" Storage: ColumnStorage1 ColumnCache: ColumnCacheEver }
            }
        )", {NKikimrScheme::StatusSchemeError, NKikimrScheme::StatusInvalidParameter});

        // change colFamily + change family for columns

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "c1" Family: 0 }
            PartitionConfig {
                ColumnFamilies { Id: 0 ColumnCache: ColumnCacheEver }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "c1" Family: 0 }
            PartitionConfig {
                ColumnFamilies { Id: 0 ColumnCache: ColumnCacheEver }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        for (ui64 tabletId : tabletIds) {
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 2);
            auto partConfig = tableDescription.GetPartitionConfig();
            //Cdbg << ToString(partConfig) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(tableDescription.ColumnsSize(), 4);
            UNIT_ASSERT_VALUES_EQUAL(partConfig.ColumnFamiliesSize(), 1);

            const auto& otherFamily = partConfig.GetColumnFamilies(0);
            UNIT_ASSERT_VALUES_EQUAL(otherFamily.GetId(), 0);
            UNIT_ASSERT_VALUES_EQUAL(otherFamily.GetName(), "default");
            UNIT_ASSERT_EQUAL(otherFamily.GetColumnCache(), EColumnCache::ColumnCacheEver);

            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(1).GetFamily(), 0);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::IsTable,
                            [] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                                Cerr << record.DebugString() << Endl;
                                UNIT_ASSERT_EQUAL(record.GetStatus(), NKikimrScheme::StatusSuccess);
                                UNIT_ASSERT_EQUAL(1, record.GetPathDescription().GetTable().GetPartitionConfig().ColumnFamiliesSize());
                            }});
    }

    Y_UNIT_TEST(MultipleColumnFamilies) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestAlterSubDomain(runtime, ++txId,  "/", R"(
                            StoragePools {
                              Name: "pool-1"
                              Kind: "pool-kind-1"
                            }
                            StoragePools {
                              Name: "pool-2"
                              Kind: "pool-kind-2"
                            }
                            StoragePools {
                                Name: "name_USER_0_kind_hdd-1"
                                Kind: "hdd-1"
                            }
                            StoragePools {
                                Name: "name_USER_0_kind_hdd-2"
                                Kind: "hdd-2"
                            }
                            Name: "MyRoot"
                            )");
        env.TestWaitNotification(runtime, txId);

        // Multiple column families with StorageConfig are forbidden
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table0"
                            Columns { Name: "key"    Type: "Uint32" }
                            Columns { Name: "Value"  Type: "Utf8"   Family: 1 }
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {}
                                  Log {}
                                }
                              }
                              ColumnFamilies {
                                Id: 1
                                ColumnCodec: ColumnCodecLZ4
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {}
                                  Log {}
                                }
                              }
                            })", {NKikimrScheme::StatusInvalidParameter});

        // Auto generation column families do not generate by ID
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table0"
                            Columns { Name: "key"    Type: "Uint32" }
                            Columns { Name: "Value"  Type: "Utf8"   Family: 1 }
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {}
                                  Log {}
                                }
                              }
                            })", {NKikimrScheme::StatusSchemeError});

        // Creating a table with family should be OK
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "key"    Type: "Uint32" FamilyName: "default" }
                            Columns { Name: "Value"  Type: "Utf8"   FamilyName: "alt" }
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {}
                                  Log {}
                                }
                              }
                              ColumnFamilies {
                                Name: "alt"
                                ColumnCodec: ColumnCodecLZ4
                                ColumnCache: ColumnCacheNone
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        // Specifying FamilyName should be enough to autogenerate the family
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table2"
                            Columns { Name: "key"    Type: "Uint32" }
                            Columns { Name: "Value"  Type: "Utf8"   FamilyName: "alt" }
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {}
                                  Log {}
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        // Create a table that has no alternative families initially
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table3"
                            Columns { Name: "key"    Type: "Uint32" }
                            Columns { Name: "Value"  Type: "Utf8"   }
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {}
                                  Log {}
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        // Create a table that has no alternative families initially
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table4"
                            Columns { Name: "key"    Type: "Uint32" }
                            Columns { Name: "Value"  Type: "Utf8"   }
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                Name: ""
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {}
                                  Log {}
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        auto schemaChecker = [] (size_t families = 2) {
            return [=] (const NKikimrSchemeOp::TTableDescription& tableDescription) {
                Cerr << "-----------\n" << tableDescription.DebugString() << "\n~~~~~~\n";

                const auto& partConfig = tableDescription.GetPartitionConfig();

                UNIT_ASSERT_VALUES_EQUAL(tableDescription.ColumnsSize(), 2);
                UNIT_ASSERT_VALUES_EQUAL(partConfig.ColumnFamiliesSize(), families);

                const auto& mainFamily = partConfig.GetColumnFamilies(0);
                UNIT_ASSERT_VALUES_EQUAL(mainFamily.GetId(), 0);
                UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(0).GetFamily(), 0);

                if (families > 1) {
                    const auto& altFamily = partConfig.GetColumnFamilies(1);
                    UNIT_ASSERT_VALUES_EQUAL(altFamily.GetId(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(altFamily.GetName(), "alt");
                    UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(1).GetFamily(), 1);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(1).GetFamily(), 0);
                }
            };
        };

        auto descrChecker = [schemaChecker] (size_t families = 2) {
            return [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                UNIT_ASSERT_EQUAL(record.GetStatus(), NKikimrScheme::StatusSuccess);
                schemaChecker(families)(record.GetPathDescription().GetTable());
            };
        };

        Cerr << "Checking Table1" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1", true),
                           {descrChecker()});

        Cerr << "Checking Table2" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table2", true),
                           {descrChecker()});

        Cerr << "Checking Table3" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table3", true),
                           {descrChecker(1)});

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        Cerr << "Checking Table1" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1", true),
                           {descrChecker()});

        Cerr << "Checking Table2" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table2", true),
                           {descrChecker()});

        Cerr << "Checking Table3" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table3", true),
                           {descrChecker(1)});

        Cerr << "Checking Table4" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table4", true),
                           {descrChecker(1)});

        // Copy table of table with families should succeed
        TestCopyTable(runtime, ++txId, "/MyRoot", "Table2Copy", "/MyRoot/Table2");
        env.TestWaitNotification(runtime, txId);

        // Auto generation column families do not generate by ID
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table3"
                            Columns { Name: "Value"  Family: 1 }
                            )", {NKikimrScheme::StatusInvalidParameter});

        // Auto generation column families do not generate by ID
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table3"
                            Columns { Name: "Value"  Family: 1 FamilyName: "alt" }
                            )", {NKikimrScheme::StatusInvalidParameter});

        // Altering a column should autogenerate the family
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table3"
                            Columns { Name: "Value"  FamilyName: "alt" }
                            )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table4"
                            Columns { Name: "Value"  Family: 0 FamilyName: "" }
                            )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table4"
                            Columns { Name: "Value"  Family: 0 FamilyName: "default" }
                            )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table4"
                            Columns { Name: "Value"  Family: 1 FamilyName: "" }
                            )", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table4"
                            Columns { Name: "Value"  Family: 1 FamilyName: "default" }
                            )", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table4"
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 1
                                Name: ""
                              }
                            })", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table4"
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 1
                                Name: "default"
                              }
                            })", {NKikimrScheme::StatusInvalidParameter});

        // Copy table of table with families should succeed
        TestCopyTable(runtime, ++txId, "/MyRoot", "Table3Copy", "/MyRoot/Table3");
        env.TestWaitNotification(runtime, txId);

        // Trying to specify a non-default column family for key columns should fail
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table3"
                            Columns { Name: "key"  FamilyName: "more" }
                            )", {NKikimrScheme::StatusInvalidParameter});

        Cerr << "Checking Table1" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1", true),
                           {descrChecker()});

        Cerr << "Checking Table2" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table2", true),
                           {descrChecker()});

        Cerr << "Checking Table3" << Endl;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table3", true),
                           {descrChecker()});

        // /MyRoot/Table1
        Cerr << "Checking tablets for Table1" << Endl;
        for (ui64 tabletId : {TTestTxConfig::FakeHiveTablets+0, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2}) {
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 2);
            schemaChecker()(tableDescription);
        }

        // /MyRoot/Table2
        Cerr << "Checking tablets for Table2" << Endl;
        for (ui64 tabletId : {TTestTxConfig::FakeHiveTablets+3, TTestTxConfig::FakeHiveTablets+4, TTestTxConfig::FakeHiveTablets+5}) {
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 3);
            schemaChecker()(tableDescription);
        }

        // /MyRoot/Table3
        Cerr << "Checking tablets for Table3" << Endl;
        for (ui64 tabletId : {TTestTxConfig::FakeHiveTablets+6, TTestTxConfig::FakeHiveTablets+7, TTestTxConfig::FakeHiveTablets+8}) {
            NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 4);
            schemaChecker()(tableDescription);
        }
    }

    Y_UNIT_TEST(DefaultColumnFamiliesWithNonCanonicName) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestAlterSubDomain(runtime, ++txId,  "/", R"(
                            StoragePools {
                              Name: "pool-1"
                              Kind: "pool-kind-1"
                            }
                            StoragePools {
                              Name: "pool-2"
                              Kind: "pool-kind-2"
                            }
                            StoragePools {
                                Name: "name_USER_0_kind_hdd-1"
                                Kind: "hdd-1"
                            }
                            StoragePools {
                                Name: "name_USER_0_kind_hdd-2"
                                Kind: "hdd-2"
                            }
                            StoragePools {
                                Name: "name_USER_0_kind_hdd-3"
                                Kind: "hdd-3"
                            }
                            Name: "MyRoot"
                            )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "key"    Type: "Uint32" FamilyName: "default" }
                            Columns { Name: "Value"  Type: "Utf8" }
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                Name: "ExtBlobsOnHDD"
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog { PreferredPoolKind: "hdd-1" }
                                  Log { PreferredPoolKind: "hdd-1" }
                                  Data { PreferredPoolKind: "hdd-1" }
                                }
                              }
                            })", {NKikimrScheme::StatusSchemeError});

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "key"    Type: "Uint32" }
                            Columns { Name: "Value"  Type: "Utf8" }
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                Name: "ExtBlobsOnHDD"
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog { PreferredPoolKind: "hdd-1" }
                                  Log { PreferredPoolKind: "hdd-1" }
                                  Data { PreferredPoolKind: "hdd-1" }
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        TestCopyTable(runtime, ++txId, "/MyRoot", "Table1Copy", "/MyRoot/Table1");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                Name: "default"
                              }
                            })", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            PartitionConfig {
                              ColumnFamilies {
                                Name: "ExtBlobsOnHDD"
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "key"  Family: 0 FamilyName: "default" }
                            )", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "Value"  Family: 0 FamilyName: "default" }
                            )", {NKikimrScheme::StatusInvalidParameter});

//        KIKIMR-10458
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "key"  Family: 0 FamilyName: "ExtBlobsOnHDD" }
                            )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "Value"  Family: 0 FamilyName: "ExtBlobsOnHDD" }
                            )");
        env.TestWaitNotification(runtime, txId);

        auto descrChecker = [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
            const NKikimrSchemeOp::TTableDescription& tableDescription = record.GetPathDescription().GetTable();
            Cerr << "-----------\n" << tableDescription.DebugString() << "\n~~~~~~\n";

            const auto& partConfig = tableDescription.GetPartitionConfig();

            UNIT_ASSERT_VALUES_EQUAL(tableDescription.ColumnsSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(partConfig.ColumnFamiliesSize(), 1);

            const auto& mainFamily = partConfig.GetColumnFamilies(0);
            UNIT_ASSERT_VALUES_EQUAL(mainFamily.GetId(), 0);
            UNIT_ASSERT_VALUES_EQUAL(mainFamily.GetName(), "ExtBlobsOnHDD");

            UNIT_ASSERT_VALUES_EQUAL(tableDescription.GetColumns(0).GetFamily(), 0);
        };

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1", true),
                           {NLs::ColumnFamiliesCount(1),
                            NLs::ColumnFamiliesHas(0, "ExtBlobsOnHDD"),
                            descrChecker});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1Copy", true),
                           {descrChecker,
                            NLs::ColumnFamiliesHas(0, "ExtBlobsOnHDD"),
                            NLs::ColumnFamiliesCount(1)});
    }


    Y_UNIT_TEST(MultipleColumnFamiliesWithStorage) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestAlterSubDomain(runtime, ++txId,  "/", R"(
                            StoragePools {
                              Name: "pool-1"
                              Kind: "pool-kind-1"
                            }
                            StoragePools {
                              Name: "pool-2"
                              Kind: "pool-kind-2"
                            }
                            StoragePools {
                                Name: "name_USER_0_kind_hdd-1"
                                Kind: "hdd-1"
                            }
                            StoragePools {
                                Name: "name_USER_0_kind_hdd-2"
                                Kind: "hdd-2"
                            }
                            StoragePools {
                                Name: "name_USER_0_kind_hdd-3"
                                Kind: "hdd-3"
                            }
                            Name: "MyRoot"
                            )");
        env.TestWaitNotification(runtime, txId);

        struct TColumnExpectation {
            ui32 FamilyId;
        };

        struct TFamilyExpectation {
            ui32 FamilyId;
            TString FamilyName;
            ui32 RoomId;
        };

        struct TRoomExpectation {
            ui32 RoomId;
            ui32 DataChannel;
        };

        struct TSchemaChecker {
            TString CheckName;
            TVector<TColumnExpectation> Columns;
            TVector<TFamilyExpectation> Families;
            TVector<TRoomExpectation> Rooms;

            void operator()(const NKikimrSchemeOp::TTableDescription& tableDescription) const {
                Cerr << "-----------\n" << tableDescription.DebugString() << "\n~~~~~~\n";

                const auto& partConfig = tableDescription.GetPartitionConfig();

                UNIT_ASSERT_VALUES_EQUAL(tableDescription.ColumnsSize(), Columns.size());
                UNIT_ASSERT_VALUES_EQUAL(partConfig.ColumnFamiliesSize(), Families.size());
                UNIT_ASSERT_VALUES_EQUAL(partConfig.StorageRoomsSize(), Rooms.size());

                for (size_t idx = 0; idx < Columns.size(); ++idx) {
                    const auto& protoColumn = tableDescription.GetColumns(idx);
                    const auto& expectedColumn = Columns[idx];
                    UNIT_ASSERT_VALUES_EQUAL_C(protoColumn.GetFamily(), expectedColumn.FamilyId,
                        CheckName << " while checking family of column " << idx);
                }

                for (size_t idx = 0; idx < Families.size(); ++idx) {
                    const auto& protoFamily = partConfig.GetColumnFamilies(idx);
                    const auto& expectedFamily = Families[idx];
                    UNIT_ASSERT_VALUES_EQUAL_C(protoFamily.GetId(), expectedFamily.FamilyId,
                        CheckName << " while checking id of family " << idx);
                    UNIT_ASSERT_VALUES_EQUAL_C(protoFamily.GetName(), expectedFamily.FamilyName,
                        CheckName << " while checking name of family " << idx);
                    UNIT_ASSERT_VALUES_EQUAL_C(protoFamily.GetRoom(), expectedFamily.RoomId,
                        CheckName << " while checking room of family " << idx);
                }

                for (size_t idx = 0; idx < Rooms.size(); ++idx) {
                    const auto& protoRoom = partConfig.GetStorageRooms(idx);
                    const auto& expectedRoom = Rooms[idx];
                    UNIT_ASSERT_VALUES_EQUAL_C(protoRoom.GetRoomId(), expectedRoom.RoomId,
                        CheckName << " while checking id of room " << idx);
                    ui32 syslogChannel = Max<ui32>();
                    ui32 logChannel = Max<ui32>();
                    ui32 dataChannel = Max<ui32>();
                    for (const auto& exp : protoRoom.GetExplanation()) {
                        switch (exp.GetPurpose()) {
                            case NKikimrStorageSettings::TChannelPurpose::SysLog:
                                syslogChannel = exp.GetChannel();
                                break;
                            case NKikimrStorageSettings::TChannelPurpose::Log:
                                logChannel = exp.GetChannel();
                                break;
                            case NKikimrStorageSettings::TChannelPurpose::Data:
                                dataChannel = exp.GetChannel();
                                break;
                            default:
                                break;
                        }
                    }
                    // Primary room must have syslog and log defined correctly
                    // Other rooms must not have any mention of syslog and log
                    ui32 syslogExpected = protoRoom.GetRoomId() == 0 ? 0 : Max<ui32>();
                    ui32 logExpected = protoRoom.GetRoomId() == 0 ? 1 : Max<ui32>();
                    UNIT_ASSERT_VALUES_EQUAL_C(syslogChannel, syslogExpected,
                        CheckName << " while checking syslog channel of room " << idx);
                    UNIT_ASSERT_VALUES_EQUAL_C(logChannel, logExpected,
                        CheckName << " while checking log channel of room " << idx);
                    UNIT_ASSERT_VALUES_EQUAL_C(dataChannel, expectedRoom.DataChannel,
                        CheckName << " while checking data channel of room " << idx);
                }
            }

            void operator()(const NKikimrScheme::TEvDescribeSchemeResult& record) const {
                UNIT_ASSERT_EQUAL(record.GetStatus(), NKikimrScheme::StatusSuccess);

                // Schemeshard must return data without any mention of rooms
                TSchemaChecker derived{ CheckName, Columns, Families, { } };
                for (auto& family : derived.Families) {
                    family.RoomId = 0;
                }

                derived(record.GetPathDescription().GetTable());
            }
        };

        auto checkSchema = [&] (const TSchemaChecker& checker) {
            Cerr << "Checking tablets for Table1" << Endl;
            for (ui64 tabletId : {TTestTxConfig::FakeHiveTablets+0, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2}) {
                NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 2);
                checker(tableDescription);
            }

            Cerr << "Checking schema for Table1" << Endl;
            TestLs(runtime, "/MyRoot/Table1", true, checker);
        };

        // Creating a table with alt family on a different data storage should be OK
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "key"    Type: "Uint32" FamilyName: "default" }
                            Columns { Name: "Value"  Type: "Utf8"   FamilyName: "alt" }
                            KeyColumnNames: ["key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            PartitionConfig {
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog { PreferredPoolKind: "hdd-1" }
                                  Log { PreferredPoolKind: "hdd-1" }
                                  Data { PreferredPoolKind: "hdd-1" }
                                }
                              }
                              ColumnFamilies {
                                Name: "alt"
                                ColumnCodec: ColumnCodecLZ4
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  Data { PreferredPoolKind: "hdd-2" }
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        checkSchema({
            "After initial create",
            { { 0 }, { 1 } },
            { { 0, "default", 0 }, { 1, "alt", 1 } },
            { { 0, 1 }, { 1, 2 } }
        });

        // Trying to move default column family to the same storage should add default name
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            PartitionConfig {
                              ColumnFamilies {
                                Name: "default"
                                StorageConfig {
                                  Data { PreferredPoolKind: "hdd-1" }
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        checkSchema({
            "After no-op move of default family",
            { { 0 }, { 1 } },
            { { 0, "default", 0 }, { 1, "alt", 1 } },
            { { 0, 1 }, { 1, 2 } }
        });

        // Trying to move alt column family to the same storage should succeed without changes
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            PartitionConfig {
                              ColumnFamilies {
                                Name: "alt"
                                StorageConfig {
                                  Data { PreferredPoolKind: "hdd-2" }
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        checkSchema({
            "After no-op move of alt family",
            { { 0 }, { 1 } },
            { { 0, "default", 0 }, { 1, "alt", 1 } },
            { { 0, 1 }, { 1, 2 } }
        });

        // Trying to move default column family to a different storage
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            PartitionConfig {
                              ColumnFamilies {
                                Name: "default"
                                StorageConfig {
                                  Data { PreferredPoolKind: "hdd-3" }
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        checkSchema({
            "After moving default family to hdd-3",
            { { 0 }, { 1 } },
            { { 0, "default", 0 }, { 1, "alt", 1 } },
            { { 0, 3 }, { 1, 2 } }
        });

        // Trying to move alt column family to the same different storage should combine rooms
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            PartitionConfig {
                              ColumnFamilies {
                                Name: "alt"
                                StorageConfig {
                                  Data { PreferredPoolKind: "hdd-3" }
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        checkSchema({
            "After moving alt family to hdd-3",
            { { 0 }, { 1 } },
            { { 0, "default", 0 }, { 1, "alt", 0 } },
            { { 0, 3 } }
        });

        // Trying to move alt column family to some older storage should reuse existing channel
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            PartitionConfig {
                              ColumnFamilies {
                                Name: "alt"
                                StorageConfig {
                                  Data { PreferredPoolKind: "hdd-1" }
                                }
                              }
                            })");
        env.TestWaitNotification(runtime, txId);

        checkSchema({
            "After moving alt family to hdd-1",
            { { 0 }, { 1 } },
            { { 0, "default", 0 }, { 1, "alt", 1 } },
            { { 0, 3 }, { 1, 1 } }
        });
    }

    NLs::TCheckFunc CheckCompactionPolicy(NKikimr::NLocalDb::TCompactionPolicyPtr expectedPolicy) {
        return [=] (const NKikimrScheme::TEvDescribeSchemeResult& describeRec) {
            NKikimr::NLocalDb::TCompactionPolicy realPolicy(describeRec.GetPathDescription().GetTable().GetPartitionConfig().GetCompactionPolicy());
            UNIT_ASSERT(realPolicy == *expectedPolicy);
        };
    }

    Y_UNIT_TEST(AlterTableCompactionPolicy) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const ui64 datashardTabletId = TTestTxConfig::FakeHiveTablets;
        NKikimr::NLocalDb::TCompactionPolicyPtr defaultUserTablePolicy = NKikimr::NLocalDb::CreateDefaultUserTablePolicy();
        NKikimr::NLocalDb::TCompactionPolicyPtr defaultSystemTablePolicy = NKikimr::NLocalDb::CreateDefaultTablePolicy();

        // Create table with 1-level compaction policy
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key2"   Type: "Uint32"}
                            Columns { Name: "key1"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key1", "key2"]
                            PartitionConfig {
                                NamedCompactionPolicy: "SystemTableDefault"
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        NKikimr::NLocalDb::TCompactionPolicyPtr policyInDatashard;
        policyInDatashard = GetCompactionPolicy(runtime, datashardTabletId, 1001);
        UNIT_ASSERT(*policyInDatashard == *defaultSystemTablePolicy);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {CheckCompactionPolicy(policyInDatashard),
                            NLs::PathVersionEqual(3)});

        // Invalid compaction policy name - should fail
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "add_2"  Type: "Uint64"}
                            PartitionConfig {
                                NamedCompactionPolicy: "Non-existing policy"
                            }
                       )", {NKikimrScheme::StatusInvalidParameter});
        policyInDatashard = GetCompactionPolicy(runtime, datashardTabletId, 1001);
        UNIT_ASSERT(*policyInDatashard == *defaultSystemTablePolicy);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {CheckCompactionPolicy(policyInDatashard),
                            NLs::PathVersionEqual(3)});

        // Valid policy with more levels - should succeed
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "add_2"  Type: "Uint64"}
                            PartitionConfig {
                                NamedCompactionPolicy: "UserTableDefault"
                            }
                       )");
        env.TestWaitNotification(runtime, txId);
        policyInDatashard = GetCompactionPolicy(runtime, datashardTabletId, 1001);
        UNIT_ASSERT(*policyInDatashard == *defaultUserTablePolicy);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {CheckCompactionPolicy(policyInDatashard),
                            NLs::PathVersionEqual(4)});

        // Try to switch back to fewer levels - should fail
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                NamedCompactionPolicy: "SystemTableDefault"
                            }
                       )", {NKikimrScheme::StatusInvalidParameter});
        policyInDatashard = GetCompactionPolicy(runtime, datashardTabletId, 1001);
        UNIT_ASSERT(*policyInDatashard == *defaultUserTablePolicy);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {CheckCompactionPolicy(policyInDatashard),
                            NLs::PathVersionEqual(4)});
    }

    Y_UNIT_TEST(AlterTableFollowers) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            PartitionConfig {
                                FollowerCount: 100
                            }
                        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            PartitionConfig {
                                FollowerCount: 1
                                CrossDataCenterFollowerCount: 1
                            }
                        )", {NKikimrScheme::StatusInvalidParameter});


        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            PartitionConfig {
                                FollowerCount: 1
                                FollowerGroups { }
                            }
                        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            PartitionConfig {
                                CrossDataCenterFollowerCount: 1
                                FollowerGroups { }
                            }
                        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            PartitionConfig {
                                FollowerCount: 1
                                CrossDataCenterFollowerCount: 1
                                FollowerGroups { }
                            }
                        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            PartitionConfig {
                                FollowerGroups { }
                                FollowerGroups { }
                            }
                        )", {NKikimrScheme::StatusInvalidParameter});


        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key"]
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(0),
                               NLs::CrossDataCenterFollowerCount(0),
                               NLs::AllowFollowerPromotion(true),
                               NLs::FollowerGroups({})
                            });

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            FollowerCount: 100
                            AllowFollowerPromotion: true
                        }
                    )", {NKikimrScheme::StatusInvalidParameter});
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            CrossDataCenterFollowerCount: 100
                            AllowFollowerPromotion: true
                        }
                    )", {NKikimrScheme::StatusInvalidParameter});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                FollowerCount: 2
                                AllowFollowerPromotion: true
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(2),
                               NLs::CrossDataCenterFollowerCount(0),
                               NLs::AllowFollowerPromotion(true),
                               NLs::FollowerGroups({})
                           });

        //////////
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            DropColumns { Name: "value" }
                            PartitionConfig {
                                FollowerCount: 1
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(1),
                               NLs::CrossDataCenterFollowerCount(0),
                               NLs::AllowFollowerPromotion(true),
                               NLs::FollowerGroups({})
                           });

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(1)});


        //////////
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "value" Type: "Utf8" }
                            PartitionConfig {
                                AllowFollowerPromotion: false
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(1),
                               NLs::CrossDataCenterFollowerCount(0),
                               NLs::AllowFollowerPromotion(false),
                               NLs::FollowerGroups({})
                           });

        //////////
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                AllowFollowerPromotion: true
                                ExecutorCacheSize: 100500
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(1),
                               NLs::CrossDataCenterFollowerCount(0),
                               NLs::AllowFollowerPromotion(true),
                               NLs::FollowerGroups({})
                           });

        //////////
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                FollowerCount: 0
                                AllowFollowerPromotion: false
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(0),
                               NLs::CrossDataCenterFollowerCount(0),
                               NLs::AllowFollowerPromotion(false),
                               NLs::FollowerGroups({})
                           });

        //////////
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                CrossDataCenterFollowerCount: 1
                                AllowFollowerPromotion: true
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(0),
                               NLs::CrossDataCenterFollowerCount(1),
                               NLs::AllowFollowerPromotion(true),
                               NLs::FollowerGroups({})
                           });

        //////////
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                CrossDataCenterFollowerCount: 0
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(0),
                               NLs::CrossDataCenterFollowerCount(0),
                               NLs::AllowFollowerPromotion(true),
                               NLs::FollowerGroups({})
                           });

        //////////
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                AllowFollowerPromotion: false
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(0),
                               NLs::CrossDataCenterFollowerCount(0),
                               NLs::AllowFollowerPromotion(false),
                               NLs::FollowerGroups({})
                           });

        //////////
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                CrossDataCenterFollowerCount: 2
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(0),
                               NLs::CrossDataCenterFollowerCount(2),
                               NLs::AllowFollowerPromotion(false),
                               NLs::FollowerGroups({})
                           });

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            PartitionConfig {
                                FollowerCount: 1
                            }
                        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {
                               NLs::FollowerCount(0),
                               NLs::CrossDataCenterFollowerCount(2),
                               NLs::AllowFollowerPromotion(false),
                               NLs::FollowerGroups({})
                           });

        /////////
        {
            TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerGroups {
                                        FollowerCount: 1
                                    }
                                }
                            )");
            env.TestWaitNotification(runtime, txId);

            NKikimrHive::TFollowerGroup control;
            control.SetFollowerCount(1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {
                                   NLs::FollowerCount(0),
                                   NLs::CrossDataCenterFollowerCount(0),
                                   NLs::AllowFollowerPromotion(false),
                                   NLs::FollowerGroups({control})
                               });
        }

        /////////
        {
            TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerCount: 1
                                }
                            )", {NKikimrScheme::StatusInvalidParameter});
            env.TestWaitNotification(runtime, txId);

            NKikimrHive::TFollowerGroup control;
            control.SetFollowerCount(1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {
                                   NLs::FollowerCount(0),
                                   NLs::CrossDataCenterFollowerCount(0),
                                   NLs::AllowFollowerPromotion(false),
                                   NLs::FollowerGroups({control})
                               });
        }

        /////////
        {
            TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    CrossDataCenterFollowerCount: 2
                                }
                            )", {NKikimrScheme::StatusInvalidParameter});
            env.TestWaitNotification(runtime, txId);

            NKikimrHive::TFollowerGroup control;
            control.SetFollowerCount(1);

            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                               {
                                   NLs::FollowerCount(0),
                                   NLs::CrossDataCenterFollowerCount(0),
                                   NLs::AllowFollowerPromotion(false),
                                   NLs::FollowerGroups({control})
                               });
         }

         //////////
         {
             TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    AllowFollowerPromotion: false
                                }
                            )", {NKikimrScheme::StatusInvalidParameter});
             env.TestWaitNotification(runtime, txId);

             NKikimrHive::TFollowerGroup control;
             control.SetFollowerCount(1);

             TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                {
                                    NLs::FollowerCount(0),
                                    NLs::CrossDataCenterFollowerCount(0),
                                    NLs::AllowFollowerPromotion(false),
                                    NLs::FollowerGroups({control})
                                });
          }

          ////////////
          {
              TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerGroups {
                                        FollowerCount: 10
                                        AllowLeaderPromotion: false
                                        RequireAllDataCenters: true
                                    }
                                }
                            )", {NKikimrScheme::StatusInvalidParameter});
              env.TestWaitNotification(runtime, txId);

              NKikimrHive::TFollowerGroup control;
              control.SetFollowerCount(1);

              TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                 {
                                     NLs::FollowerCount(0),
                                     NLs::CrossDataCenterFollowerCount(0),
                                     NLs::AllowFollowerPromotion(false),
                                     NLs::FollowerGroups({control})
                                 });
          }

          ////////////
          {
              TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerGroups {
                                        LocalNodeOnly: true
                                    }
                                }
                            )", {NKikimrScheme::StatusInvalidParameter});
              env.TestWaitNotification(runtime, txId);

              NKikimrHive::TFollowerGroup control;
              control.SetFollowerCount(1);

              TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                 {
                                     NLs::FollowerCount(0),
                                     NLs::CrossDataCenterFollowerCount(0),
                                     NLs::AllowFollowerPromotion(false),
                                     NLs::FollowerGroups({control})
                                 });
          }

          ////////////
          {
              TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerGroups {
                                        RequireDifferentNodes: true
                                    }
                                }
                            )", {NKikimrScheme::StatusInvalidParameter});
              env.TestWaitNotification(runtime, txId);

              NKikimrHive::TFollowerGroup control;
              control.SetFollowerCount(1);

              TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                 {
                                     NLs::FollowerCount(0),
                                     NLs::CrossDataCenterFollowerCount(0),
                                     NLs::AllowFollowerPromotion(false),
                                     NLs::FollowerGroups({control})
                                 });
          }

          ////////////
          {
              TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerGroups {
                                        FollowerCount: 3
                                        AllowLeaderPromotion: true
                                        RequireAllDataCenters: true
                                    }
                                    FollowerGroups {
                                        FollowerCount: 3
                                        AllowLeaderPromotion: true
                                        RequireAllDataCenters: true
                                    }
                                }
                            )", {NKikimrScheme::StatusInvalidParameter});
              env.TestWaitNotification(runtime, txId);

              NKikimrHive::TFollowerGroup control;
              control.SetFollowerCount(1);

              TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                 {
                                     NLs::FollowerCount(0),
                                     NLs::CrossDataCenterFollowerCount(0),
                                     NLs::AllowFollowerPromotion(false),
                                     NLs::FollowerGroups({control})
                                 });
          }

          ////////////
          {
              TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                                Name: "Table"
                                PartitionConfig {
                                    FollowerGroups {
                                        FollowerCount: 3
                                        AllowLeaderPromotion: true
                                        RequireAllDataCenters: true
                                    }
                                }
                            )");
              env.TestWaitNotification(runtime, txId);

              NKikimrHive::TFollowerGroup control;
              control.SetFollowerCount(3);
              control.SetAllowLeaderPromotion(true);
              control.SetRequireAllDataCenters(true);

              TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                                 {
                                     NLs::FollowerCount(0),
                                     NLs::CrossDataCenterFollowerCount(0),
                                     NLs::AllowFollowerPromotion(false),
                                     NLs::FollowerGroups({control})
                                 });
          }
    }


    Y_UNIT_TEST(AlterTableSizeToSplit) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key"]
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::SizeToSplitEqual(0)});

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            PartitioningPolicy {
                                SizeToSplit: 100500
                            }
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {NLs::SizeToSplitEqual(100500)});
    }

    Y_UNIT_TEST(AlterTableSplitSchema) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto schemaChecker = [](TVector<TString> columns) {
            return [=] (const NKikimrSchemeOp::TTableDescription& tableDescription) {
                UNIT_ASSERT_VALUES_EQUAL(tableDescription.ColumnsSize(), columns.size());
                for (size_t idx = 0; idx < tableDescription.ColumnsSize(); ++idx) {
                    UNIT_ASSERT_VALUES_EQUAL_C(tableDescription.GetColumns(idx).GetName(), columns[idx],
                        "While comparing column " << idx);
                }
            };
        };

        auto checkSchema = [&](TVector<int> tabletIdxs, TVector<TString> columns) {
            auto checker = schemaChecker(std::move(columns));
            for (int tabletIdx : tabletIdxs) {
                ui64 tabletId = TTestTxConfig::FakeHiveTablets + tabletIdx;
                NKikimrSchemeOp::TTableDescription tableDescription = GetDatashardSchema(runtime, tabletId, 2);
                checker(tableDescription);
            }
        };

        // Creating a table with initial columns
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "Key"    Type: "Uint32" }
                            Columns { Name: "Value"  Type: "Utf8"   }
                            KeyColumnNames: ["Key"]
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 100 } }
                            }}
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 200 } }
                            }}
                            )");
        env.TestWaitNotification(runtime, txId);

        checkSchema({ 0, 1, 2 }, { "Key", "Value" });

        // Alter table adding more volumes
        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table1"
                            Columns { Name: "Add1"  Type: "Uint32" }
                            Columns { Name: "Add2"  Type: "Uint32" }
                            )");
        env.TestWaitNotification(runtime, txId);

        checkSchema({ 0, 1, 2 }, { "Key", "Value", "Add1", "Add2" });

        // Split the middle tablet in two
        TestSplitTable(runtime, ++txId, "/MyRoot/Table1", R"(
                            SourceTabletId: 72075186233409547
                            SplitBoundary { KeyPrefix {
                                Tuple { Optional { Uint32 : 150 } }
                            }}
                            )");
        env.TestWaitNotification(runtime, txId);

        checkSchema({ 0, 3, 4, 2 }, { "Key", "Value", "Add1", "Add2" });
    }

    Y_UNIT_TEST(AlterTableSettings) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        struct TCheckExecutorFastLogPolicy {
            bool ExpectedValue;
            void operator ()(const NKikimrScheme::TEvDescribeSchemeResult& describeRec) {
                bool real = describeRec.GetPathDescription().GetTable().GetPartitionConfig().GetExecutorFastLogPolicy();
                UNIT_ASSERT_VALUES_EQUAL(ExpectedValue, real);
            }
        };

        struct TCheckEnableFilterByKey {
            bool ExpectedValue;
            void operator ()(const NKikimrScheme::TEvDescribeSchemeResult& describeRec) {
                bool real = describeRec.GetPathDescription().GetTable().GetPartitionConfig().GetEnableFilterByKey();
                UNIT_ASSERT_VALUES_EQUAL(ExpectedValue, real);
            }
        };

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                            Name: "Table"
                            Columns { Name: "key"   Type: "Uint64"}
                            Columns { Name: "value"  Type: "Utf8"}
                            KeyColumnNames: ["key"]
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {TCheckExecutorFastLogPolicy{true},
                            TCheckEnableFilterByKey{false}});
        UNIT_ASSERT_VALUES_EQUAL_C(true, GetFastLogPolicy(runtime, TTestTxConfig::FakeHiveTablets), "FastLogPolicy must be enabled by default");
        UNIT_ASSERT_VALUES_EQUAL_C(false, GetByKeyFilterEnabled(runtime, TTestTxConfig::FakeHiveTablets, 1001), "ByKeyFilter must be disabled by default");

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            ExecutorFastLogPolicy: false
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {TCheckExecutorFastLogPolicy{false},
                            TCheckEnableFilterByKey{false}});
        UNIT_ASSERT_VALUES_EQUAL(false, GetFastLogPolicy(runtime, TTestTxConfig::FakeHiveTablets));
        UNIT_ASSERT_VALUES_EQUAL(false, GetByKeyFilterEnabled(runtime, TTestTxConfig::FakeHiveTablets, 1001));

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            EnableFilterByKey: true
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {TCheckExecutorFastLogPolicy{false},
                            TCheckEnableFilterByKey{true}});
        UNIT_ASSERT_VALUES_EQUAL(false, GetFastLogPolicy(runtime, TTestTxConfig::FakeHiveTablets));
        UNIT_ASSERT_VALUES_EQUAL(true, GetByKeyFilterEnabled(runtime, TTestTxConfig::FakeHiveTablets, 1001));

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            EnableEraseCache: true
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {TCheckExecutorFastLogPolicy{false},
                            TCheckEnableFilterByKey{true}});
        UNIT_ASSERT_VALUES_EQUAL(false, GetFastLogPolicy(runtime, TTestTxConfig::FakeHiveTablets));
        UNIT_ASSERT_VALUES_EQUAL(true, GetByKeyFilterEnabled(runtime, TTestTxConfig::FakeHiveTablets, 1001));
        UNIT_ASSERT_VALUES_EQUAL(true, GetEraseCacheEnabled(runtime, TTestTxConfig::FakeHiveTablets, 1001));

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            EnableEraseCache: false
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {TCheckExecutorFastLogPolicy{false},
                            TCheckEnableFilterByKey{true}});
        UNIT_ASSERT_VALUES_EQUAL(false, GetFastLogPolicy(runtime, TTestTxConfig::FakeHiveTablets));
        UNIT_ASSERT_VALUES_EQUAL(true, GetByKeyFilterEnabled(runtime, TTestTxConfig::FakeHiveTablets, 1001));
        UNIT_ASSERT_VALUES_EQUAL(false, GetEraseCacheEnabled(runtime, TTestTxConfig::FakeHiveTablets, 1001));

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Table"
                        PartitionConfig {
                            ExecutorFastLogPolicy: true
                            EnableFilterByKey: false
                        }
                    )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table", true),
                           {TCheckExecutorFastLogPolicy{true},
                            TCheckEnableFilterByKey{false}});
        UNIT_ASSERT_VALUES_EQUAL(true, GetFastLogPolicy(runtime, TTestTxConfig::FakeHiveTablets));
        UNIT_ASSERT_VALUES_EQUAL(false, GetByKeyFilterEnabled(runtime, TTestTxConfig::FakeHiveTablets, 1001));
        UNIT_ASSERT_VALUES_EQUAL(false, GetEraseCacheEnabled(runtime, TTestTxConfig::FakeHiveTablets, 1001));
    }

    Y_UNIT_TEST(CreatePersQueueGroup) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 1000;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", "");
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_2\""
                        "TotalGroupCount: 10 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_3\""
                        "TotalGroupCount: 10 "
                        "PartitionPerTablet: 3 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");
        TestModificationResult(runtime, txId-3, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusSchemeError);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId-3, txId-2, txId-1, txId});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::Finished,
                            NLs::PathsInsideDomain(3),
                            NLs::ShardsInsideDomain(7)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup_2", true),
                           {NLs::CheckPartCount("PQGroup_2", 10, 10, 1, 10)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup_3", true),
                           {NLs::CheckPartCount("PQGroup_3", 10, 3, 4, 10)});

        TestCreatePQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 100 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}"
                        );

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/PQGroup_1", true),
                           {NLs::CheckPartCount("PQGroup_1", 100, 10, 10, 100),
                            NLs::PathsInsideDomain(4),
                            NLs::ShardsInsideDomain(18),
                            NLs::PathVersionEqual(2),
                            NLs::Finished});
    }

    Y_UNIT_TEST(AlterPersQueueGroup) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 1000;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 4 "
                        "PartitionPerTablet: 3 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup"),
                           {NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(3)});

        // invalid params
        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 3 "
                        "PartitionPerTablet: 3 ",
                         {NKikimrScheme::StatusInvalidParameter});
        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 4 "
                        "PartitionPerTablet: 2 ",
                         {NKikimrScheme::StatusInvalidParameter});

        // TODO: describe + alter by PathId

        // same sizes - reconfig
        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup\""
                        "PartitionPerTablet: 3 "); // do not change TotalGroupCount
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 4, 3, 2, 4),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(3)});

        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 6 "
                        "PartitionPerTablet: 3 "
                        "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 42}}");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 6, 3, 2, 6),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(3)});

        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 8 "); // do not change PartitionPerTablet
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 8, 3, 3, 8),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(4)});

        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 8 "
                        "PartitionPerTablet: 4 ");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 8, 4, 3, 8),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(4)});

        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 14 "
                        "PartitionPerTablet: 4 ");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 14, 4, 4, 14),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(5)});

        // Alter + Alter + reboot
        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
                        "Name: \"PQGroup\""
                        "TotalGroupCount: 400 "
                        "PartitionPerTablet: 10 ");
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", "Name: \"PQGroup\"",
                         {NKikimrScheme::StatusMultipleModifications});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 14, 4, 4, 14, NKikimrSchemeOp::EPathStateAlter),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(41)});


        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        env.TestWaitNotification(runtime, txId-1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup", true),
                           {NLs::CheckPartCount("PQGroup", 400, 10, 40, 400),
                            NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(41)});
    }

    Y_UNIT_TEST(CreatePersQueueGroupWithKeySchema) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Partition count and partition boundaries mismatch
        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
                PartitionKeySchema { Name: "key1" TypeId: 2 }
            }
            PartitionBoundaries {
                Tuple { Optional { Uint32: 1000 } }
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        // Missing key schema
        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup"
            TotalGroupCount: 2
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
            }
            PartitionBoundaries {
                Tuple { Optional { Uint32: 1000 } }
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        // Invalid partition boundary
        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup"
            TotalGroupCount: 2
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
                PartitionKeySchema { Name: "key1" TypeId: 2 }
            }
            PartitionBoundaries {
                Tuple { Optional { Uint32: 1000 } }
                Tuple { Optional { Uint32: 2000 } }
            }
        )", {NKikimrScheme::StatusSchemeError});

        // Invalid type
        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup"
            TotalGroupCount: 2
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
                PartitionKeySchema { Name: "key1" TypeId: 0 }
            }
            PartitionBoundaries {
                Tuple { Optional { Uint32: 1000 } }
            }
        )", {NKikimrScheme::StatusSchemeError});

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
                PartitionKeySchema { Name: "key1" TypeId: 2 }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup1", true), {
            NLs::CheckPartCount("PQGroup1", 1, 1, 1, 1),
        });

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup2"
            TotalGroupCount: 2
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
                PartitionKeySchema { Name: "key1" TypeId: 2 }
            }
            PartitionBoundaries {
                Tuple { Optional { Uint32: 1000 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup2", true), {
            NLs::CheckPartCount("PQGroup2", 2, 1, 2, 2),
        });

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup3"
            TotalGroupCount: 2
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
                PartitionKeySchema { Name: "key1" TypeId: 2 }
                PartitionKeySchema { Name: "key2" TypeId: 2 }
            }
            PartitionBoundaries {
                Tuple { Optional { Uint32: 1000 } }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQGroup3", true), {
            NLs::CheckPartCount("PQGroup3", 2, 1, 2, 2),
        });
    }

    Y_UNIT_TEST(AlterPersQueueGroupWithKeySchema) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
                PartitionKeySchema { Name: "key1" TypeId: 2 }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // change partition count
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup"
            TotalGroupCount: 2
        )", {NKikimrScheme::StatusInvalidParameter});

        // add partitions
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup"
            PartitionsToAdd { PartitionId: 1 GroupId: 1 }
        )", {NKikimrScheme::StatusInvalidParameter});

        // change key schema
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "PQGroup"
            PQTabletConfig {
                PartitionKeySchema { Name: "key2" TypeId: 2 }
            }
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(TopicMeteringMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
                MeteringMode: METERING_MODE_REQUEST_UNITS
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(
            DescribePath(runtime, "/MyRoot/Topic1"), {
                NLs::PathExist,
                NLs::Finished, [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& config = record.GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
                    UNIT_ASSERT(config.HasMeteringMode());
                    UNIT_ASSERT(config.GetMeteringMode() == NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);
                }
            }
        );

        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(
            DescribePath(runtime, "/MyRoot/Topic1"), {
                NLs::PathExist,
                NLs::Finished, [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& config = record.GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
                    UNIT_ASSERT(config.HasMeteringMode());
                    UNIT_ASSERT(config.GetMeteringMode() == NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY);
                }
            }
        );

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic2"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig { LifetimeSeconds: 10 }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(
            DescribePath(runtime, "/MyRoot/Topic2"), {
                NLs::PathExist,
                NLs::Finished, [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& config = record.GetPathDescription().GetPersQueueGroup().GetPQTabletConfig();
                    UNIT_ASSERT(!config.HasMeteringMode());
                }
            }
        );
    }

    Y_UNIT_TEST(DropTable) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 1000;

        TString tcfg1 = "Name: \"Table\""
            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
            "KeyColumnNames: [\"RowId\"]";

        TString tcfg2 = "Name: \"PartTable\""
                    "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                    "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                    "Columns { Name: \"key3\"       Type: \"Uint64\"}"
                    "Columns { Name: \"value\"      Type: \"Utf8\"}"
                    "KeyColumnNames: [\"key1\", \"key2\", \"key3\"]"
                    "UniformPartitionsCount: 10";

        TestMkDir(runtime, ++txId, "/MyRoot", "Ops");

        TestDropTable(runtime, ++txId, "/MyRoot/Ops", "Table", {NKikimrScheme::StatusPathDoesNotExist});

        Cdbg << "Create, Drop (simple table)" << Endl;
        TestCreateTable(runtime, ++txId, "/MyRoot/Ops", tcfg1);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/Table", true),
                           {NLs::Finished});

        TestRmDir(runtime, ++txId, "/MyRoot", "Ops", {NKikimrScheme::StatusNameConflict});

        TestDropTable(runtime, ++txId, "/MyRoot/Ops", "Table");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/Table"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops"),
                           {NLs::NoChildren});

        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets);


        Cdbg << "Create, Drop (partitioned table)" << Endl;
        TestCreateTable(runtime, ++txId, "/MyRoot/Ops", tcfg2);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/PartTable", true),
                           {NLs::Finished});

        TestRmDir(runtime, ++txId, "/MyRoot", "Ops", {NKikimrScheme::StatusNameConflict});

        TestDropTable(runtime, ++txId, "/MyRoot/Ops", "PartTable");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/Table"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops"),
                           {NLs::NoChildren});

        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets+1);
    }

    Y_UNIT_TEST(DropTableById) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 1000;

        TString tcfg = "Name: \"Table\""
            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
            "KeyColumnNames: [\"RowId\"]";

        //

        Cdbg << "Create, Drop (simple table)" << Endl;
        TestCreateTable(runtime, ++txId, "/MyRoot", tcfg);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::Finished});

        TestDropTable(runtime, ++txId, 2);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::NoChildren});

        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets);
    }

    Y_UNIT_TEST(DropPQ) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 1000;

        TString tableConfig = "Name: \"DropMeBaby\""
            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
            "KeyColumnNames: [\"RowId\"]";

        TString pqGroupConfig = "Name: \"DropMeBaby\""
            "TotalGroupCount: 100 "
            "PartitionPerTablet: 10 "
            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}";

        TString pqGroupConfig1 = "Name: \"DropMeBaby\""
            "TotalGroupCount: 2 "
            "PartitionPerTablet: 1 "
            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}";

        TString pqGroupAlter = "Name: \"DropMeBaby\""
                "TotalGroupCount: 3 ";

        TestMkDir(runtime, ++txId, "/MyRoot", "Ops");

        TestDropPQGroup(runtime, ++txId, "/MyRoot/Ops", "DropMeBaby", {NKikimrScheme::StatusPathDoesNotExist});

        TestCreatePQGroup(runtime, ++txId, "/MyRoot/Ops", pqGroupConfig);
        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops"),
                           {NLs::Finished});

        TestRmDir(runtime, ++txId, "/MyRoot", "Ops", {NKikimrScheme::StatusNameConflict});

        TestDropPQGroup(runtime, ++txId, "/MyRoot/Ops", "DropMeBaby");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops"),
                           {NLs::NoChildren});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/DropMeBaby"),
                           {NLs::PathNotExist});

        // check config after Drop

        TestCreatePQGroup(runtime, ++txId, "/MyRoot/Ops", pqGroupConfig1);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/DropMeBaby", true),
                           {NLs::CheckPartCount("DropMeBaby", 2, 1, 2, 2)});

        TestAlterPQGroup(runtime, ++txId, "/MyRoot/Ops", pqGroupAlter);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/DropMeBaby", true),
                           {NLs::CheckPartCount("DropMeBaby", 3, 1, 3, 3)});

        TestDropPQGroup(runtime, ++txId, "/MyRoot/Ops", "DropMeBaby");
        env.TestWaitNotification(runtime, txId);

        TestCreatePQGroup(runtime, ++txId, "/MyRoot/Ops", pqGroupConfig1);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/DropMeBaby", true),
                           {NLs::CheckPartCount("DropMeBaby", 2, 1, 2, 2)});

        TestDropPQGroup(runtime, ++txId, "/MyRoot/Ops", "DropMeBaby");
        env.TestWaitNotification(runtime, txId);

        //

        TestCreateTable(runtime, ++txId, "/MyRoot/Ops", tableConfig);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/DropMeBaby", true),
                           {NLs::Finished});

        TestRmDir(runtime, ++txId, "/MyRoot", "Ops", {NKikimrScheme::StatusNameConflict});

        TestDropTable(runtime, ++txId, "/MyRoot/Ops", "DropMeBaby");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops"),
                           {NLs::NoChildren});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/DropMeBaby"),
                           {NLs::PathNotExist});

        //

        Cdbg << "Create + Drop + Drop" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot/Ops", pqGroupConfig);
        auto pVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/DropMeBaby"));
        AsyncDropPQGroup(runtime, ++txId, "/MyRoot/Ops", "DropMeBaby");
        AsyncForceDropUnsafe(runtime, ++txId, pVer.PathId.LocalPathId);

        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        env.TestWaitNotification(runtime, {txId, txId-1, txId-2});
        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+20));

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops/DropMeBaby"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops"),
                           {NLs::NoChildren,
                            NLs::PathExist});

        TestRmDir(runtime, ++txId, "/MyRoot", "Ops");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Ops"),
                           {NLs::PathNotExist});

        TestRmDir(runtime, ++txId, "/MyRoot", "Ops", {NKikimrScheme::StatusPathDoesNotExist});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+40));
    }

    Y_UNIT_TEST(ParallelModifying) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 1000;

        TString pqGroupConfig = "Name: \"Isolda\""
            "TotalGroupCount: 40 "
            "PartitionPerTablet: 10 "
            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}";

        TString pqGroupAlter = "Name: \"Isolda\""
            "TotalGroupCount: 100 ";

        Cdbg << "Create + Drop + Create" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        AsyncDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        env.TestWaitNotification(runtime, txId);

        Cdbg << "Create + Create + Drop" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        AsyncDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        env.TestWaitNotification(runtime, txId);

        Cdbg << "Create + Alter + Drop" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        AsyncAlterPQGroup(runtime, ++txId, "/MyRoot", pqGroupAlter);
        AsyncDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        env.TestWaitNotification(runtime, txId);

        Cdbg << "Create + Drop + Alter" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        AsyncDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        AsyncAlterPQGroup(runtime, ++txId, "/MyRoot", pqGroupAlter);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        env.TestWaitNotification(runtime, txId);

        Cdbg << "Create + Drop + Drop" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        AsyncDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        AsyncDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        env.TestWaitNotification(runtime, txId);

        Cdbg << "Create + Alter + Create + Drop" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        AsyncAlterPQGroup(runtime, ++txId, "/MyRoot", pqGroupAlter);
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        AsyncDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        TestModificationResult(runtime, txId-3, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-3, txId-2, txId-1, txId});

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        env.TestWaitNotification(runtime, txId);

        Cdbg << "Create + Alter + Alter + Drop" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, "/MyRoot", pqGroupConfig);
        AsyncAlterPQGroup(runtime, ++txId, "/MyRoot", pqGroupAlter);
        AsyncAlterPQGroup(runtime, ++txId, "/MyRoot", pqGroupAlter);
        AsyncDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        TestModificationResult(runtime, txId-3, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-3, txId-2, txId-1, txId});

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Isolda");
        env.TestWaitNotification(runtime, txId);

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+40));
    }

    Y_UNIT_TEST(DropPQFail) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString pqGroupConfig = "Name: \"Isolda\""
                                "TotalGroupCount: 100 "
                                "PartitionPerTablet: 10 "
                                "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}";

        const char * path = "/MyRoot/A/B/C/D/I/F";
        AsyncMkDir(runtime, ++txId, "/MyRoot", "A");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A", "B");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A/B", "C");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A/B/C", "D");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A/B/C/D", "I");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A/B/C/D/I", "F");
        AsyncCreatePQGroup(runtime, ++txId, path, pqGroupConfig);
        AsyncDropPQGroup(runtime, ++txId, path, "Isolda");
        SkipModificationReply(runtime, 8);
        env.TestWaitNotification(runtime, xrange(txId-7, txId+1));

        TestDescribeResult(DescribePath(runtime, TString(path) + "/Isolda"),
                           {NLs::Finished});

        TestDropPQGroup(runtime, ++txId, path, "Isolda");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, TString(path) + "/Isolda"),
                           {NLs::PathNotExist});

        Cdbg << "Create + Drop (fail) + Drop" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, path, pqGroupConfig);
        AsyncDropPQGroup(runtime, ++txId, path, "Isolda");
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId-1, txId});

        TestDropPQGroup(runtime, ++txId, path, "Isolda");
        env.TestWaitNotification(runtime, txId);

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+5));
    }

    Y_UNIT_TEST(DropPQAbort) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        //using ESts = NKikimrScheme::EStatus;

        const TString basePath = "/MyRoot/A/B/C/D/I/F";
        const TString pqPath = basePath + "/Isolda";

        TString pqGroupConfig = "Name: \"Isolda\""
            "TotalGroupCount: 1 "
            "PartitionPerTablet: 10 "
            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}";

        TString pqGroupBigConfig = "Name: \"Isolda\""
            "TotalGroupCount: 1000 "
            "PartitionPerTablet: 10 "
            "PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}";

        TString pqGroupBigAlter = "Name: \"Isolda\""
            "TotalGroupCount: 1000 ";

        AsyncMkDir(runtime, ++txId, "/MyRoot", "A");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A", "B");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A/B", "C");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A/B/C", "D");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A/B/C/D", "I");
        AsyncMkDir(runtime, ++txId, "/MyRoot/A/B/C/D/I", "F");
        Cdbg << "N*MkDir + CreatePQ (big) + Drop (abort)" << Endl;
        AsyncCreatePQGroup(runtime, ++txId, basePath, pqGroupBigConfig);
        AsyncDropPQGroup(runtime, ++txId, basePath, "Isolda");
        auto pVer = TestDescribeResult(DescribePath(runtime, pqPath));
        AsyncForceDropUnsafe(runtime, ++txId, pVer.PathId.LocalPathId);
        TestModificationResult(runtime, txId-8, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-7, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-6, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-5, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-4, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-3, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, xrange(txId - 8, txId + 1));

        TestDescribeResult(DescribePath(runtime, pqPath),
                           {NLs::PathNotExist});

        TestCreatePQGroup(runtime, ++txId, basePath, pqGroupBigConfig);
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, pqPath),
                           {NLs::PathExist});

        Cdbg << "AlterPQ (big) + Drop (abort)" << Endl;
        AsyncAlterPQGroup(runtime, ++txId, basePath, pqGroupBigAlter);
        AsyncDropPQGroup(runtime, ++txId, basePath, "Isolda");
        pVer = TestDescribeResult(DescribePath(runtime, pqPath));
        AsyncForceDropUnsafe(runtime, ++txId, pVer.PathId.LocalPathId);
        TestModificationResult(runtime, txId-2, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusMultipleModifications);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusAccepted);
        env.TestWaitNotification(runtime, {txId-2, txId-1, txId});

        TestDescribeResult(DescribePath(runtime, pqPath),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+20));
    }

    Y_UNIT_TEST(PQGroupExplicitChannels) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");

        // Cannot create a group with 2 channels
        TestCreatePQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 10 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig { PartitionConfig { "
                        "  LifetimeSeconds : 10 "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "} }",
                        {NKikimrScheme::StatusInvalidParameter});

        // It's ok to create a group with 4 channels
        TestCreatePQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 10 "
                        "PartitionPerTablet: 10 "
                        "PQTabletConfig { PartitionConfig { "
                        "  LifetimeSeconds : 10 "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-2\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-2\" } "
                        "} }");
        env.TestWaitNotification(runtime, txId);

        for (const auto& kv : env.GetHiveState()->Tablets) {
            if (kv.second.Type == ETabletType::PersQueue) {
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels.size(), 4u);
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[0].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[1].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[2].GetStoragePoolName(), "pool-2");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[3].GetStoragePoolName(), "pool-2");
            }
        }

        // Add more tablets, channels shouldn't change
        TestAlterPQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 11 "
                        "PartitionPerTablet: 11 "
                        "PQTabletConfig { PartitionConfig { "
                        "  LifetimeSeconds : 10 "
                        "} }");
        env.TestWaitNotification(runtime, txId);

        // One more time, we should still use explicit channel profiles
        TestAlterPQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 12 "
                        "PartitionPerTablet: 12 "
                        "PQTabletConfig { PartitionConfig { "
                        "  LifetimeSeconds : 10 "
                        "} }");
        env.TestWaitNotification(runtime, txId);

        for (const auto& kv : env.GetHiveState()->Tablets) {
            if (kv.second.Type == ETabletType::PersQueue) {
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels.size(), 4u);
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[0].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[1].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[2].GetStoragePoolName(), "pool-2");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[3].GetStoragePoolName(), "pool-2");
            }
        }

        // Add more channels, tablets should be recreated
        TestAlterPQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 12 "
                        "PartitionPerTablet: 12 "
                        "PQTabletConfig { PartitionConfig { "
                        "  LifetimeSeconds : 10 "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-2\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-2\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-2\" } "
                        "} }");
        env.TestWaitNotification(runtime, txId);

        for (const auto& kv : env.GetHiveState()->Tablets) {
            if (kv.second.Type == ETabletType::PersQueue) {
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels.size(), 5u);
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[0].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[1].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[2].GetStoragePoolName(), "pool-2");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[3].GetStoragePoolName(), "pool-2");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[4].GetStoragePoolName(), "pool-2");
            }
        }

        // Cannot reduce the number of channels back to 4
        TestAlterPQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 12 "
                        "PartitionPerTablet: 12 "
                        "PQTabletConfig { PartitionConfig { "
                        "  LifetimeSeconds : 10 "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-2\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-2\" } "
                        "} }",
                        {NKikimrScheme::StatusInvalidParameter});

        // Changing pool kinds, tablets should be recreated
        TestAlterPQGroup(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"PQGroup_1\""
                        "TotalGroupCount: 12 "
                        "PartitionPerTablet: 12 "
                        "PQTabletConfig { PartitionConfig { "
                        "  LifetimeSeconds : 10 "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "  ExplicitChannelProfiles { PoolKind: \"pool-kind-1\" } "
                        "} }");
        env.TestWaitNotification(runtime, txId);

        for (const auto& kv : env.GetHiveState()->Tablets) {
            if (kv.second.Type == ETabletType::PersQueue) {
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels.size(), 5u);
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[0].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[1].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[2].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[3].GetStoragePoolName(), "pool-1");
                UNIT_ASSERT_VALUES_EQUAL(kv.second.BoundChannels[4].GetStoragePoolName(), "pool-1");
            }
        }
    }

    Y_UNIT_TEST(Restart) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        AsyncMkDir(runtime, ++txId, "/MyRoot/DirA", "SubDirA");
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA",
                            "Name: \"Table1\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]");
        AsyncMkDir(runtime, ++txId, "/MyRoot/DirA/SubDirA", "AAA");
        AsyncMkDir(runtime, ++txId, "/MyRoot/DirA/SubDirA", "BBB");
        AsyncMkDir(runtime, ++txId, "/MyRoot/DirA/SubDirA", "CCC");

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA"),
                           {NLs::PathExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table1"),
                           {NLs::PathExist,
                            NLs::IsTable});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA"),
                           {NLs::PathExist,
                            NLs::ChildrenCount(3)});

        TestMkDir(runtime, ++txId, "/MyRoot/DirA/SubDirA", "DDD");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/SubDirA/DDD"),
                           {NLs::PathExist});

        env.TestWaitNotification(runtime, xrange(txId-6, txId+1));

    }

    Y_UNIT_TEST(ReadOnlyMode) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "SubDirA");
        AsyncCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table1\""
                            "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                            "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                            "KeyColumnNames: [\"RowId\"]");
        // Set ReadOnly
        SetSchemeshardReadOnlyMode(runtime, true);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Verify that table creation successfully finished
        env.TestWaitNotification(runtime, txId);

        // Check that describe works
        TestDescribeResult(DescribePath(runtime, "/MyRoot/SubDirA"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                           {NLs::Finished,
                            NLs::IsTable});

        // Check that new modifications fail
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB", {NKikimrScheme::StatusReadOnly});
        TestCreateTable(runtime, ++txId, "/MyRoot",
                                "Name: \"Table1\""
                                    "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                    "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                    "KeyColumnNames: [\"RowId\"]",
                                {NKikimrScheme::StatusReadOnly});

        // Disable ReadOnly
        SetSchemeshardReadOnlyMode(runtime, false);
        sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        // Check that modifications now work again
        TestMkDir(runtime, ++txId, "/MyRoot", "SubDirBBBB");
    }

    Y_UNIT_TEST(PathErrors) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        auto tableDescr = [=] (const TString& name) {
            return Sprintf(R"(Name: "%s"
                Columns { Name: "RowId" Type: "Uint64" }
                KeyColumnNames: ["RowId"]
            )", name.c_str());
        };

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA", {NKikimrScheme::StatusAlreadyExists});
        TestCreateTable(runtime, ++txId, "/MyRoot", tableDescr("DirA"), {NKikimrScheme::StatusNameConflict});
        TestMkDir(runtime, ++txId, "/MyRoot/DirA/SubDirA", "AAA", {NKikimrScheme::StatusPathDoesNotExist});

        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA", tableDescr("Table1"));
        AsyncCreateTable(runtime, ++txId, "/MyRoot/DirA/", tableDescr("Table1"));
        TestModificationResult(runtime, txId-1, NKikimrScheme::StatusAccepted);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId, txId-1});

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA/", tableDescr("Table1"), {NKikimrScheme::StatusAlreadyExists});
        TestMkDir(runtime, ++txId, "/MyRoot/DirA", "Table1", {NKikimrScheme::StatusNameConflict});
        TestMkDir(runtime, ++txId, "/MyRoot/DirA/Table1", "CCC", {NKikimrScheme::StatusPathIsNotDirectory});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/Table1"),
                           {NLs::IsTable,
                            NLs::Finished});

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA", tableDescr("/WrongPath"), {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot/DirA", tableDescr("WrongPath/"), {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot/DirA", tableDescr("Table1/WrongPath"), {NKikimrScheme::StatusPathIsNotDirectory});
    }

    Y_UNIT_TEST(SchemeErrors) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table2\""
                            "Columns { Name: \"RowId\"      Type: \"BlaBlaType\"}"
                            "KeyColumnNames: [\"RowId\"]",
                        {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table2\""
                            "Columns { Name: \"RowId\"      Type: \"Uint32\"}",
                        {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table2\""
                            "Columns { Name: \"RowId\"      Type: \"Uint32\"}"
                            "KeyColumnNames: [\"AAAA\"]",
                        {NKikimrScheme::StatusSchemeError});
        TestCreateTable(runtime, ++txId, "/MyRoot/DirA",
                        "Name: \"Table2\""
                            "Columns { Name: \"RowId\"      Type: \"Uint32\"}"
                            "KeyColumnNames: [\"RowId\", \"RowId\"]",
                        {NKikimrScheme::StatusSchemeError});
    }

    Y_UNIT_TEST(ManyDirs) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 num = 500;
        ui64 txId = 123;
        TSet<ui64> ids;

        for (ui32 id = 0; id < num; ++id) {
            AsyncMkDir(runtime, ++txId, "/MyRoot", Sprintf("Dir_%u", id));
            ids.insert(txId);
        }
        env.TestWaitNotification(runtime, ids);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(num),
                            NLs::ChildrenCount(num)});

        ids.clear();
        for (ui32 id = 0; id < num; ++id) {
            AsyncRmDir(runtime, ++txId, "/MyRoot", Sprintf("Dir_%u", id));
            ids.insert(txId);
        }
        env.TestWaitNotification(runtime, ids);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(0),
                            NLs::ChildrenCount(0)});
    }

    Y_UNIT_TEST(NestedDirs) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 123;
        TSchemeLimits limits;

        TString path = "/MyRoot";
        TSet<ui64> ids;
        for (ui32 i = 1; i < limits.MaxDepth; ++i) {
            TString name = Sprintf("%u", i);
            TestMkDir(runtime, ++txId, path, name);
            path += '/';
            path += name;
            ids.insert(txId);
        }
        env.TestWaitNotification(runtime, ids);

        TestMkDir(runtime, ++txId, path, "fail", {NKikimrScheme::StatusSchemeError});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(limits.MaxDepth - 1)});
    }

    void VerifyEqualCells(const TCell& a, const TCell& b) {
        UNIT_ASSERT_VALUES_EQUAL_C(a.IsNull(), b.IsNull(), "NULL/not-NULL mismatch");
        UNIT_ASSERT_VALUES_EQUAL_C(a.Size(), b.Size(), "size mismatch");
        if (!a.IsNull()) {
            TString aVal(a.Data(), a.Size());
            TString bVal(b.Data(), b.Size());
//            Cdbg << aVal << Endl;
            UNIT_ASSERT_VALUES_EQUAL_C(aVal, bVal, "data mismatch");
        }
    }

    void TestSerializedCellVec(TVector<TCell>& cells) {
        TString serialized = TSerializedCellVec::Serialize(TArrayRef<const TCell>(cells));
        UNIT_ASSERT_VALUES_EQUAL_C(cells.empty(), serialized.empty(), "Empty/non-empty mismatch");
        TSerializedCellVec deserialized(serialized);
        UNIT_ASSERT_VALUES_EQUAL_C(cells.size(), deserialized.GetCells().size(), "Sizes don't match");
        for (size_t i = 0; i < cells.size(); ++i) {
            VerifyEqualCells(cells[i], deserialized.GetCells()[i]);
        }
    }

    Y_UNIT_TEST(SerializedCellVec) { //+
        TVector<TCell> cells;
        TestSerializedCellVec(cells);

        for (size_t i = 0; i < 100; ++i) {
            cells.push_back(TCell());
            TestSerializedCellVec(cells);
        }

        cells.clear();
        char a[] = "1234728729hadjfhjvnaldjsagjkhsajklghslfkajshfgajklh";
        for (size_t i = 0; i < sizeof(a); ++i) {
            cells.push_back(TCell(a, i));
            TestSerializedCellVec(cells);
        }

        cells.clear();
        for (size_t i = 0; i < sizeof(a); ++i) {
            cells.push_back(TCell(a, i));
            cells.push_back(TCell());
            TestSerializedCellVec(cells);
        }
    }

    Y_UNIT_TEST(CreateFinishedInDescription) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncMkDir(runtime, ++txId, "/MyRoot", "DirA");
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::NotFinished});

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                           {NLs::Finished});
    }

    class TSuppressPlanStepObserver : public TNonCopyable {
    public:
        typedef std::function<void(TTestActorRuntime& runtime, bool& suppress)> TEventInjection;

    private:
        ui64 Coordinator;
        ui64 Shard;
        TEventInjection InjectionUnderSuppress;
        bool Injected;
        bool Suppress;

    public:
        TSuppressPlanStepObserver(ui64 fromCoordinator,
                                 ui64 toShard,
                                 TEventInjection injection)
            : Coordinator(fromCoordinator)
            , Shard(toShard)
            , InjectionUnderSuppress(injection)
            , Injected(false)
            , Suppress(true)
        {
        }

        void SetUp(TTestActorRuntime& runtime) {
            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                return this->OnEvent(static_cast<TTestActorRuntime&>(runtime), event);
            });
        }

        bool IsPlanStepMessage(TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() != TEvTxProcessing::EvPlanStep) {
                return false;
            }

            NKikimrTx::TEvMediatorPlanStep& record = event->Get<TEvTxProcessing::TEvPlanStep>()->Record;
            if (record.GetTabletID() != Shard)
                return false;

            for(auto &tx: record.GetTransactions()) {
                if (tx.GetCoordinator() == Coordinator)
                    return true;
            }

            return false;
        }

        TTestActorRuntime::EEventAction OnEvent(TTestActorRuntime& runtime, TAutoPtr<IEventHandle>& event) {
            if (!IsPlanStepMessage(event)) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            if (!Injected) {
                Cerr << "!Do injection!\n";
                Injected = true;
                InjectionUnderSuppress(runtime, Suppress);
                Cerr << "!Do injection DONE!\n";
            }

            if (Suppress) {
                Cerr << "!Suppress planStep event!\n";
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        }
    };

    Y_UNIT_TEST(CreateBlockStoreVolume) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();

        // Missing parameters
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", "", {NKikimrScheme::StatusSchemeError});

        vc.SetBlockSize(4096);

        // Creating volumes without partitions not allowed
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                   vdescr.DebugString(),
                                   {NKikimrScheme::StatusSchemeError});

        vc.AddPartitions()->SetBlockCount(16);

        // Specifying config version not allowed
        vc.SetVersion(123);

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                   vdescr.DebugString(),
                                   {NKikimrScheme::StatusSchemeError});

        vc.ClearVersion();

        // No channel profiles
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                   vdescr.DebugString(),
                                   {NKikimrScheme::StatusInvalidParameter});

        // Normal volume with 2 partitions
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.AddPartitions()->SetBlockCount(16);

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(3)});

        // Already exists
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                   vdescr.DebugString(),
                                   {NKikimrScheme::StatusAlreadyExists});
    }

    Y_UNIT_TEST(CreateBlockStoreVolumeWithVolumeChannelsProfiles) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();

        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        // Too few volume channel profiles
        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                   vdescr.DebugString(),
                                   {NKikimrScheme::StatusInvalidParameter});

        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});
    }

    Y_UNIT_TEST(CreateBlockStoreVolumeWithNonReplicatedPartitions) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();

        vc.SetBlockSize(4096);

        auto* partition = vc.AddPartitions();
        partition->SetBlockCount(16);
        partition->SetType(NKikimrBlockStore::EPartitionType::NonReplicated);

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(1)});
    }

    Y_UNIT_TEST(AlterBlockStoreVolume) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, /* nchannels */ 6);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();

        // Cannot alter missing volumes
        vc.AddPartitions()->SetBlockCount(32);
        vc.AddPartitions()->SetBlockCount(32);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusPathDoesNotExist});
        vc.Clear();

        // Create volume with 1 partition
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.SetDiskId("foo");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableExplicitChannelProfiles(0)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(1)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(2)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(3)->SetSize(1024);
        vc.MutableExplicitChannelProfiles(3)->SetReadIops(100);
        vc.MutableExplicitChannelProfiles(3)->SetReadBandwidth(100500);
        vc.MutableExplicitChannelProfiles(3)->SetWriteIops(200);
        vc.MutableExplicitChannelProfiles(3)->SetWriteBandwidth(200500);
        vc.MutableExplicitChannelProfiles(3)->SetDataKind(1);
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});

        // Alter it into 2 bigger partitions
        vc.SetVersion(1);
        vc.AddPartitions()->SetBlockCount(32);
        vc.AddPartitions()->SetBlockCount(32);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(3)});

        vc.SetVersion(2);

        // Alter with less partitions not allowed
        vc.AddPartitions()->SetBlockCount(32);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusInvalidParameter});

        // Changing block size not allowed
        vc.AddPartitions()->SetBlockCount(32);
        vc.SetBlockSize(8192);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusInvalidParameter});
        vc.ClearBlockSize();
        vc.ClearPartitions();

        // Setting media kind for the first time is allowed
        vc.SetStorageMediaKind(1);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        {
            NKikimrBlockStore::TVolumeConfig config;
            TestLs(runtime, "/MyRoot/BSVolume", false, NLs::ExtractVolumeConfig(&config));
            UNIT_ASSERT_VALUES_EQUAL(config.GetStorageMediaKind(), 1);
        }

        vc.SetVersion(3);

        // But changing media kind is not allowed
        vc.SetStorageMediaKind(2);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusInvalidParameter});
        vc.ClearStorageMediaKind();

        // Adding channel profiles is allowed
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableExplicitChannelProfiles(0)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(1)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(2)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(3)->SetSize(1024);
        vc.MutableExplicitChannelProfiles(3)->SetReadIops(100);
        vc.MutableExplicitChannelProfiles(3)->SetReadBandwidth(100500);
        vc.MutableExplicitChannelProfiles(3)->SetWriteIops(200);
        vc.MutableExplicitChannelProfiles(3)->SetWriteBandwidth(200500);
        vc.MutableExplicitChannelProfiles(3)->SetDataKind(1);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(4)->SetSize(2048);
        vc.MutableExplicitChannelProfiles(4)->SetReadIops(300);
        vc.MutableExplicitChannelProfiles(4)->SetReadBandwidth(300500);
        vc.MutableExplicitChannelProfiles(4)->SetWriteIops(400);
        vc.MutableExplicitChannelProfiles(4)->SetWriteBandwidth(400500);
        vc.MutableExplicitChannelProfiles(4)->SetDataKind(2);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(5)->SetSize(3072);
        vc.MutableExplicitChannelProfiles(5)->SetReadIops(500);
        vc.MutableExplicitChannelProfiles(5)->SetReadBandwidth(500500);
        vc.MutableExplicitChannelProfiles(5)->SetWriteIops(600);
        vc.MutableExplicitChannelProfiles(5)->SetWriteBandwidth(600500);
        vc.MutableExplicitChannelProfiles(5)->SetDataKind(2);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetVersion(4);

        // Deleting channel profiles is not allowed
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableExplicitChannelProfiles(0)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(1)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(2)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(3)->SetSize(1024);
        vc.MutableExplicitChannelProfiles(3)->SetReadIops(100);
        vc.MutableExplicitChannelProfiles(3)->SetReadBandwidth(100500);
        vc.MutableExplicitChannelProfiles(3)->SetWriteIops(200);
        vc.MutableExplicitChannelProfiles(3)->SetWriteBandwidth(200500);
        vc.MutableExplicitChannelProfiles(3)->SetDataKind(1);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(4)->SetSize(2048);
        vc.MutableExplicitChannelProfiles(4)->SetReadIops(300);
        vc.MutableExplicitChannelProfiles(4)->SetReadBandwidth(300500);
        vc.MutableExplicitChannelProfiles(4)->SetWriteIops(400);
        vc.MutableExplicitChannelProfiles(4)->SetWriteBandwidth(400500);
        vc.MutableExplicitChannelProfiles(4)->SetDataKind(2);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusInvalidParameter});
        vc.ClearExplicitChannelProfiles();

        // Number of volume channel explicit profiles must be equal to 3
        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusInvalidParameter});
        vc.ClearVolumeExplicitChannelProfiles();

        // Changing PoolKind is not allowed
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableExplicitChannelProfiles(0)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(1)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(2)->SetSize(128);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(3)->SetSize(1024);
        vc.MutableExplicitChannelProfiles(3)->SetReadIops(100);
        vc.MutableExplicitChannelProfiles(3)->SetReadBandwidth(100500);
        vc.MutableExplicitChannelProfiles(3)->SetWriteIops(200);
        vc.MutableExplicitChannelProfiles(3)->SetWriteBandwidth(200500);
        vc.MutableExplicitChannelProfiles(3)->SetDataKind(1);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(4)->SetSize(2048);
        vc.MutableExplicitChannelProfiles(4)->SetReadIops(300);
        vc.MutableExplicitChannelProfiles(4)->SetReadBandwidth(300500);
        vc.MutableExplicitChannelProfiles(4)->SetWriteIops(400);
        vc.MutableExplicitChannelProfiles(4)->SetWriteBandwidth(400500);
        vc.MutableExplicitChannelProfiles(4)->SetDataKind(2);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableExplicitChannelProfiles(5)->SetSize(3072);
        vc.MutableExplicitChannelProfiles(5)->SetReadIops(500);
        vc.MutableExplicitChannelProfiles(5)->SetReadBandwidth(500500);
        vc.MutableExplicitChannelProfiles(5)->SetWriteIops(600);
        vc.MutableExplicitChannelProfiles(5)->SetWriteBandwidth(600500);
        vc.MutableExplicitChannelProfiles(5)->SetDataKind(2);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusInvalidParameter});

        // Actually, it's allowed if you say a magic word
        vc.SetPoolKindChangeAllowed(true);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        vc.ClearPoolKindChangeAllowed();
        vc.ClearExplicitChannelProfiles();

        vc.SetVersion(5);

        // Modifying fields apart from PoolKind is allowed
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableExplicitChannelProfiles(0)->SetSize(129);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(1)->SetSize(129);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(2)->SetSize(129);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(3)->SetSize(1025);
        vc.MutableExplicitChannelProfiles(3)->SetReadIops(101);
        vc.MutableExplicitChannelProfiles(3)->SetReadBandwidth(100501);
        vc.MutableExplicitChannelProfiles(3)->SetWriteIops(201);
        vc.MutableExplicitChannelProfiles(3)->SetWriteBandwidth(200501);
        vc.MutableExplicitChannelProfiles(3)->SetDataKind(2);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");
        vc.MutableExplicitChannelProfiles(4)->SetSize(2049);
        vc.MutableExplicitChannelProfiles(4)->SetReadIops(301);
        vc.MutableExplicitChannelProfiles(4)->SetReadBandwidth(300501);
        vc.MutableExplicitChannelProfiles(4)->SetWriteIops(401);
        vc.MutableExplicitChannelProfiles(4)->SetWriteBandwidth(400501);
        vc.MutableExplicitChannelProfiles(4)->SetDataKind(1);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableExplicitChannelProfiles(5)->SetSize(3073);
        vc.MutableExplicitChannelProfiles(5)->SetReadIops(501);
        vc.MutableExplicitChannelProfiles(5)->SetReadBandwidth(500501);
        vc.MutableExplicitChannelProfiles(5)->SetWriteIops(601);
        vc.MutableExplicitChannelProfiles(5)->SetWriteBandwidth(600501);
        vc.MutableExplicitChannelProfiles(5)->SetDataKind(1);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetVersion(6);

        // Decreasing BlockCount is not allowed
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddPartitions()->SetBlockCount(32);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusInvalidParameter});
        vc.ClearPartitions();

        // Changing Opaque is allowed
        vc.SetOpaque("binary data");
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        {
            NKikimrBlockStore::TVolumeConfig config;
            TestLs(runtime, "/MyRoot/BSVolume", false, NLs::ExtractVolumeConfig(&config));
            UNIT_ASSERT_VALUES_EQUAL(config.GetDiskId(), "foo");
            UNIT_ASSERT_VALUES_EQUAL(config.GetOpaque(), "binary data");
        }

        vc.SetVersion(7);

        // Now change DiskId, FolderId and add some Tags, it shouldn't affect other values
        vc.SetDiskId("foobaz");
        vc.SetFolderId("baz");
        *vc.AddTags() = "tag1";
        *vc.AddTags() = "tag2";
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetVersion(8);

        // Changing Tags
        *vc.AddTags() = "tag1";
        *vc.AddTags() = "tag3";
        *vc.AddTags() = "tag4";
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        vc.SetVersion(9);
        // Set volume tablet channels

        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableVolumeExplicitChannelProfiles(0)->SetSize(128);
        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableVolumeExplicitChannelProfiles(1)->SetSize(128);
        vc.AddVolumeExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.MutableVolumeExplicitChannelProfiles(2)->SetSize(128);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        vc.SetVersion(10);
        // Changing volume tablet channels

        vc.MutableVolumeExplicitChannelProfiles(0)->SetPoolKind("pool-kind-2");
        vc.MutableVolumeExplicitChannelProfiles(1)->SetPoolKind("pool-kind-2");
        vc.MutableVolumeExplicitChannelProfiles(2)->SetPoolKind("pool-kind-2");
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        {
            NKikimrBlockStore::TVolumeConfig config;
            NLs::ExtractVolumeConfig(&config)(DescribePath(runtime, "/MyRoot/BSVolume"));
            UNIT_ASSERT_VALUES_EQUAL(config.GetBlockSize(), 4096u);
            UNIT_ASSERT_VALUES_EQUAL(config.PartitionsSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(config.GetDiskId(), "foobaz");
            UNIT_ASSERT_VALUES_EQUAL(config.GetFolderId(), "baz");
            UNIT_ASSERT_VALUES_EQUAL(config.GetOpaque(), "binary data");

            UNIT_ASSERT_VALUES_EQUAL(config.TagsSize(), 3);
            UNIT_ASSERT_VALUES_EQUAL(config.GetTags(0), "tag1");
            UNIT_ASSERT_VALUES_EQUAL(config.GetTags(1), "tag3");
            UNIT_ASSERT_VALUES_EQUAL(config.GetTags(2), "tag4");

            UNIT_ASSERT_VALUES_EQUAL(config.ExplicitChannelProfilesSize(), 6);

            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(0).GetPoolKind(), "pool-kind-1");
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(0).GetSize(), 129);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(0).GetReadIops(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(0).GetReadBandwidth(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(0).GetWriteIops(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(0).GetWriteBandwidth(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(0).GetDataKind(), 0);

            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(1).GetPoolKind(), "pool-kind-2");
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(1).GetSize(), 129);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(1).GetReadIops(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(1).GetReadBandwidth(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(1).GetWriteIops(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(1).GetWriteBandwidth(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(1).GetDataKind(), 0);

            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(2).GetPoolKind(), "pool-kind-2");
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(2).GetSize(), 129);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(2).GetReadIops(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(2).GetReadBandwidth(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(2).GetWriteIops(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(2).GetWriteBandwidth(), 0);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(2).GetDataKind(), 0);

            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(3).GetPoolKind(), "pool-kind-2");
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(3).GetSize(), 1025);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(3).GetReadIops(), 101);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(3).GetReadBandwidth(), 100501);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(3).GetWriteIops(), 201);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(3).GetWriteBandwidth(), 200501);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(3).GetDataKind(), 2);

            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(4).GetPoolKind(), "pool-kind-2");
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(4).GetSize(), 2049);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(4).GetReadIops(), 301);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(4).GetReadBandwidth(), 300501);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(4).GetWriteIops(), 401);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(4).GetWriteBandwidth(), 400501);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(4).GetDataKind(), 1);

            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(5).GetPoolKind(), "pool-kind-1");
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(5).GetSize(), 3073);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(5).GetReadIops(), 501);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(5).GetReadBandwidth(), 500501);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(5).GetWriteIops(), 601);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(5).GetWriteBandwidth(), 600501);
            UNIT_ASSERT_VALUES_EQUAL(config.GetExplicitChannelProfiles(5).GetDataKind(), 1);

            UNIT_ASSERT_VALUES_EQUAL(config.GetVolumeExplicitChannelProfiles(0).GetPoolKind(), "pool-kind-2");
            UNIT_ASSERT_VALUES_EQUAL(config.GetVolumeExplicitChannelProfiles(0).GetSize(), 128);

            UNIT_ASSERT_VALUES_EQUAL(config.GetVolumeExplicitChannelProfiles(1).GetPoolKind(), "pool-kind-2");
            UNIT_ASSERT_VALUES_EQUAL(config.GetVolumeExplicitChannelProfiles(1).GetSize(), 128);

            UNIT_ASSERT_VALUES_EQUAL(config.GetVolumeExplicitChannelProfiles(2).GetPoolKind(), "pool-kind-2");
            UNIT_ASSERT_VALUES_EQUAL(config.GetVolumeExplicitChannelProfiles(2).GetSize(), 128);
        }
    }

    Y_UNIT_TEST(AlterBlockStoreVolumeWithNonReplicatedPartitions) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();

        // Create volume with 1 partition
        vc.SetBlockSize(4096);
        {
            auto* partition = vc.AddPartitions();
            partition->SetBlockCount(16);
            partition->SetType(NKikimrBlockStore::EPartitionType::NonReplicated);
        }
        vc.SetDiskId("foo");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(1)});

        // Alter it into 2 bigger partitions
        vc.SetVersion(1);

        for (ui32 i = 0; i < 2; ++i) {
            auto* partition = vc.AddPartitions();
            partition->SetBlockCount(32);
            partition->SetType(NKikimrBlockStore::EPartitionType::NonReplicated);
        }
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(1)});

        vc.SetVersion(2);

        // Alter with less partitions not allowed
        {
            auto* partition = vc.AddPartitions();
            partition->SetBlockCount(32);
            partition->SetType(NKikimrBlockStore::EPartitionType::NonReplicated);
        }
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(DropBlockStoreVolume) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume", 0, {NKikimrScheme::StatusPathDoesNotExist});

        // Create volume with 1 partition
        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});

        // Drop the volume
        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished, NLs::PathsInsideDomain(0), NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(DropBlockStoreVolumeWithNonReplicatedPartitions) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();

        // Create volume with 1 partition
        vc.SetBlockSize(4096);
        {
            auto* partition = vc.AddPartitions();
            partition->SetBlockCount(16);
            partition->SetType(NKikimrBlockStore::EPartitionType::NonReplicated);
        }
        vc.SetDiskId("foo");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(1)});

        // Drop the volume
        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished, NLs::PathsInsideDomain(0), NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(DropBlockStoreVolume2) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Create volume with 1 partition
        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});

        // Drop the volume twice in parallel
        AsyncDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume");
        AsyncDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume");
        TestModificationResult(runtime, txId-1);

        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>();
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), txId);
        CheckExpectedStatus(
            { NKikimrScheme::StatusMultipleModifications },
            event->Record.GetStatus(), event->Record.GetReason());

        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetPathDropTxId(), txId-1);

        env.TestWaitNotification(runtime, txId-1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::PathNotExist});
    }

    Y_UNIT_TEST(DropBlockStoreVolumeWithFillGeneration) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto createVolume = [&](const TString& volumeName, bool isFillFinished = false) {
            // Create volume with fill generation 713
            NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
            vdescr.SetName(volumeName);
            auto& vc = *vdescr.MutableVolumeConfig();
            vc.SetBlockSize(4096);
            vc.AddPartitions()->SetBlockCount(16);
            vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
            vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
            vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
            vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
            vc.SetFillGeneration(713);
            vc.SetIsFillFinished(isFillFinished);

            TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/" + volumeName),
                               {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});
        };

        auto successfullyDropVolume = [&](const TString& volumeName, ui64 fillGeneration) {
            TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", volumeName, fillGeneration, {NKikimrScheme::StatusAccepted});
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/" + volumeName),
                               {NLs::PathNotExist});
            env.TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1});
            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                               {NLs::Finished, NLs::PathsInsideDomain(0), NLs::ShardsInsideDomain(0)});
        };

        auto failToDropVolume = [&](const TString& volumeName, ui64 fillGeneration) {
            TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", volumeName, fillGeneration, {NKikimrScheme::StatusSuccess});
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/" + volumeName),
                               {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});
        };

        createVolume("BSVolume");
        // Try to drop the volume using smaller fill generation
        failToDropVolume("BSVolume", 1);
        failToDropVolume("BSVolume", 712);
        // Drop the volume using equal fill generation
        successfullyDropVolume("BSVolume", 713);

        createVolume("BSVolume_2");
        // Drop the volume using greater fill generation
        successfullyDropVolume("BSVolume_2", 714);

        createVolume("BSVolume_3");
        // Drop the volume using greater fill generation
        successfullyDropVolume("BSVolume_3", 777);

        createVolume("BSVolume_4");
        // Drop the volume using zero fill generation (should be successful)
        successfullyDropVolume("BSVolume_4", 0);

        createVolume("BSVolume_5", true /* isFillFinished */);
        // Can't drop the volume if filling is finished.
        failToDropVolume("BSVolume_5", 100500);
    }

    Y_UNIT_TEST(AssignBlockStoreVolume) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Cannot assign non-existant volume
        TestAssignBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume", "Owner123", 0,
                {NKikimrScheme::StatusPathDoesNotExist});

        // Create volume with 1 partition
        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished});

        // Assign volume to Owner123
        TestAssignBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume", "Owner123");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::CheckMountToken("BSVolume", "Owner123")});

        // AssignVolume to Owner124
        TestAssignBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume", "Owner124");

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::CheckMountToken("BSVolume", "Owner124")});

        // AssignVolume using TokenVersion
        TestAssignBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume", "Owner125", 2);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::CheckMountToken("BSVolume", "Owner125")});

        // AssignVolume using wrong TokenVersion
        TestAssignBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume", "Owner126", 2,
            {NKikimrScheme::StatusPreconditionFailed});

        // Alter is allowed
        vc.SetVersion(1);
        vc.AddPartitions()->SetBlockCount(32);
        vc.AddPartitions()->SetBlockCount(32);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());

        // Drop is allowed
        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume");
    }

    Y_UNIT_TEST(AssignBlockStoreVolumeDuringAlter) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Create volume with 1 partition
        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished});

        vc.SetVersion(1);
        vc.AddPartitions()->SetBlockCount(32);
        vc.AddPartitions()->SetBlockCount(32);
        AsyncAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        AsyncAssignBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume", "Owner123");

        TestModificationResult(runtime, txId-1);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusSuccess);

        env.TestWaitNotification(runtime, {txId-1, txId});

        // Mount token should be set correctly
        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::CheckMountToken("BSVolume", "Owner123")});
    }

    Y_UNIT_TEST(AssignBlockStoreCheckVersionInAlter) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetVersion(1);
        vc.AddPartitions()->SetBlockCount(24);
        vc.AddPartitions()->SetBlockCount(24);

        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetVersion(0);
        vc.AddPartitions()->SetBlockCount(25);
        vc.AddPartitions()->SetBlockCount(25);

        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                       vdescr.DebugString(),
                       {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetVersion(1);
        vc.AddPartitions()->SetBlockCount(32);
        vc.AddPartitions()->SetBlockCount(32);

        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                       vdescr.DebugString(),
                       {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetVersion(2);
        vc.AddPartitions()->SetBlockCount(48);
        vc.AddPartitions()->SetBlockCount(48);

        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AssignBlockStoreCheckFillGenerationInAlter) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.SetFillGeneration(1);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetFillGeneration(1);
        vc.AddPartitions()->SetBlockCount(24);
        vc.AddPartitions()->SetBlockCount(24);

        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetFillGeneration(2);
        vc.AddPartitions()->SetBlockCount(25);
        vc.AddPartitions()->SetBlockCount(25);

        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                       vdescr.DebugString(),
                       {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        vc.SetFillGeneration(1);
        vc.AddPartitions()->SetBlockCount(48);
        vc.AddPartitions()->SetBlockCount(48);

        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(BlockStoreVolumeLimits) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestUserAttrs(runtime, ++txId, "", "MyRoot",
            AlterUserAttrs({{"__volume_space_limit", "131072"}})); /* 32 x 4096 */
        env.TestWaitNotification(runtime, txId);

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        // Use half of the quota initially
        vdescr.SetName("BSVolume1");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // Cannot have more than quota
        vdescr.SetName("BSVolume2");
        vc.MutablePartitions(0)->SetBlockCount(17);
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot",
                        vdescr.DebugString(),
                        {NKikimrScheme::StatusPreconditionFailed});

        // It's ok to use quota completely, but only the first create should succeed
        vc.SetSizeDecreaseAllowed(true);
        vc.MutablePartitions(0)->SetBlockCount(16);
        AsyncCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        AsyncCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        TestModificationResult(runtime, txId-1);
        TestModificationResult(runtime, txId, NKikimrScheme::StatusMultipleModifications);
        env.TestWaitNotification(runtime, {txId, txId-1});

        // Cannot increase volume size beyond current quota
        vc.Clear();
        vc.SetVersion(1);
        vc.AddPartitions()->SetBlockCount(17);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                        vdescr.DebugString(),
                        {NKikimrScheme::StatusPreconditionFailed});

        // It's ok to decrease volume size for volumes with the SizeDecreaseAllowed flag
        vc.MutablePartitions(0)->SetBlockCount(8);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // After successful alter we may use freed quota for more volumes
        vdescr.SetName("BSVolume3");
        vc.Clear();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(8);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // We may drop a volume and then use freed quota in an alter
        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume1");
        env.TestWaitNotification(runtime, txId);

        vdescr.SetName("BSVolume2");
        vc.Clear();
        vc.SetVersion(2);
        vc.AddPartitions()->SetBlockCount(24);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // Currently quota is full, cannot create even a single block volume
        vdescr.SetName("BSVolume4");
        vc.Clear();
        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(1);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot",
                        vdescr.DebugString(),
                        {NKikimrScheme::StatusPreconditionFailed});

        // It's possible to modify quota size
        TestUserAttrs(runtime, ++txId, "", "MyRoot",
            AlterUserAttrs({{"__volume_space_limit", "135168"}})); /* 33 x 4096 */
        env.TestWaitNotification(runtime, txId);

        // Now single block volume should succeed
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // Change limit to be specifically about SSD
        TestUserAttrs(runtime, ++txId, "", "MyRoot",
            AlterUserAttrs({{"__volume_space_limit_ssd", "147456"}}, {"__volume_space_limit"})); /* (32 + 4) x 4096 */
        env.TestWaitNotification(runtime, txId);

        // Now we should be able to create any size volumes with the default kind
        vdescr.SetName("BSVolume5");
        vc.MutablePartitions(0)->SetBlockCount(128);
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // But no more than 32 blocks for SSD volumes
        vdescr.SetName("BSVolume6");
        vc.SetStorageMediaKind(1);
        vc.SetSizeDecreaseAllowed(true);
        vc.MutablePartitions(0)->SetBlockCount(33);
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot",
                        vdescr.DebugString(),
                        {NKikimrScheme::StatusPreconditionFailed});
        vc.MutablePartitions(0)->SetBlockCount(32);
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            NLs::UserAttrsHas({
                {"__volume_space_allocated", "790528"},
                {"__volume_space_allocated_ssd", "147456"}})});

        // It's ok to decrease volume size
        vc.Clear();
        vc.SetVersion(1);
        vc.AddPartitions()->SetBlockCount(8);
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            NLs::UserAttrsHas({
                {"__volume_space_allocated", "692224"},
                {"__volume_space_allocated_ssd", "36864"}})});

        TestMkDir(runtime, txId++, "/MyRoot", "MyDir");
        env.TestWaitNotification(runtime, txId);

        // Now we can create a 24-block volume (in a subdirectory)
        vdescr.SetName("BSVolume7");
        vc.Clear();
        vc.SetBlockSize(4096);
        vc.SetStorageMediaKind(1);
        vc.AddPartitions()->SetBlockCount(24);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot/MyDir", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            NLs::UserAttrsHas({
                {"__volume_space_allocated", "790528"},
                {"__volume_space_allocated_ssd", "147456"}})});

        // We cannot create another volume in a subdirectory (due to root limits)
        vdescr.SetName("BSVolume8");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot/MyDir",
                        vdescr.DebugString(),
                        {NKikimrScheme::StatusPreconditionFailed});

        // Create a subdomain
        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "Coordinators: 0 "
                            "Mediators: 0 "
                            "Name: \"NBS\""
                            "StoragePools {"
                            "  Name: \"name_NBS_kind_hdd-1\""
                            "  Kind: \"pool-kind-1\""
                            "}"
                            "StoragePools {"
                            "  Name: \"name_NBS_kind_hdd-2\""
                            "  Kind: \"pool-kind-2\""
                            "}");
        env.TestWaitNotification(runtime, txId);

        // It's ok to create volume in a subdomain (separate limits)
        vdescr.SetName("BSVolume1");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot/NBS", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // Root allocated attributes must not change
        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            NLs::UserAttrsHas({
                {"__volume_space_allocated", "790528"},
                {"__volume_space_allocated_ssd", "147456"}})});

        // New volume is counted towards subdomain volume limits
        TestDescribeResult(DescribePath(runtime, "/MyRoot/NBS"), {
            NLs::UserAttrsHas({
                {"__volume_space_allocated", "98304"},
                {"__volume_space_allocated_ssd", "110592"}})});

        // Apply limits on a subdomain
        TestUserAttrs(runtime, ++txId, "/MyRoot", "NBS",
            AlterUserAttrs({{"__volume_space_limit_ssd", "221184"}})); /* (48 + 6) x 4096 */
        env.TestWaitNotification(runtime, txId);

        // We should be able to create another 24-block volume
        vdescr.SetName("BSVolume2");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot/NBS", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/NBS"), {
            NLs::UserAttrsHas({
                {"__volume_space_allocated", "196608"},
                {"__volume_space_allocated_ssd", "221184"}})});

        // We shouldn't be able to create more ssd volumes
        vdescr.SetName("BSVolume3");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot/NBS",
                        vdescr.DebugString(),
                        {NKikimrScheme::StatusPreconditionFailed});
    }

    Y_UNIT_TEST(BlockStoreNonreplVolumeLimits) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestUserAttrs(
            runtime,
            ++txId,
            "",
            "MyRoot",
            AlterUserAttrs({{
                "__volume_space_limit_ssd_nonrepl",
                ToString(32 * 4_KB)
            }})
        );
        env.TestWaitNotification(runtime, txId);

        // Other pool kinds should not be affected
        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetBlockSize(4_KB);
        vc.AddPartitions()->SetBlockCount(100500);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        vdescr.SetName("BSVolumeOther");
        vc.SetStorageMediaKind(1);
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // Creating a nonrepl volume
        vdescr.SetName("BSVolume1");
        vc.SetStorageMediaKind(4);
        vc.ClearExplicitChannelProfiles();
        vc.MutablePartitions(0)->SetType(
            NKikimrBlockStore::EPartitionType::NonReplicated
        );
        vc.MutablePartitions(0)->SetBlockCount(16);
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString()
        );
        env.TestWaitNotification(runtime, txId);

        // Cannot have more than quota
        vdescr.SetName("BSVolume2");
        vc.MutablePartitions(0)->SetBlockCount(17);
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString(),
            {NKikimrScheme::StatusPreconditionFailed}
        );

        // It's ok to use quota completely, but only the first create should succeed
        vdescr.SetName("BSVolume2");
        vc.MutablePartitions(0)->SetBlockCount(16);
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString()
        );
        env.TestWaitNotification(runtime, txId);

        // Cannot increase volume size beyond current quota
        vc.Clear();
        vc.AddPartitions()->SetType(
            NKikimrBlockStore::EPartitionType::NonReplicated
        );
        vc.MutablePartitions(0)->SetBlockCount(17);
        vc.SetVersion(1);
        TestAlterBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString(),
            {NKikimrScheme::StatusPreconditionFailed}
        );

        // We may drop a volume and then use freed quota in an alter
        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume1");
        env.TestWaitNotification(runtime, txId);

        vdescr.SetName("BSVolume2");
        vc.Clear();
        vc.AddPartitions()->SetType(
            NKikimrBlockStore::EPartitionType::NonReplicated
        );
        vc.MutablePartitions(0)->SetBlockCount(32);
        vc.SetVersion(1);
        TestAlterBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString()
        );
        env.TestWaitNotification(runtime, txId);

        // Cannot have more than quota
        vdescr.SetName("BSVolume3");
        vc.MutablePartitions(0)->SetBlockCount(1);
        vc.SetStorageMediaKind(4);
        vc.ClearVersion();
        vc.SetBlockSize(4_KB);
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString(),
            {NKikimrScheme::StatusPreconditionFailed}
        );

        // It's possible to modify quota size
        TestUserAttrs(
            runtime,
            ++txId,
            "",
            "MyRoot",
            AlterUserAttrs({{
                "__volume_space_limit_ssd_nonrepl",
                ToString(33 * 4_KB)
            }})
        );
        env.TestWaitNotification(runtime, txId);

        // Ok
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString()
        );
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(BlockStoreSystemVolumeLimits) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestUserAttrs(
            runtime,
            ++txId,
            "",
            "MyRoot",
            AlterUserAttrs({{
                "__volume_space_limit_ssd_system",
                ToString(32 * 4_KB)
            }})
        );
        env.TestWaitNotification(runtime, txId);

        // Other pool kinds should not be affected
        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        auto& vc = *vdescr.MutableVolumeConfig();
        vc.SetStorageMediaKind(1);
        vc.SetBlockSize(4_KB);
        vc.AddPartitions()->SetBlockCount(100500);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

        vdescr.SetName("BSVolumeOther");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        // Creating a SSD system volume
        vdescr.SetName("BSVolume1");
        vc.SetIsSystem(true);
        vc.MutablePartitions(0)->SetBlockCount(16);
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString()
        );
        env.TestWaitNotification(runtime, txId);

        // Cannot have more than quota
        vdescr.SetName("BSVolume2");
        vc.MutablePartitions(0)->SetBlockCount(17);
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString(),
            {NKikimrScheme::StatusPreconditionFailed}
        );
        env.TestWaitNotification(runtime, txId);

        // It's ok to use quota completely, but only the first create should succeed
        vdescr.SetName("BSVolume2");
        vc.MutablePartitions(0)->SetBlockCount(16);
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString()
        );
        env.TestWaitNotification(runtime, txId);

        // We may drop a volume and then use freed quota in an alter
        TestDropBlockStoreVolume(runtime, ++txId, "/MyRoot", "BSVolume1");
        env.TestWaitNotification(runtime, txId);

        vc.Clear();
        vdescr.SetName("BSVolume2");
        vc.AddPartitions()->SetBlockCount(32);
        vc.SetVersion(1);
        TestAlterBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString()
        );
        env.TestWaitNotification(runtime, txId);

        // Cannot have more than quota
        vdescr.SetName("BSVolume3");
        vc.SetIsSystem(true);
        vc.SetStorageMediaKind(1);
        vc.SetBlockSize(4_KB);
        vc.MutablePartitions(0)->SetBlockCount(1);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.ClearVersion();
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString(),
            {NKikimrScheme::StatusPreconditionFailed}
        );

        // It's possible to modify quota size
        TestUserAttrs(
            runtime,
            ++txId,
            "",
            "MyRoot",
            AlterUserAttrs({{
                "__volume_space_limit_ssd_system",
                ToString(33 * 4_KB)
            }})
        );
        env.TestWaitNotification(runtime, txId);

        // Ok
        TestCreateBlockStoreVolume(
            runtime,
            ++txId,
            "/MyRoot",
            vdescr.DebugString()
        );
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateAlterBlockStoreVolumeWithInvalidPoolKinds) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, /* nchannels */ 6);
        ui64 txId = 100;

        NKikimrSchemeOp::TBlockStoreVolumeDescription vdescr;
        vdescr.SetName("BSVolume");
        auto& vc = *vdescr.MutableVolumeConfig();

        vc.SetBlockSize(4096);
        vc.AddPartitions()->SetBlockCount(16);
        vc.SetDiskId("foo");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("invalid-pool-kind");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                   vdescr.DebugString(),
                                   {NKikimrScheme::StatusInvalidParameter});

        vc.MutableExplicitChannelProfiles(3)->SetPoolKind("pool-kind-1");
        TestCreateBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);
        vc.Clear();

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});

        vc.SetVersion(1);
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        vc.AddExplicitChannelProfiles()->SetPoolKind("invalid-pool-kind");
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot",
                                  vdescr.DebugString(),
                                  {NKikimrScheme::StatusInvalidParameter});

        vc.MutableExplicitChannelProfiles(3)->SetPoolKind("pool-kind-1");
        TestAlterBlockStoreVolume(runtime, ++txId, "/MyRoot", vdescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/BSVolume"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});
    }

    Y_UNIT_TEST(CreateDropKesus) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Create two kesus nodes
        TestCreateKesus(runtime, ++txId, "/MyRoot", "Name: \"Kesus1\"");
        TestCreateKesus(runtime, ++txId, "/MyRoot", "Name: \"Kesus2\"");
        env.TestWaitNotification(runtime, {txId, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(2), NLs::ShardsInsideDomain(2)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Kesus1"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Kesus2"),
                           {NLs::Finished});

        // Already exists
        TestCreateKesus(runtime, ++txId, "/MyRoot", "Name: \"Kesus1\"",
            {NKikimrScheme::StatusAlreadyExists});

        // Drop the first one
        TestDropKesus(runtime, ++txId, "/MyRoot", "Kesus1");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Kesus1"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Kesus2"),
                           {NLs::Finished});

        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets);
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(1)});

        // Drop the second one
        TestDropKesus(runtime, ++txId, "/MyRoot", "Kesus2");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Kesus2"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, TTestTxConfig::FakeHiveTablets + 1);
        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(0), NLs::ShardsInsideDomain(0)});
    }

    Y_UNIT_TEST(CreateAlterKesus) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateKesus(runtime, ++txId, "/MyRoot",
                        "Name: \"Kesus1\" "
                        "Config: { self_check_period_millis: 1234 session_grace_period_millis: 5678 }");
        env.TestWaitNotification(runtime, txId);


        auto checkKesusConfig = [=] (ui64 a, ui64 b) {
          return [=] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                const auto& config = record.GetPathDescription().GetKesus().GetConfig();
                UNIT_ASSERT_EQUAL(config.self_check_period_millis(), a);
                UNIT_ASSERT_EQUAL(config.session_grace_period_millis(), b);
            };
        };

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Kesus1"),
                           {NLs::Finished, checkKesusConfig(1234, 5678)});


        // Test the first setting is modified independently
        TestAlterKesus(runtime, ++txId, "/MyRoot",
                        "Name: \"Kesus1\" "
                        "Config: { self_check_period_millis: 2345 }");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Kesus1"),
                           {checkKesusConfig(2345, 5678)});

        // Test the second setting is modified independently
        TestAlterKesus(runtime, ++txId, "/MyRoot",
                        "Name: \"Kesus1\" "
                        "Config: { session_grace_period_millis: 6789 }");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Kesus1"),
                           {checkKesusConfig(2345, 6789)});
    }

    Y_UNIT_TEST(CreateDropSolomon) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                      "PartitionCount: 40 ");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Solomon"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(40)});

        // Already exists
        TestCreateSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                      "PartitionCount: 40 ",
            {NKikimrScheme::StatusAlreadyExists});

        TestDropSolomon(runtime, ++txId, "/MyRoot", "Solomon");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Solomon"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 40));

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished, NLs::PathsInsideDomain(0), NLs::ShardsInsideDomain(0)});
    }

    NKikimrSchemeOp::TCreateSolomonVolume
            GenerateAdoptedPartitions(ui32 count
                                      , ui64 ownerIdStart, ui64 ownerIdDelta
                                      , ui64 shardIdStart, ui64 shardIdDelta
                                      , ui64 tabletIdStart, ui64 tabletIdDelta)
    {
        NKikimrSchemeOp::TCreateSolomonVolume volume;


        ui64 ownerId = ownerIdStart;
        ui64 shardId = shardIdStart;
        ui64 tabletId = tabletIdStart;

        for (ui32 i = 0; i < count; ++i) {
            NKikimrSchemeOp::TCreateSolomonVolume::TAdoptedPartition* part = volume.AddAdoptedPartitions();
            part->SetOwnerId(ownerId);
            part->SetShardIdx(shardId);
            part->SetTabletId(tabletId);

            ownerId += ownerIdDelta;
            shardId += shardIdDelta;
            tabletId += tabletIdDelta;
        }

        return volume;
    }

    Y_UNIT_TEST(AdoptDropSolomon) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSolomon(runtime, ++txId, "/MyRoot", "Name: \"JunkSolomon\" "
                                                      "PartitionCount: 5 ");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathsInsideDomain(1),
                            NLs::ShardsInsideDomain(5)});

        NKikimrScheme::TEvDescribeSchemeResult ls = DescribePath(runtime, "/MyRoot/JunkSolomon");
        NLs::Finished(ls);

        auto volumeDescr = TakeTabletsFromAnotherSolomonVol("Solomon", ls.DebugString());

        TestCreateSolomon(runtime, ++txId, "/MyRoot", volumeDescr.DebugString());
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Solomon"),
                           {NLs::Finished,
                            NLs::PathsInsideDomain(2),
                            NLs::ShardsInsideDomain(10)});

        TestDropSolomon(runtime, ++txId, "/MyRoot", "Solomon");
        env.TestWaitNotification(runtime, txId);

        TestDropSolomon(runtime, ++txId, "/MyRoot", "JunkSolomon");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Solomon"),
                           {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/JunkSolomon"),
                           {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CreateAlterDropSolomon) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                      "PartitionCount: 2 ");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Solomon"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});

        TestAlterSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                     "PartitionCount: 4 "
                                                     "ChannelProfileId: 0 ");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Solomon"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(4)});

        TestDropSolomon(runtime, ++txId, "/MyRoot", "Solomon");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Solomon"),
                           {NLs::PathNotExist});

        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets + 4));

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished, NLs::PathsInsideDomain(0), NLs::ShardsInsideDomain(0)});
    }

    void UpdateChannelsBindingSolomon(bool allow) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().AllowUpdateChannelsBindingOfSolomonPartitions(allow));
        ui64 txId = 100;

        auto check = [&](const TString& path, ui64 shards, const TVector<THashMap<TString, ui32>>& expectedChannels) {
            NKikimrSchemeOp::TDescribeOptions opts;
            opts.SetReturnChannelsBinding(true);

            TestDescribeResult(DescribePath(runtime, path, opts), {
                NLs::Finished,
                NLs::ShardsInsideDomain(shards),
                [&expectedChannels] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                    const auto& desc = record.GetPathDescription().GetSolomonDescription();

                    UNIT_ASSERT_VALUES_EQUAL(expectedChannels.size(), desc.PartitionsSize());

                    for (size_t i = 0; i < desc.PartitionsSize(); ++i) {
                        const auto& partition = desc.GetPartitions(i);

                        THashMap<TString, ui32> channels;
                        for (const auto& channel : partition.GetBoundChannels()) {
                            auto it = channels.find(channel.GetStoragePoolName());
                            if (it == channels.end()) {
                                it = channels.emplace(channel.GetStoragePoolName(), 0).first;
                            }
                            ++it->second;
                        }

                        UNIT_ASSERT_VALUES_EQUAL(expectedChannels.at(i).size(), channels.size());

                        for (const auto& [name, count] : expectedChannels.at(i)) {
                            UNIT_ASSERT_C(channels.contains(name), "Cannot find channel: " << name);
                            UNIT_ASSERT_VALUES_EQUAL(channels.at(name), count);
                        }
                    }
                }
            });
        };

        TestCreateSolomon(runtime, ++txId, "/MyRoot", R"(
            Name: "Solomon"
            PartitionCount: 1
            ChannelProfileId: 2
        )");

        env.TestWaitNotification(runtime, txId);
        check("/MyRoot/Solomon", 1, {{{"pool-1", 4}}});

        // case 1: empty alter
        TestAlterSolomon(runtime, ++txId, "/MyRoot", R"(
            Name: "Solomon"
            ChannelProfileId: 3
        )", {NKikimrScheme::StatusInvalidParameter});

        // case 2: add partition, do not update channels binding
        TestAlterSolomon(runtime, ++txId, "/MyRoot", R"(
            Name: "Solomon"
            ChannelProfileId: 3
            PartitionCount: 2
        )");

        env.TestWaitNotification(runtime, txId);
        check("/MyRoot/Solomon", 2, {{{"pool-1", 4}}, {{"pool-2", 4}}});

        // case 3: just update channels binding
        TestAlterSolomon(runtime, ++txId, "/MyRoot", R"(
            Name: "Solomon"
            ChannelProfileId: 3
            UpdateChannelsBinding: true
        )", {allow ? NKikimrScheme::StatusAccepted : NKikimrScheme::StatusPreconditionFailed});

        if (!allow) {
            return;
        }

        env.TestWaitNotification(runtime, txId);
        check("/MyRoot/Solomon", 2, {{{"pool-2", 4}}, {{"pool-2", 4}}});

        // case 4: add partition & update channels binding
        TestAlterSolomon(runtime, ++txId, "/MyRoot", R"(
            Name: "Solomon"
            ChannelProfileId: 2
            PartitionCount: 3
            UpdateChannelsBinding: true
        )");

        env.TestWaitNotification(runtime, txId);
        check("/MyRoot/Solomon", 3, {{{"pool-1", 4}}, {{"pool-1", 4}}, {{"pool-1", 4}}});
    }

    Y_UNIT_TEST(UpdateChannelsBindingSolomonShouldNotUpdate) {
        UpdateChannelsBindingSolomon(false);
    }

    Y_UNIT_TEST(UpdateChannelsBindingSolomonShouldUpdate) {
        UpdateChannelsBindingSolomon(true);
    }

    Y_UNIT_TEST(RejectAlterSolomon) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                      "PartitionCount: 2 ");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Solomon"),
                           {NLs::Finished, NLs::PathsInsideDomain(1), NLs::ShardsInsideDomain(2)});

        TestAlterSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                     "PartitionCount: 100000000 "
                                                     "ChannelProfileId: 0 ",
                         { NKikimrScheme::StatusResourceExhausted });

        TestAlterSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                     "PartitionCount: 4 "
                                                     "ChannelProfileId: 100 ",
                         { NKikimrScheme::StatusInvalidParameter });

        TestAlterSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                     "PartitionCount: 4 ",
                         { NKikimrScheme::StatusInvalidParameter });

        TestAlterSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                     "PartitionCount: 2 "
                                                     "ChannelProfileId: 0 ",
                         { NKikimrScheme::StatusSuccess });

        TestAlterSolomon(runtime, ++txId, "/MyRoot", "Name: \"Solomon\" "
                                                     "PartitionCount: 1 "
                                                     "ChannelProfileId: 0 ",
                         { NKikimrScheme::StatusInvalidParameter });
    }


    Y_UNIT_TEST(CreateTableWithCompactionStrategies) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: [ "key" ]
            PartitionConfig {
                CompactionPolicy {
                    CompactionStrategy: CompactionStrategyGenerational
                }
            }
            )");
        env.TestWaitNotification(runtime, txId);
        TestDescribe(runtime, "/MyRoot/Table1");

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table2"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: [ "key" ]
            PartitionConfig {
                CompactionPolicy {
                    CompactionStrategy: CompactionStrategySharded
                }
            }
            )",
            { NKikimrScheme::StatusInvalidParameter });
    }

    Y_UNIT_TEST(AlterTableWithCompactionStrategies) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: [ "key" ]
            PartitionConfig {
                CompactionPolicy {
                }
            }
            )");
        env.TestWaitNotification(runtime, txId);
        TestDescribe(runtime, "/MyRoot/Table1");

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            PartitionConfig {
                CompactionPolicy {
                    CompactionStrategy: CompactionStrategyGenerational
                }
            }
            )");
        env.TestWaitNotification(runtime, txId);
        TestDescribe(runtime, "/MyRoot/Table1");

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            PartitionConfig {
                CompactionPolicy {
                    CompactionStrategy: CompactionStrategySharded
                }
            }
            )",
            { NKikimrScheme::StatusInvalidParameter });
    }

    Y_UNIT_TEST(SimultaneousDropForceDrop) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");

        env.TestWaitNotification(runtime, 101);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::PathExist});

        auto pathVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                                          {NLs::PathExist,
                                           NLs::PathVersionEqual(3)});

        AsyncDropTable(runtime, ++txId,  "/MyRoot", "Table1");
        AsyncForceDropUnsafe(runtime, ++txId,  pathVer.PathId.LocalPathId);
        env.TestWaitNotification(runtime, {txId, txId-1});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                           {NLs::PathNotExist});

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::NoChildren,
                            NLs::PathsInsideDomain(0),
                            NLs::ShardsInsideDomainOneOf({0, 1})});
    }

    Y_UNIT_TEST(CreateWithIntermediateDirs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA/DirB");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/DirB"),
                           {NLs::Finished});

        TestMkDir(runtime, ++txId, "/MyRoot/DirA", "DirB/DirC");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/DirB/DirC"),
                           {NLs::Finished});

        TestCreateTable(runtime, ++txId, "/MyRoot",
            R"( Name: "Tables/Table1"
                Columns { Name: "RowId" Type: "Uint64" }
                KeyColumnNames: ["RowId"]
            )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Tables"),
                          {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Tables/Table1"),
                          {NLs::Finished,
                           NLs::IsTable});

        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
            R"( Name: "PQ/Topic1"
                TotalGroupCount: 2
                PartitionPerTablet: 2
                PQTabletConfig: {PartitionConfig { LifetimeSeconds : 10}}
            )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQ"),
                           {NLs::Finished});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/PQ/Topic1"),
                           {NLs::Finished});

        TestMkDir(runtime, ++txId, "/MyRoot", "DirA/DirB", {NKikimrScheme::StatusAlreadyExists});
        TestMkDir(runtime, ++txId, "/MyRoot/DirC", "DirA/DirB", {NKikimrScheme::StatusPathDoesNotExist});
        TestMkDir(runtime, ++txId, "/MyRoot/DirA", "/DirD/DirE", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot/DirA", "DirD/DirE/", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot/Tables/Table1", "DirA/DirB", {NKikimrScheme::StatusPathIsNotDirectory});

        TestMkDir(runtime, ++txId, "/MyRoot", "x/y/z");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/x/y/z"),
                           {NLs::Finished,
                            NLs::CreatedAt(txId)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/x/y"),
                           {NLs::Finished,
                            NLs::CreatedAt(txId)});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/x"),
                           {NLs::Finished,
                            NLs::CreatedAt(txId)});

        TestRmDir(runtime, ++txId, "/MyRoot/x/y", "z");
        env.TestWaitNotification(runtime, txId);
        TestRmDir(runtime, ++txId, "/MyRoot/x", "y");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "x/y/z");
        env.TestWaitNotification(runtime, txId);

        TestRmDir(runtime, ++txId, "/MyRoot/x/y", "z");
        env.TestWaitNotification(runtime, txId);
        TestMkDir(runtime, ++txId, "/MyRoot", "x/y/z");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot",
            R"( Name: "WrongTables/Table1"
                Columns { Name: "RowId" Type: "Uint64" }
                KeyColumnNames: ["WrongRowId"]
            )", {NKikimrScheme::StatusSchemeError});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/WrongTables"),
                           {NLs::PathNotExist});
    }

    Y_UNIT_TEST(AlterTableAndAfterSplit) { //+
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 123;

        TestCreateSubDomain(runtime, ++txId,  "/MyRoot",
                            "PlanResolution: 50 "
                            "Coordinators: 1 "
                            "Mediators: 1 "
                            "TimeCastBucketsPerMediator: 2 "
                            "Name: \"USER_0\""
                            "StoragePools {"
                            "  Name: \"name_USER_0_kind_hdd-1\""
                            "  Kind: \"hdd-1\""
                            "}"
                            "StoragePools {"
                            "  Name: \"name_USER_0_kind_hdd-2\""
                            "  Kind: \"hdd-2\""
                            "}");

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            UniformPartitionsCount: 1
                            PartitionConfig {
                              CompactionPolicy {
                                InMemSizeToSnapshot: 4194304
                                InMemStepsToSnapshot: 300
                                InMemForceStepsToSnapshot: 500
                                InMemForceSizeToSnapshot: 16777216
                                InMemCompactionBrokerQueue: 0
                                ReadAheadHiThreshold: 67108864
                                ReadAheadLoThreshold: 16777216
                                MinDataPageSize: 7168
                                SnapBrokerQueue: 0
                                Generation {
                                  GenerationId: 0
                                  SizeToCompact: 0
                                  CountToCompact: 8
                                  ForceCountToCompact: 8
                                  ForceSizeToCompact: 134217728
                                  CompactionBrokerQueue: 1
                                  KeepInCache: true
                                }
                              }
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {
                                    PreferredPoolKind: "hdd-1"
                                  }
                                  Log {
                                    PreferredPoolKind: "hdd-1"
                                  }
                                  Data {
                                    PreferredPoolKind: "hdd-1"
                                  }
                                  External {
                                    PreferredPoolKind: "hdd-2"
                                  }
                                  ExternalThreshold: 524288
                                }
                              }
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {
                                    PreferredPoolKind: "hdd-1"
                                  }
                                  Log {
                                    PreferredPoolKind: "hdd-1"
                                  }
                                  Data {
                                    PreferredPoolKind: "hdd-1"
                                  }
                                  External {
                                    PreferredPoolKind: "hdd-2"
                                  }
                                  ExternalThreshold: 524288
                                }
                              }
                            }
                        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table"
                            Columns { Name: "key"        Type: "Uint32"}
                            Columns { Name: "Value"      Type: "Utf8"}
                            KeyColumnNames: ["key"]
                            UniformPartitionsCount: 1
                            PartitionConfig {
                              CompactionPolicy {
                                InMemSizeToSnapshot: 4194304
                                InMemStepsToSnapshot: 300
                                InMemForceStepsToSnapshot: 500
                                InMemForceSizeToSnapshot: 16777216
                                InMemCompactionBrokerQueue: 0
                                ReadAheadHiThreshold: 67108864
                                ReadAheadLoThreshold: 16777216
                                MinDataPageSize: 7168
                                SnapBrokerQueue: 0
                                Generation {
                                  GenerationId: 0
                                  SizeToCompact: 0
                                  CountToCompact: 8
                                  ForceCountToCompact: 8
                                  ForceSizeToCompact: 134217728
                                  CompactionBrokerQueue: 1
                                  KeepInCache: true
                                }
                              }
                              ColumnFamilies {
                                Id: 0
                                ColumnCodec: ColumnCodecPlain
                                ColumnCache: ColumnCacheNone
                                StorageConfig {
                                  SysLog {
                                    PreferredPoolKind: "hdd-1"
                                  }
                                  Log {
                                    PreferredPoolKind: "hdd-1"
                                  }
                                  Data {
                                    PreferredPoolKind: "hdd-1"
                                  }
                                  External {
                                    PreferredPoolKind: "hdd-2"
                                  }
                                  ExternalThreshold: 524288
                                }
                              }
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table"
                            PartitionConfig {
                             ColumnFamilies {
                               Id: 0
                               ColumnCodec: ColumnCodecLZ4
                               ColumnCache: ColumnCacheNone
                               StorageConfig {
                                 SysLog {
                                   PreferredPoolKind: "hdd-1"
                                 }
                                 Log {
                                   PreferredPoolKind: "hdd-1"
                                 }
                                 Data {
                                   PreferredPoolKind: "hdd-1"
                                 }
                                 External {
                                   PreferredPoolKind: "hdd-2"
                                 }
                                 ExternalThreshold: 5204288
                               }
                             }
                            }
                        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/Table", true, true),
                           {[] (auto describeRes) {
                               auto& partConfig = describeRes.GetPathDescription().GetTable().GetPartitionConfig();
                               UNIT_ASSERT_VALUES_EQUAL_C(partConfig.ColumnFamiliesSize(), 1, "ColumnFamilies is uniq");
                               UNIT_ASSERT_VALUES_EQUAL_C(partConfig.GetColumnFamilies(0).GetStorageConfig().GetExternalThreshold(), 5204288, "ExternalThreshold is altered");
                               UNIT_ASSERT_VALUES_EQUAL_C((int)partConfig.GetColumnFamilies(0).GetColumnCodec(), (int)NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4, "ColumnCodec is altered");
                           }});

        TestSplitTable(runtime, ++txId, "/MyRoot/USER_0/Table", R"(
                                SourceTabletId: 72075186233409548
                                SplitBoundary {
                                    KeyPrefix {
                                        Tuple { Optional { Uint32: 1000 } }
                                    }
                                })");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table"
                            PartitionConfig {
                             ColumnFamilies {
                               Id: 0
                               ColumnCodec: ColumnCodecLZ4
                               ColumnCache: ColumnCacheEver
                               StorageConfig {
                                 SysLog {
                                   PreferredPoolKind: "hdd-1"
                                 }
                                 Log {
                                   PreferredPoolKind: "hdd-1"
                                 }
                                 Data {
                                   PreferredPoolKind: "hdd-1"
                                 }
                                 External {
                                   PreferredPoolKind: "hdd-2"
                                 }
                                 ExternalThreshold: 5604288
                               }
                             }
                            })");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/Table", true, true),
                           {[] (auto describeRes) {
                               auto& partConfig = describeRes.GetPathDescription().GetTable().GetPartitionConfig();
                               UNIT_ASSERT_VALUES_EQUAL_C(partConfig.ColumnFamiliesSize(), 1, "ColumnFamilies is uniq");
                               UNIT_ASSERT_VALUES_EQUAL_C(partConfig.GetColumnFamilies(0).GetStorageConfig().GetExternalThreshold(), 5604288, "ExternalThreshold is altered");
                               UNIT_ASSERT_VALUES_EQUAL_C((int)partConfig.GetColumnFamilies(0).GetColumnCache(), (int)NKikimrSchemeOp::EColumnCache::ColumnCacheEver, "ColumnCache is altered");
                           }});

        TestAlterTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table"
                            PartitionConfig {
                             ColumnFamilies {
                               Id: 0
                               ColumnCodec: ColumnCodecLZ4
                               ColumnCache: ColumnCacheEver
                               StorageConfig {
                                 ExternalThreshold: 5604289
                               }
                             }
                            })");
        env.TestWaitNotification(runtime, txId);


        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0/Table", true, true),
                           {[] (auto describeRes) {
                               auto& partConfig = describeRes.GetPathDescription().GetTable().GetPartitionConfig();
                               UNIT_ASSERT_VALUES_EQUAL_C(partConfig.ColumnFamiliesSize(), 1, "ColumnFamilies is uniq");
                               UNIT_ASSERT_VALUES_EQUAL_C(partConfig.GetColumnFamilies(0).GetStorageConfig().GetExternalThreshold(), 5604289, "ExternalThreshold is altered");
                               UNIT_ASSERT_VALUES_EQUAL_C((int)partConfig.GetColumnFamilies(0).GetColumnCache(), (int)NKikimrSchemeOp::EColumnCache::ColumnCacheEver, "ColumnCache is altered");
                               const auto& storageConfig = partConfig.GetColumnFamilies(0).GetStorageConfig();
                               UNIT_ASSERT_VALUES_EQUAL_C(storageConfig.GetSysLog().GetPreferredPoolKind(), "hdd-1", "SysLog pool kind must not be lost");
                               UNIT_ASSERT_VALUES_EQUAL_C(storageConfig.GetLog().GetPreferredPoolKind(), "hdd-1", "Log pool kind must not be lost");
                               UNIT_ASSERT_VALUES_EQUAL_C(storageConfig.GetData().GetPreferredPoolKind(), "hdd-1", "Data pool kind must not be lost");
                               UNIT_ASSERT_VALUES_EQUAL_C(storageConfig.GetExternal().GetPreferredPoolKind(), "hdd-2", "External pool kind must not be lost");
                           }});

        TestAlterTable(runtime, ++txId, "/MyRoot/USER_0", R"(
                            Name: "Table"
                            PartitionConfig {
                             ColumnFamilies {
                               Id: 0
                               ColumnCodec: ColumnCodecLZ4
                               ColumnCache: ColumnCacheEver
                               StorageConfig {
                                   SysLog {
                                     PreferredPoolKind: "hdd-1"
                                   }
                                   Log {
                                     PreferredPoolKind: "hdd-1"
                                   }
                                   Data {
                                     PreferredPoolKind: "hdd-1"
                                   }
                                   External {
                                     PreferredPoolKind: "hdd-2"
                                   }
                                   ExternalThreshold: 5604288
                               }
                             }
                             StorageRooms {}
                            }
                        )", {NKikimrScheme::StatusInvalidParameter});

        // Drop the table and wait for every TFamilyDescription to be cleaned up
        TestDropTable(runtime, ++txId, "/MyRoot/USER_0", "Table");
        env.TestWaitNotification(runtime, txId);
        env.TestWaitTabletDeletion(runtime, xrange(TTestTxConfig::FakeHiveTablets+2, TTestTxConfig::FakeHiveTablets+10));
    }

    Y_UNIT_TEST(RejectSystemViewPath) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, 4, true, &CreateFlatTxSchemeShard, true);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \".sys\""
                            "Columns { Name: \"key\" Type: \"Uint32\"}"
                            "KeyColumnNames: [\"key\"]",
                        {NKikimrScheme::StatusSchemeError});

        TestMkDir(runtime, ++txId, "/MyRoot", ".sys", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot", ".sys/partition_stats", {NKikimrScheme::StatusSchemeError});
        TestMkDir(runtime, ++txId, "/MyRoot", "DirA/.sys/partition_stats", {NKikimrScheme::StatusSchemeError});
    }

    Y_UNIT_TEST(DocumentApiVersion) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // Document api version must be a number
        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\" "
                        "Columns { Name: \"Key\" Type: \"Uint64\"} "
                        "Columns { Name: \"Value\" Type: \"Uint64\"} "
                        "KeyColumnNames: [\"Key\"]",
                        {NKikimrScheme::StatusInvalidParameter},
                        AlterUserAttrs({{"__document_api_version", "foo"}}));

        // Document api version cannot be zero
        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\" "
                        "Columns { Name: \"Key\" Type: \"Uint64\"} "
                        "Columns { Name: \"Value\" Type: \"Uint64\"} "
                        "KeyColumnNames: [\"Key\"]",
                        {NKikimrScheme::StatusInvalidParameter},
                        AlterUserAttrs({{"__document_api_version", "0"}}));

        // Document api version 1 is ok
        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"Table\" "
                        "Columns { Name: \"Key\" Type: \"Uint64\"} "
                        "Columns { Name: \"Value\" Type: \"Uint64\"} "
                        "KeyColumnNames: [\"Key\"]",
                        {NKikimrScheme::StatusAccepted},
                        AlterUserAttrs({{"__document_api_version", "1"}}));
        env.TestWaitNotification(runtime, txId);

        // Changing document api version is not allowed
        TestUserAttrs(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusInvalidParameter},
            AlterUserAttrs({{"__document_api_version", "2"}}), { });

        // Removing document api version is not allowed
        TestUserAttrs(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusInvalidParameter},
            AlterUserAttrs({{"__document_api_version", ""}}), { });

        // Create a new table without an attribute
        TestCreateTable(runtime, ++txId, "/MyRoot",
                        "Name: \"TableWoDocumentApi\" "
                        "Columns { Name: \"Key\" Type: \"Uint64\"} "
                        "Columns { Name: \"Value\" Type: \"Uint64\"} "
                        "KeyColumnNames: [\"Key\"]");
        env.TestWaitNotification(runtime, txId);

        // Adding document api version is not allowed
        TestUserAttrs(runtime, ++txId, "/MyRoot", "TableWoDocumentApi", {NKikimrScheme::StatusInvalidParameter},
            AlterUserAttrs({{"__document_api_version", "1"}}), { });

        // Creating other objects (e.g. directories) with document api version is not allowed
        TestMkDir(runtime, ++txId, "/MyRoot", "DirA",
            {NKikimrScheme::StatusInvalidParameter},
            AlterUserAttrs({{"__document_api_version", "1"}}));
    }


    class TSchemaHelper {
    private:
        NScheme::TTypeRegistry TypeRegistry;
        const TVector<NKikimr::NScheme::TTypeInfo> KeyColumnTypes;

    public:
        explicit TSchemaHelper(const TArrayRef<NKikimr::NScheme::TTypeInfo>& keyColumnTypes)
            : KeyColumnTypes(keyColumnTypes.begin(), keyColumnTypes.end())
        {}

        TString FindSplitKey(const TVector<TVector<TString>>& histogramKeys, TVector<ui64> histogramValues = {}, ui64 total = 0) const {
            if (histogramValues.empty() && !histogramKeys.empty()) {
                for (size_t i = 0; i < histogramKeys.size(); i++) {
                    histogramValues.push_back(i + 1);
                }
                total = histogramKeys.size() + 1;
            }

            NKikimrTableStats::THistogram histogram = FillHistogram(histogramKeys, histogramValues);
            TSerializedCellVec splitKey = ChooseSplitKeyByHistogram(histogram, total, KeyColumnTypes);
            return PrintKey(splitKey);
        }

    private:
        NKikimr::TSerializedCellVec MakeCells(const TVector<TString>& tuple) const {
            UNIT_ASSERT(tuple.size() <= KeyColumnTypes.size());
            TSmallVec<NKikimr::TCell> cells;

            for (size_t i = 0; i < tuple.size(); ++i) {
                if (tuple[i] == "NULL") {
                    cells.push_back(NKikimr::TCell());
                } else {
                    switch (KeyColumnTypes[i].GetTypeId()) {
#define ADD_CELL_FROM_STRING(ydbType, cppType) \
                    case NKikimr::NScheme::NTypeIds::ydbType: { \
                        cppType val = FromString<cppType>(tuple[i]); \
                        cells.push_back(NKikimr::TCell((const char*)&val, sizeof(val))); \
                        break; \
                    }

                    ADD_CELL_FROM_STRING(Bool, bool);

                    ADD_CELL_FROM_STRING(Uint8, ui8);
                    ADD_CELL_FROM_STRING(Int8, i8);
                    ADD_CELL_FROM_STRING(Uint16, ui16);
                    ADD_CELL_FROM_STRING(Int16, i16);
                    ADD_CELL_FROM_STRING(Uint32, ui32);
                    ADD_CELL_FROM_STRING(Int32, i32);
                    ADD_CELL_FROM_STRING(Uint64, ui64);
                    ADD_CELL_FROM_STRING(Int64, i64);

                    ADD_CELL_FROM_STRING(Double, double);
                    ADD_CELL_FROM_STRING(Float, float);

                    case NKikimr::NScheme::NTypeIds::String:
                    case NKikimr::NScheme::NTypeIds::Utf8: {
                        cells.push_back(NKikimr::TCell(tuple[i].data(), tuple[i].size()));
                        break;
                    }
#undef ADD_CELL_FROM_STRING
                    default:
                        UNIT_ASSERT_C(false, "Unexpected type");
                    }
                }
            }

            return NKikimr::TSerializedCellVec(cells);
        }

        NKikimrTableStats::THistogram FillHistogram(const TVector<TVector<TString>>& keys, const TVector<ui64>& values) const {
            NKikimrTableStats::THistogram histogram;
            for (auto i : xrange(keys.size())) {
                TSerializedCellVec sk(MakeCells(keys[i]));
                auto bucket = histogram.AddBuckets();
                bucket->SetKey(sk.GetBuffer());
                bucket->SetValue(values[i]);
            }
            return histogram;
        }

        TString PrintKey(const TSerializedCellVec& key) const {
            return PrintKey(key.GetCells());
        }

        TString PrintKey(const TConstArrayRef<TCell>& cells) const {
            return DbgPrintTuple(TDbTupleRef(KeyColumnTypes.data(), cells.data(), cells.size()), TypeRegistry);
        }
    };

    Y_UNIT_TEST(SplitKey) {
        TSmallVec<NScheme::TTypeInfo> keyColumnTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TSchemaHelper schemaHelper(keyColumnTypes);

        {
            TString splitKey = schemaHelper.FindSplitKey({
                                                  { "1", "aaaaaaaaaaaaaaaaaaaaaa", "42" },
                                                  { "3", "bbbbbbbb", "42" },
                                                  { "5", "cccccccccccccccccccccccc", "42" }
                                              });
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 3, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "1", "aaaaaaaaaaaaaaaaaaaaaa", "42" },
                                                  { "1", "bbbbbbbb", "42" },
                                                  { "1", "cccccccccccccccccccccccc", "42" }
                                              });
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 1, Utf8 : bbbbbbbb, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "1", "aaaaaaaaaaaaaaaaaaaaaa", "42" },
                                                  { "1", "bb", "42" },
                                                  { "1", "cc", "42" },
                                                  { "2", "cd", "42" },
                                                  { "2", "d", "42" },
                                                  { "2", "e", "42" },
                                                  { "2", "f", "42" },
                                                  { "2", "g", "42" }
                                              });
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 2, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "1", "aaaaaaaaaaaaaaaaaaaaaa", "42" },
                                                  { "1", "bb", "42" },
                                                  { "1", "cc", "42" },
                                                  { "1", "cd", "42" },
                                                  { "1", "d", "42" },
                                                  { "2", "e", "42" },
                                                  { "2", "f", "42" },
                                                  { "2", "g", "42" }
                                              });
            //TODO: FIX this case
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 2, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "1", "aaaaaaaaaaaaaaaaaaaaaa", "42" },
                                                  { "1", "bb", "42" },
                                                  { "1", "cc", "42" },
                                                  { "1", "cd", "42" },
                                                  { "1", "d", "42" },
                                                  { "3", "e", "42" },
                                                  { "3", "f", "42" },
                                                  { "3", "g", "42" }
                                              });
            //TODO: FIX this case
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 2, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "1", "aaaaaaaaaaaaaaaaaaaaaa", "42" },
                                                  { "1", "bb", "42" },
                                                  { "1", "cc", "42" },
                                                  { "1", "cd", "42" },
                                                  { "2", "d", "42" },
                                                  { "3", "e", "42" },
                                                  { "3", "f", "42" },
                                                  { "3", "g", "42" }
                                              });
            //TODO: FIX this case
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 2, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "1", "aaaaaaaaaaaaaaaaaaaaaa", "42" },
                                                  { "2", "a", "42" },
                                                  { "2", "b", "42" },
                                                  { "2", "c", "42" },
                                                  { "2", "d", "42" },
                                                  { "2", "e", "42" },
                                                  { "2", "f", "42" },
                                                  { "3", "cccccccccccccccccccccccc", "42" }
                                              });
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 2, Utf8 : c, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "2", "aaa", "1" },
                                                  { "2", "aaa", "2" },
                                                  { "2", "aaa", "3" },
                                                  { "2", "aaa", "4" },
                                                  { "2", "aaa", "5" },
                                                  { "2", "bbb", "1" },
                                                  { "2", "bbb", "2" },
                                                  { "3", "ccc", "42" }
                                              });
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 2, Utf8 : bbb, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({});
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "()");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "0", "a", "1" },
                                              }, {
                                                  53,
                                              }, 100);
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 0, Utf8 : a, Uint32 : 1)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "0", "a", "1" },
                                              }, {
                                                  25,
                                              }, 100);
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 0, Utf8 : a, Uint32 : 1)");
        }
        
        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "0", "a", "1" },
                                              }, {
                                                  75,
                                              }, 100);
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 0, Utf8 : a, Uint32 : 1)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "0", "a", "1" },
                                              }, {
                                                  24,
                                              }, 100);
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "()");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "0", "a", "1" },
                                              }, {
                                                  76,
                                              }, 100);
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "()");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "0", "a", "1" },
                                                  { "1", "a", "1" },
                                                  { "2", "a", "2" },
                                                  { "3", "a", "3" },
                                                  { "4", "a", "4" },
                                                  { "5", "a", "5" },
                                                  { "6", "a", "1" },
                                                  { "7", "a", "2" },
                                                  { "8", "a", "42" },
                                              }, {
                                                  1,
                                                  2,
                                                  3,
                                                  4,
                                                  5,
                                                  6,
                                                  7,
                                                  8,
                                                  9
                                              }, 10);
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 4, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "0", "a", "1" },
                                                  { "1", "a", "1" },
                                                  { "2", "a", "2" },
                                                  { "3", "a", "3" },
                                                  { "4", "a", "4" },
                                                  { "5", "a", "5" },
                                                  { "6", "a", "1" },
                                                  { "7", "a", "2" },
                                                  { "8", "a", "42" },
                                              }, {
                                                  1,
                                                  2,
                                                  3,
                                                  4,
                                                  5,
                                                  6,
                                                  30,
                                                  40,
                                                  70
                                              }, 100);
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 7, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            TString splitKey =
                    schemaHelper.FindSplitKey({
                                                  { "0", "a", "1" },
                                                  { "1", "a", "1" },
                                                  { "2", "a", "2" },
                                                  { "3", "a", "3" },
                                                  { "4", "a", "4" },
                                                  { "5", "a", "5" },
                                                  { "6", "a", "1" },
                                                  { "7", "a", "2" },
                                                  { "8", "a", "42" },
                                              }, {
                                                  30,
                                                  40,
                                                  70,
                                                  90,
                                                  91,
                                                  92,
                                                  93,
                                                  94,
                                                  95
                                              }, 100);
            UNIT_ASSERT_VALUES_EQUAL(splitKey, "(Uint64 : 1, Utf8 : NULL, Uint32 : NULL)");
        }
    }

    Y_UNIT_TEST(ListNotCreatedDirCase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TVector<THolder<IEventHandle>> suppressed;
        auto defObserver = SetSuppressObserver(runtime, suppressed, TEvTxProcessing::EvPlanStep);
        TestMkDir(runtime, ++txId, "/MyRoot", "Dir");
        WaitForSuppressed(runtime, suppressed, 1, defObserver);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir"),
                           {NLs::NotFinished,
                           NLs::PathVersionEqual(2),
                           NLs::ChildrenCount(0)});

        for (auto &msg : suppressed) {
            runtime.Send(msg.Release());
        }
        suppressed.clear();

        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir"),
                           {NLs::Finished,
                           NLs::PathVersionEqual(3),
                           NLs::ChildrenCount(0)});
    }

    Y_UNIT_TEST(ListNotCreatedIndexCase) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TVector<THolder<IEventHandle>> suppressed;
        auto defObserver = SetSuppressObserver(runtime, suppressed, TEvTxProcessing::EvPlanStep);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Dir/Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "UserDefinedIndex"
              KeyColumnNames: ["indexed"]
              Type: EIndexTypeGlobalAsync
            }
        )");
        WaitForSuppressed(runtime, suppressed, 1, defObserver);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished,
                           NLs::ChildrenCount(1),
                           NLs::ShardsInsideDomain(2),
                           NLs::PathsInsideDomain(4)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir"),
                           {NLs::NotFinished,
                           NLs::PathVersionEqual(3),
                           NLs::ChildrenCount(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir/Table"),
                           {NLs::NotFinished,
                           NLs::PathVersionEqual(1),
                           NLs::ChildrenCount(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir/Table/UserDefinedIndex", true, true, true),
                           {NLs::NotFinished,
                           NLs::PathVersionEqual(1),
                           NLs::ChildrenCount(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir/Table/UserDefinedIndex/indexImplTable", true, true, true),
                           {NLs::NotFinished,
                           NLs::PathVersionEqual(1),
                           NLs::ChildrenCount(0)});

        for (auto &msg : suppressed) {
            runtime.Send(msg.Release());
        }
        suppressed.clear();

        env.TestWaitNotification(runtime, txId);


        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::Finished,
                           NLs::ChildrenCount(1),
                           NLs::ShardsInsideDomain(2),
                           NLs::PathsInsideDomain(4)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir"),
                           {NLs::Finished,
                           NLs::PathVersionEqual(5),
                           NLs::ChildrenCount(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir/Table"),
                           {NLs::Finished,
                           NLs::PathVersionEqual(3),
                           NLs::ChildrenCount(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir/Table/UserDefinedIndex", true, true, true),
                           {NLs::Finished,
                           NLs::PathVersionEqual(2),
                           NLs::ChildrenCount(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir/Table/UserDefinedIndex/indexImplTable", true, true, true),
                           {NLs::Finished,
                           NLs::PathVersionEqual(3),
                           NLs::ChildrenCount(0)});
    }

    Y_UNIT_TEST(ConsistentCopyAfterDropIndexes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table1"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        {
            auto fnWriteRow = [&] (ui64 tabletId, ui32 key) {
                TString writeQuery = Sprintf(R"(
                    (
                        (let key '( '('key (Uint64 '%u)) ) )
                        (let value '('('value (Utf8 'aaaaaaaa)) ) )
                        (return (AsList (UpdateRow '__user__Table1 key value) ))
                    )
                )", key);
                NKikimrMiniKQL::TResult result;
                TString err;
                ui32 res = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                UNIT_ASSERT_VALUES_EQUAL(err, "");
                UNIT_ASSERT_VALUES_EQUAL(res, 0);
            };
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 0);
        }

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table1", "Sync", {"value"});
        env.TestWaitNotification(runtime, txId, TTestTxConfig::SchemeShard);

        auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", txId);
        Y_ASSERT(descr.GetIndexBuild().GetState() == Ydb::Table::IndexBuildState::STATE_DONE);

        TestCopyTable(runtime, ++txId, "/MyRoot", "Copy1", "/MyRoot/Table1");
        env.TestWaitNotification(runtime, txId);

        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table1"
            IndexName: "Sync"
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::ChildrenCount(2),
                            NLs::PathsInsideDomain(4),
                            NLs::ShardsInsideDomain(4)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                           {NLs::IsTable,
                            NLs::PathVersionEqual(8),
                            NLs::CheckColumns("Table1", {"key", "value"}, {}, {"key"}),
                            NLs::IndexesCount(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy1"),
                           {NLs::IsTable,
                            NLs::PathVersionEqual(3),
                            NLs::CheckColumns("Copy1", {"key", "value"}, {}, {"key"}),
                            NLs::IndexesCount(1)});

        TestConsistentCopyTables(runtime, ++txId, "/", R"(
                       CopyTableDescriptions {
                         SrcPath: "/MyRoot/Table1"
                         DstPath: "/MyRoot/Copy2"
                       }
                      CopyTableDescriptions {
                        SrcPath: "/MyRoot/Copy1"
                        DstPath: "/MyRoot/Copy3"
                      }
         )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                           {NLs::ChildrenCount(4),
                            NLs::PathsInsideDomain(8),
                            NLs::ShardsInsideDomain(7)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table1"),
                           {NLs::IsTable,
                            NLs::PathVersionEqual(8),
                            NLs::IndexesCount(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy1"),
                           {NLs::IsTable,
                            NLs::PathVersionEqual(3),
                            NLs::IndexesCount(1)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy2"),
                           {NLs::IsTable,
                            NLs::PathVersionEqual(3),
                            NLs::IndexesCount(0)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Copy3"),
                           {NLs::IsTable,
                            NLs::PathVersionEqual(3),
                            NLs::IndexesCount(1)});
    }

    Y_UNIT_TEST(SplitAlterCopy) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "key"   Type: "Uint64" }
              Columns { Name: "value" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        {
            TVector<THolder<IEventHandle>> suppressed;
            auto defObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::EvInitSplitMergeDestination);

            ++txId;
            TestSplitTable(runtime, 103, "/MyRoot/Table", R"(
                                    SourceTabletId: 72075186233409546
                                    SplitBoundary {
                                        KeyPrefix {
                                            Tuple { Optional { Uint64: 1000 } }
                                        }
                                    })");
            ++txId;
            TestAlterTable(runtime, 102, "/MyRoot",
                    R"(Name: "Table"
                        Columns { Name: "add_1"  Type: "Uint32"}
                    )");

            WaitForSuppressed(runtime, suppressed, 2, defObserver);
        }

        {
            TVector<THolder<IEventHandle>> suppressed;
            auto defObserver = SetSuppressObserver(runtime, suppressed, TEvTxProcessing::EvPlanStep);

            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

            env.TestWaitNotification(runtime, 103);

            TestConsistentCopyTables(runtime, ++txId, "/", R"(
                           CopyTableDescriptions {
                             SrcPath: "/MyRoot/Table"
                             DstPath: "/MyRoot/copy"
                           })", {NKikimrScheme::StatusMultipleModifications});

            WaitForSuppressed(runtime, suppressed, 3, defObserver);
        }

    }

    Y_UNIT_TEST(AlterIndexTableDirectly) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.EnableBackgroundCompaction(false);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(1);
        NDataShard::gDbStatsDataSizeResolution = 10;
        NDataShard::gDbStatsRowCountResolution = 10;

        runtime.GetAppData().AdministrationAllowedSIDs.push_back("true-root@builtin");

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
              Name: "table"
              Columns { Name: "key"   Type: "Uint32" }
              Columns { Name: "value" Type: "Utf8" }
              KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, {txId, txId-1});

        {
            auto fnWriteRow = [&] (ui64 tabletId, ui32 key, TString value, const char* table) {
                TString writeQuery = Sprintf(R"(
                    (
                        (let key   '( '('key   (Uint32 '%u ) ) ) )
                        (let row   '( '('value (Utf8 '%s) ) ) )
                        (return (AsList (UpdateRow '__user__%s key row) ))
                    )
                )", key, value.c_str(), table);
                NKikimrMiniKQL::TResult result;
                TString err;
                NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
                UNIT_ASSERT_VALUES_EQUAL(err, "");
                UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
            };

            fnWriteRow(TTestTxConfig::FakeHiveTablets, 1, "A", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 2, "B", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 3, "C", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 4, "D", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 5, "E", "table");

            fnWriteRow(TTestTxConfig::FakeHiveTablets, 6, "F", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 7, "G", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 8, "H", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 9, "I", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 10, "J", "table");


            fnWriteRow(TTestTxConfig::FakeHiveTablets, 11, "K", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 12, "L", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 13, "M", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 14, "N", "table");
            fnWriteRow(TTestTxConfig::FakeHiveTablets, 15, "O", "table");
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(0),
                            NLs::PathVersionEqual(3)});

        {
            TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/table", "indexByValue", {"value"});
            ui64 buildIndexId = txId;

            auto listing = TestListBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot");
            Y_ASSERT(listing.EntriesSize() == 1);

            env.TestWaitNotification(runtime, buildIndexId, TTestTxConfig::SchemeShard);

            auto descr = TestGetBuildIndex(runtime, TTestTxConfig::SchemeShard, "/MyRoot", buildIndexId);
            Y_ASSERT(descr.GetIndexBuild().GetState() == Ydb::Table::IndexBuildState::STATE_DONE);
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/table"),
                           {NLs::PathExist,
                            NLs::IndexesCount(1),
                            NLs::PathVersionEqual(6)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/table/indexByValue", true, true, true),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(3)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/table/indexByValue/indexImplTable", true, true, true),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(4),
                            NLs::PartitionCount(1),
                            NLs::MinPartitionsCountEqual(1),
                            NLs::NoMaxPartitionsCount
                            });


        TestSplitTable(runtime, ++txId, "/MyRoot/table/indexByValue/indexImplTable", R"(
                            SourceTabletId: 72075186233409547
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "B" } }
                                }
                            }
                            SplitBoundary {
                                KeyPrefix {
                                    Tuple { Optional { Text: "D" } } Tuple {}
                                }
                            })");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/table/indexByValue/indexImplTable", true, true, true),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(5),
                            NLs::PartitionCount(3),
                            NLs::MinPartitionsCountEqual(1),
                            NLs::NoMaxPartitionsCount
                            });

        // request direct alter of forbidden fields of indexImplTable
        TestAlterTable(runtime, ++txId, "/MyRoot/table/indexByValue/", R"(
                Name: "indexImplTable"
                KeyColumnNames: ["key", "value"]
                PartitionConfig {
                    PartitioningPolicy {
                        MinPartitionsCount: 1
                        SizeToSplit: 100502
                        FastSplitSettings {
                            SizeThreshold: 100502
                            RowCountThreshold: 100502
                        }
                    }
                }
            )",
            {TEvSchemeShard::EStatus::StatusNameConflict}
        );
        env.TestWaitNotification(runtime, txId);

        {
            TestAlterTable(runtime, ++txId, "/MyRoot/table/indexByValue/", R"(
                        Name: "indexImplTable"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 1
                                SizeToSplit: 100500
                                FastSplitSettings {
                                    SizeThreshold: 100500
                                    RowCountThreshold: 100500
                                }
                            }
                        }
               )"
            );
            env.TestWaitNotification(runtime, txId);
        }

        while (true) {
            TVector<THolder<IEventHandle>> suppressed;
            auto prevObserver = SetSuppressObserver(runtime, suppressed, TEvDataShard::TEvPeriodicTableStats::EventType);

            WaitForSuppressed(runtime, suppressed, 10, prevObserver);
            for (auto &msg : suppressed) {
                runtime.Send(msg.Release());
            }
            suppressed.clear();

            bool itIsEnough = false;

            auto descr = DescribePath(runtime, "/MyRoot/table/indexByValue/indexImplTable", true, true, true);

            NLs::TCheckFunc checkPartitionCount = [&] (const NKikimrScheme::TEvDescribeSchemeResult& record) {
                if (record.GetPathDescription().TablePartitionsSize() == 1) {
                    itIsEnough = true;
                }
            };

            auto pathVersion = TestDescribeResult(descr, {checkPartitionCount});

            if (itIsEnough) {
                break;
            }

            for (const auto& tPart: descr.GetPathDescription().GetTablePartitions()) {
                TActorId sender = runtime.AllocateEdgeActor();
                auto evTx = new TEvDataShard::TEvCompactBorrowed(pathVersion.PathId);
                ForwardToTablet(runtime, tPart.GetDatashardId(), sender, evTx);
            }
        }

        TestDescribeResult(DescribePath(runtime, "/MyRoot/table/indexByValue", true, true, true),
                           {NLs::PathExist,
                            NLs::PathVersionEqual(4)});

        TestDescribeResult(DescribePath(runtime, "/MyRoot/table/indexByValue/indexImplTable", true, true, true),
                           {NLs::PathExist,
                            NLs::PartitionCount(1),
                            NLs::MinPartitionsCountEqual(1),
                            NLs::NoMaxPartitionsCount,
                            NLs::SizeToSplitEqual(100500)});
    }

    template <typename TCreateFn, typename TDropFn>
    void DisablePublicationsOfDropping(NSchemeCache::TSchemeCacheNavigate::EOp op, TCreateFn&& createFn, TDropFn&& dropFn) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // disable publications
        {
            TAtomic unused;
            runtime.GetAppData().Icb->SetValue("SchemeShard_DisablePublicationsOfDropping", true, unused);
        }

        createFn(runtime, txId);
        env.TestWaitNotification(runtime, txId);

        {
            auto nav = Navigate(runtime, "/MyRoot/Obj", op);
            const auto& entry = nav->ResultSet.at(0);
            UNIT_ASSERT_VALUES_EQUAL(entry.Status, NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
        }

        dropFn(runtime, txId);
        env.TestWaitNotification(runtime, txId);

        // still ok
        {
            auto nav = Navigate(runtime, "/MyRoot/Obj", op);
            const auto& entry = nav->ResultSet.at(0);
            UNIT_ASSERT_VALUES_EQUAL(entry.Status, NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
        }

        // check after reboot (should be removed in process of sync)
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        while (true) {
            auto nav = Navigate(runtime, "/MyRoot/Obj", op);
            const auto& entry = nav->ResultSet.at(0);
            if ((entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown)) {
                break;
            }

            env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
        }

        // enable publications
        {
            TAtomic unused;
            runtime.GetAppData().Icb->SetValue("SchemeShard_DisablePublicationsOfDropping", false, unused);
        }

        createFn(runtime, txId);
        env.TestWaitNotification(runtime, txId);

        {
            auto nav = Navigate(runtime, "/MyRoot/Obj", op);
            const auto& entry = nav->ResultSet.at(0);
            UNIT_ASSERT_VALUES_EQUAL(entry.Status, NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
        }

        dropFn(runtime, txId);
        env.TestWaitNotification(runtime, txId);

        {
            auto nav = Navigate(runtime, "/MyRoot/Obj", op);
            const auto& entry = nav->ResultSet.at(0);
            UNIT_ASSERT_VALUES_EQUAL(entry.Status, NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown);
        }
    }

    Y_UNIT_TEST(DisablePublicationsOfDropping_Dir) {
        DisablePublicationsOfDropping(NSchemeCache::TSchemeCacheNavigate::EOp::OpPath,
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestMkDir(runtime, ++txId, "/MyRoot", "Obj");
            },
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestRmDir(runtime, ++txId, "/MyRoot", "Obj");
            }
        );
    }

    Y_UNIT_TEST(DisablePublicationsOfDropping_Table) {
        DisablePublicationsOfDropping(NSchemeCache::TSchemeCacheNavigate::EOp::OpTable,
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Obj"
                    Columns { Name: "key"   Type: "Uint64" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
            },
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestDropTable(runtime, ++txId, "/MyRoot", "Obj");
            }
        );
    }

    Y_UNIT_TEST(DisablePublicationsOfDropping_IndexedTable) {
        DisablePublicationsOfDropping(NSchemeCache::TSchemeCacheNavigate::EOp::OpTable,
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
                    TableDescription {
                      Name: "Obj"
                      Columns { Name: "key"   Type: "Uint64" }
                      Columns { Name: "value" Type: "Utf8" }
                      KeyColumnNames: ["key"]
                    }
                    IndexDescription {
                      Name: "UserDefinedIndexByValue"
                      KeyColumnNames: ["value"]
                    }
                )");
            },
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestDropTable(runtime, ++txId, "/MyRoot", "Obj");
            }
        );
    }

    Y_UNIT_TEST(DisablePublicationsOfDropping_Pq) {
        DisablePublicationsOfDropping(NSchemeCache::TSchemeCacheNavigate::EOp::OpTopic,
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
                    Name: "Obj"
                    TotalGroupCount: 1
                    PartitionPerTablet: 1
                    PQTabletConfig: { PartitionConfig { LifetimeSeconds: 10 } }
                )");
            },
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestDropPQGroup(runtime, ++txId, "/MyRoot", "Obj");
            }
        );
    }

    Y_UNIT_TEST(DisablePublicationsOfDropping_Solomon) {
        DisablePublicationsOfDropping(NSchemeCache::TSchemeCacheNavigate::EOp::OpPath,
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestCreateSolomon(runtime, ++txId, "/MyRoot", R"(
                    Name: "Obj"
                    PartitionCount: 1
                )");
            },
            [](TTestBasicRuntime& runtime, ui64& txId) {
                return TestDropSolomon(runtime, ++txId, "/MyRoot", "Obj");
            }
        );
    }

    Y_UNIT_TEST(TopicReserveSize) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const auto AssertReserve = [&] (TString path, ui64 expectedReservedStorage) {
            TestDescribeResult(DescribePath(runtime, path),
                               {NLs::Finished,
                                NLs::TopicReservedStorage(expectedReservedStorage)});
        };

        // create with WriteSpeedInBytesPerSecond
        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 1 * 13 * 19);

        // Change MeteringMode
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_REQUEST_UNITS
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 0);

        // Change MeteringMode
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 1 * 13 * 19);

        // increase partitions count
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 7
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 7 * 13 * 19);

        // increase WriteSpeedInBytesPerSecond
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 7
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 23
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 7 * 13 * 23);

        // decrease WriteSpeedInBytesPerSecond
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 7
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 7 * 13 * 19);

        // increase LifetimeSeconds
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 7
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 17
                    WriteSpeedInBytesPerSecond : 23
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 7 * 17 * 23);

        // decrease LifetimeSeconds
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 7
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 7 * 13 * 19);

        // use StorageLimitBytes
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 7
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    StorageLimitBytes : 17
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 7 * 17);

        // increase StorageLimitBytes
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 7
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    StorageLimitBytes : 23
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 7 * 23);

        // decrease StorageLimitBytes
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 7
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    StorageLimitBytes : 17
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 7 * 17);

        // increase partitions count
        TestAlterPQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 11
            PartitionPerTablet: 11
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    StorageLimitBytes : 17
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic1", 11 * 17);

        // drop partiotion
        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Topic1");
        env.TestWaitNotification(runtime, txId);


        // create with StorageLimitBytes
        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic2"
            TotalGroupCount: 3
            PartitionPerTablet: 3
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    StorageLimitBytes : 17
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertReserve("/MyRoot/Topic2", 3 * 17);
    }

    Y_UNIT_TEST(FindSubDomainPathId) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "SubDomenA"
        )");
        env.TestWaitNotification(runtime, txId);

        // create with WriteSpeedInBytesPerSecond
        TestCreatePQGroup(runtime, ++txId, "/MyRoot/SubDomenA", R"(
            Name: "Topic1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto subDomainPathId = DescribePath(runtime, "/MyRoot/SubDomenA").GetPathId();
        auto topicTabletId = DescribePath(runtime, "/MyRoot/SubDomenA/Topic1", true, true, true)
                .GetPathDescription().GetPersQueueGroup().GetPartitions()[0].GetTabletId();

        ForwardToTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor(), new TEvSchemeShard::TEvFindTabletSubDomainPathId(topicTabletId));

        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvFindTabletSubDomainPathIdResult>(handle, TDuration::Seconds(1));
        UNIT_ASSERT(event);

        UNIT_ASSERT_VALUES_EQUAL(subDomainPathId, event->Record.GetSubDomainPathId());
    }

    Y_UNIT_TEST(FindSubDomainPathIdActor) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "SubDomenA"
        )");
        env.TestWaitNotification(runtime, txId);

        // create with WriteSpeedInBytesPerSecond
        TestCreatePQGroup(runtime, ++txId, "/MyRoot/SubDomenA", R"(
            Name: "Topic1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto subDomainPathId = DescribePath(runtime, "/MyRoot/SubDomenA").GetPathId();
        auto topicTabletId = DescribePath(runtime, "/MyRoot/SubDomenA/Topic1", true, true, true)
                .GetPathDescription().GetPersQueueGroup().GetPartitions()[0].GetTabletId();

        runtime.Register(CreateFindSubDomainPathIdActor(runtime.AllocateEdgeActor(), topicTabletId, TTestTxConfig::SchemeShard, false));

        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvSubDomainPathIdFound>(handle, TDuration::Seconds(1));
        UNIT_ASSERT(event);

        UNIT_ASSERT_VALUES_EQUAL(subDomainPathId, event->LocalPathId);
    }

    Y_UNIT_TEST(FindSubDomainPathIdActorAsync) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "SubDomenA"
        )");
        env.TestWaitNotification(runtime, txId);

        // create with WriteSpeedInBytesPerSecond
        TestCreatePQGroup(runtime, ++txId, "/MyRoot/SubDomenA", R"(
            Name: "Topic1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto subDomainPathId = DescribePath(runtime, "/MyRoot/SubDomenA").GetPathId();
        auto topicTabletId = DescribePath(runtime, "/MyRoot/SubDomenA/Topic1", true, true, true)
                .GetPathDescription().GetPersQueueGroup().GetPartitions()[0].GetTabletId();

        runtime.Register(CreateFindSubDomainPathIdActor(runtime.AllocateEdgeActor(), topicTabletId, TTestTxConfig::SchemeShard, true, TDuration::Seconds(2)));

        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvSubDomainPathIdFound>(handle, TDuration::Seconds(2));
        UNIT_ASSERT(event);

        UNIT_ASSERT_VALUES_EQUAL(subDomainPathId, event->LocalPathId);
    }

    Y_UNIT_TEST(CreateTopicOverDiskSpaceQuotas) {
        TTestBasicRuntime runtime;

        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);
        opts.EnablePersistentPartitionStats(true);
        opts.EnableTopicDiskSubDomainQuota(true);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        // Subdomain with a 1-byte data size quota
        TestCreateSubDomain(runtime, ++txId,  "/MyRoot", R"(
                        Name: "USER_1"
                        PlanResolution: 50
                        Coordinators: 1
                        Mediators: 1
                        TimeCastBucketsPerMediator: 2
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-1"
                            Kind: "hdd-1"
                        }
                        StoragePools {
                            Name: "name_USER_0_kind_hdd-2"
                            Kind: "hdd-2"
                        }
                        DatabaseQuotas {
                            data_size_hard_quota: 1
                        }
                )");
        env.TestWaitNotification(runtime, txId);

        TestCreatePQGroup(runtime, ++txId, "/MyRoot/USER_1", R"(
            Name: "Topic1"
            TotalGroupCount: 3
            PartitionPerTablet: 7
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 1
                    WriteSpeedInBytesPerSecond : 121
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )", {{TEvSchemeShard::EStatus::StatusResourceExhausted, "database size limit exceeded"}});
        env.TestWaitNotification(runtime, txId);
    }

}
