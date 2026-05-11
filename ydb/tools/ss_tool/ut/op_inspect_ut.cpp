#include <ydb/tools/ss_tool/lib/op_inspect.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr::NSchemeShard::NSsTool;
namespace NSO = NKikimrSchemeOp;

Y_UNIT_TEST_SUITE(SsToolOpInspect) {

    Y_UNIT_TEST(CollectRowFillsIdentityFromProto) {
        const auto row = CollectRow(NSO::ESchemeOpCreateTable);
        UNIT_ASSERT_VALUES_EQUAL(row.Name, "ESchemeOpCreateTable");
        UNIT_ASSERT_EQUAL(row.Type, NSO::ESchemeOpCreateTable);
    }

    Y_UNIT_TEST(CreateTableIsRegistered) {
        const auto row = CollectRow(NSO::ESchemeOpCreateTable);
        UNIT_ASSERT(row.IsRegistered);
    }

    Y_UNIT_TEST(MkDirIsNotRegistered) {
        const auto row = CollectRow(NSO::ESchemeOpMkDir);
        UNIT_ASSERT(!row.IsRegistered);
    }

    Y_UNIT_TEST(DropTableIsNotRegistered) {
        const auto row = CollectRow(NSO::ESchemeOpDropTable);
        UNIT_ASSERT(!row.IsRegistered);
    }

    Y_UNIT_TEST(AllOpsCoversEveryDistinctEnumNumber) {
        const auto ops = AllOps();

        const auto* d = NSO::EOperationType_descriptor();
        THashSet<int> expected;
        for (int i = 0; i < d->value_count(); ++i) {
            expected.insert(d->value(i)->number());
        }

        UNIT_ASSERT_VALUES_EQUAL(ops.size(), expected.size());

        THashSet<int> got;
        for (const auto& op : ops) {
            UNIT_ASSERT_C(got.insert(static_cast<int>(op.Type)).second,
                          "duplicate op number in AllOps()");
        }
        UNIT_ASSERT_EQUAL(got, expected);
    }

    Y_UNIT_TEST(AllOpsIsNonTrivialAndIncludesCreateTable) {
        const auto ops = AllOps();
        UNIT_ASSERT_C(ops.size() >= 50, "got " << ops.size());

        bool sawCreateTable = false;
        for (const auto& op : ops) {
            if (op.Type == NSO::ESchemeOpCreateTable) {
                sawCreateTable = true;
                break;
            }
        }
        UNIT_ASSERT(sawCreateTable);
    }

    Y_UNIT_TEST(MigrationProgressLowerBound) {
        const auto ops = AllOps();
        size_t registered = 0;
        for (const auto& op : ops) {
            if (op.IsRegistered) ++registered;
        }
        UNIT_ASSERT_C(registered >= 1, "expected at least CreateTable to be registered");
    }
}
