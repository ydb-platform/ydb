#include "fake_operation_context.h"
#include "parity_helpers.h"

#include <ydb/core/tx/schemeshard/generated/op_handlers.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation.h>
#include <ydb/core/tx/schemeshard/schemeshard_audit_log_fragment.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NSchemeShard;
namespace NSO = NKikimrSchemeOp;
namespace NHandlers = NKikimr::NSchemeShard::NGenerated::NOpHandlers;

static_assert(NHandlers::IsRegistered_v<NSO::ESchemeOpCreateTable>);
static_assert(!NHandlers::IsRegistered_v<NSO::ESchemeOpDropTable>);
static_assert(!NHandlers::IsRegistered_v<NSO::ESchemeOpMkDir>);

Y_UNIT_TEST_SUITE(SchemeshardOpHandlers) {

    Y_UNIT_TEST(TryCollectChangingPathsRoutesMigratedOp) {
        NSO::TModifyScheme tx;
        tx.SetOperationType(NSO::ESchemeOpCreateTable);
        tx.SetWorkingDir("/Root/Db");
        tx.MutableCreateTable()->SetName("Users");

        auto handled = NHandlers::TryCollectChangingPaths(tx);
        UNIT_ASSERT(handled.has_value());
        UNIT_ASSERT_VALUES_EQUAL(handled->size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL((*handled)[0], "/Root/Db/Users");
    }

    Y_UNIT_TEST(TryCollectChangingPathsReturnsNulloptForUnmigratedOp) {
        NSO::TModifyScheme tx;
        tx.SetOperationType(NSO::ESchemeOpMkDir);
        tx.SetWorkingDir("/Root");
        tx.MutableMkDir()->SetName("NewDir");

        auto handled = NHandlers::TryCollectChangingPaths(tx);
        UNIT_ASSERT(!handled.has_value());
    }

    Y_UNIT_TEST(MakeAuditLogFragmentRoutesCreateTableThroughHandler) {
        NSO::TModifyScheme tx;
        tx.SetOperationType(NSO::ESchemeOpCreateTable);
        tx.SetWorkingDir("/Root/Db");
        tx.MutableCreateTable()->SetName("Users");

        const auto fragment = MakeAuditLogFragment(tx);
        UNIT_ASSERT_VALUES_EQUAL(fragment.Paths.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(fragment.Paths[0], "/Root/Db/Users");
    }

    Y_UNIT_TEST(MakeAuditLogFragmentStillHandlesUnmigratedOps) {
        NSO::TModifyScheme tx;
        tx.SetOperationType(NSO::ESchemeOpMkDir);
        tx.SetWorkingDir("/Root");
        tx.MutableMkDir()->SetName("NewDir");

        const auto fragment = MakeAuditLogFragment(tx);
        UNIT_ASSERT_VALUES_EQUAL(fragment.Paths.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(fragment.Paths[0], "/Root/NewDir");
    }

    Y_UNIT_TEST(MakeAuditLogFragmentHandlesCopyFromTableVariant) {
        NSO::TModifyScheme tx;
        tx.SetOperationType(NSO::ESchemeOpCreateTable);
        tx.SetWorkingDir("/Root/Db");
        auto* createTable = tx.MutableCreateTable();
        createTable->SetName("UsersCopy");
        createTable->SetCopyFromTable("/Root/Db/Users");

        const auto fragment = MakeAuditLogFragment(tx);
        UNIT_ASSERT_VALUES_EQUAL(fragment.Paths.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(fragment.Paths[0], "/Root/Db/UsersCopy");
    }

    Y_UNIT_TEST(DirectAuditHandlerProducesExpectedPath) {
        NSO::TModifyScheme tx;
        tx.SetOperationType(NSO::ESchemeOpCreateTable);
        tx.SetWorkingDir("/Root/Db");
        tx.MutableCreateTable()->SetName("Users");

        TVector<TString> paths;
        NHandlers::CollectChangingPaths_ESchemeOpCreateTable(tx, paths);
        UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(paths[0], "/Root/Db/Users");
    }

    Y_UNIT_TEST(MigrationProgressReflectsYaml) {
        // The yaml currently lists ESchemeOpCreateTable. As more ops migrate
        // this count grows; pin the lower bound to catch accidental
        // regressions where an op silently drops off the registered set.
        size_t registered = 0;
        const auto* d = NSO::EOperationType_descriptor();
        for (int i = 0; i < d->value_count(); ++i) {
            const auto opType = static_cast<NSO::EOperationType>(d->value(i)->number());
            // Build a minimal tx and ask the dispatch — equivalent to checking
            // IsRegistered_v at runtime.
            NSO::TModifyScheme tx;
            tx.SetOperationType(opType);
            tx.MutableCreateTable()->SetName("x");
            tx.MutableMkDir()->SetName("x");
            if (NHandlers::TryCollectChangingPaths(tx).has_value()) {
                ++registered;
            }
        }
        UNIT_ASSERT_C(registered >= 1, "at least CreateTable should be registered");
    }

    // --- parity_helpers demo ---

    Y_UNIT_TEST(ParityFixturePinsCreateTableAuditOutput) {
        NTesting::AssertOpAuditPaths(
            NSO::ESchemeOpCreateTable,
            [](NSO::TModifyScheme& tx) {
                tx.SetWorkingDir("/Root/Db");
                tx.MutableCreateTable()->SetName("Users");
            },
            {"/Root/Db/Users"});
    }

    Y_UNIT_TEST(ParityFixturePinsCreateTableCopyVariant) {
        NTesting::AssertOpAuditPaths(
            NSO::ESchemeOpCreateTable,
            [](NSO::TModifyScheme& tx) {
                tx.SetWorkingDir("/Root/Db");
                auto* createTable = tx.MutableCreateTable();
                createTable->SetName("UsersCopy");
                createTable->SetCopyFromTable("/Root/Db/Users");
            },
            {"/Root/Db/UsersCopy"});
    }

    // --- fake context demo ---

    Y_UNIT_TEST(DispatchAndAuditFragmentAgreeForAllRegisteredOps) {
        // Auto-growing cross-check: for every op listed in
        // op_handler_overrides.yaml, assert that direct dispatch via
        // TryCollectChangingPaths produces the same paths as the public
        // MakeAuditLogFragment surface. Catches central-wiring regressions
        // (e.g. someone wraps the dispatch differently in one path).
        // No edits needed when a new op migrates — it's picked up via
        // IsOpRegistered. Field setup below grows as ops migrate.
        const auto* d = NSO::EOperationType_descriptor();
        for (int i = 0; i < d->value_count(); ++i) {
            const auto opType = static_cast<NSO::EOperationType>(d->value(i)->number());
            if (!NHandlers::IsOpRegistered(opType)) {
                continue;
            }

            NSO::TModifyScheme tx;
            tx.SetOperationType(opType);
            tx.SetWorkingDir("/Root");
            // Generic field setup. Add per-op cases as they migrate if the
            // op needs sub-message fields beyond what's set here.
            tx.MutableCreateTable()->SetName("X");
            tx.MutableMkDir()->SetName("X");

            const auto direct = NHandlers::TryCollectChangingPaths(tx);
            UNIT_ASSERT_C(direct.has_value(),
                          "TryCollectChangingPaths returned nullopt for migrated op "
                          << NSO::EOperationType_Name(opType));

            const auto fragment = MakeAuditLogFragment(tx);
            UNIT_ASSERT_VALUES_EQUAL_C(fragment.Paths, *direct,
                                       "central wiring diverges from direct dispatch for "
                                       << NSO::EOperationType_Name(opType));
        }
    }

    Y_UNIT_TEST(FactoryNonCopyEmitsOnePartViaFakeContext) {
        // Tier-2 demonstration: invoke the factory handler directly with a
        // fake TOperationContext. CreateTable's non-copy branch never reads
        // ctx, so the fake suffices. If a future change makes the handler
        // touch ctx.SS or ctx.GetTxc(), this test crashes — by design.
        NTesting::TFakeOperationContextHarness fakeCtx;
        TOperation op(TTxId(42));

        NSO::TModifyScheme tx;
        tx.SetOperationType(NSO::ESchemeOpCreateTable);
        tx.SetWorkingDir("/Root/Db");
        tx.MutableCreateTable()->SetName("Orders");

        const auto parts = NHandlers::MakeOperationParts_ESchemeOpCreateTable(op, tx, fakeCtx.Get());
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 1u);
        UNIT_ASSERT(parts[0] != nullptr);
    }
}
