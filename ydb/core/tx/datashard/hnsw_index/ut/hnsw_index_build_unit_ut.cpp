#include <ydb/core/tx/datashard/datashard_active_transaction.h>
#include <ydb/core/tx/datashard/hnsw_index/hnsw_index_build_unit.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NDataShard {

Y_UNIT_TEST_SUITE(HnswIndexBuildExecutionPlan) {
    Y_UNIT_TEST_QUAD(OnlyPostingTablePublicationAddsBackgroundBuildUnit, Loaded, Enabled) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData().FeatureFlags.SetEnableHnswIndex(Enabled);
        runtime.RunCall([&] {
            // Drop, ordinary alter, metadata-only alter, and publication.
            for (ui32 kind = 0; kind < 4; ++kind) {
                NKikimrTxDataShard::TFlatSchemeTransaction scheme;
                if (kind == 0) {
                    scheme.MutableDropTable()->SetId_Deprecated(1);
                } else {
                    auto* alter = scheme.MutableAlterTable();
                    alter->SetName("posting");
                    if (kind >= 2) {
                        auto* index = alter->AddTableIndexes();
                        index->SetType(NKikimrSchemeOp::EIndexTypeGlobalHnsw);
                        auto* hnsw = index->MutableHnswIndexDescription();
                        hnsw->SetEmbeddingColumnId(2);
                        hnsw->MutableSettings()->mutable_settings()->set_vector_dimension(2);
                    }
                    if (kind == 3) {
                        alter->MutablePartitionConfig()->SetShadowData(false);
                    }
                }

                TActiveTransaction tx(TBasicOpInfo(1, EOperationKind::SchemeTx, 0, 0, TInstant::Zero(), 1));
                if (!Loaded) {
                    tx.SetTxBody(scheme.SerializeAsString());
                    UNIT_ASSERT(tx.BuildSchemeTx());
                }
                // Restored transactions do not have their body until LoadTxDetails.
                tx.BuildExecutionPlan(Loaded);
                UNIT_ASSERT(Find(tx.GetExecutionPlan(), EExecutionUnitKind::BuildHnswIndex)
                    == tx.GetExecutionPlan().end());
                while (tx.GetCurrentUnit() != EExecutionUnitKind::LoadTxDetails) {
                    tx.AdvanceExecutionPlan();
                }
                if (Loaded) {
                    tx.SetTxBody(scheme.SerializeAsString());
                    UNIT_ASSERT(tx.BuildSchemeTx());
                }
                AddHnswIndexBuildToExecutionPlan(tx);
                AddHnswIndexBuildToExecutionPlan(tx); // Loading details twice must not duplicate the unit.
                const auto& plan = tx.GetExecutionPlan();
                const auto build = Find(plan, EExecutionUnitKind::BuildHnswIndex);
                if (Enabled && kind == 3) {
                    UNIT_ASSERT(build != plan.end());
                    UNIT_ASSERT(build == Find(plan, EExecutionUnitKind::AlterTable) + 1);
                    UNIT_ASSERT(Find(build + 1, plan.end(), EExecutionUnitKind::BuildHnswIndex) == plan.end());
                } else {
                    UNIT_ASSERT(build == plan.end());
                }
            }
            return true;
        });
    }
}

} // namespace NKikimr::NDataShard
