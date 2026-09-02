#pragma once

#include "schemeshard_operation_planner.h"
#include "schemeshard_path.h"

#include <optional>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

// One planner per operation. The core is the builder, the root, the failure, and the helpers
// every operation type uses; each type adds its own PlanXxx member in its own file.
class TOperationPlanner {
    using TTxTransaction = NKikimrSchemeOp::TModifyScheme;

public:
    TOperationPlanner(TSchemeShard* ss, TString databaseRoot);

    // Plans every transaction as its own request subplan.
    TOperationPlanResult Run(const TVector<TTxTransaction>& transactions);

    // The database root the plan of these transactions is relative to.
    static TString DeriveDatabaseRoot(TSchemeShard* ss, const TVector<TTxTransaction>& transactions);
    static TString DeriveDatabaseRoot(TSchemeShard* ss, const TPath& anchor);

    // schemeshard_operation_planner_drop_table.cpp
    bool PlanDropTableChildren(ui32 requestIdx, const TPath& table, TPlanEffectId tableEffect, EPlanOrigin origin);

    TOperationPlanBuilder& GetBuilder() {
        return Builder;
    }

    const TRejectedOperation& GetFailure() const {
        Y_ABORT_UNLESS(Failure);
        return *Failure;
    }

private:
    bool PlanRequest(ui32 requestIdx, const TTxTransaction& tx);

    // schemeshard_operation_planner_create_table.cpp
    bool PlanCreateTable(ui32 requestIdx, const TTxTransaction& tx);
    bool SplitCreateTable(const TTxTransaction& tx, TTxTransaction& create, TVector<TTxTransaction>& mkdirs);
    bool PlanCopySequences(ui32 requestIdx, const TTxTransaction& create, const TPath& srcTable, const TString& dstAbs,
        TPlanEffectId containerEffect, EPlanOrigin origin);
    bool FillIndexDescription(NKikimrSchemeOp::TIndexCreationConfig& operation, const TString& name, const TTableIndexInfo& indexInfo);

    // schemeshard_operation_planner_drop_table.cpp
    bool PlanDropTable(ui32 requestIdx, const TTxTransaction& tx);

protected:
    bool Fail(NKikimrScheme::EStatus status, TString reason);
    bool FailAt(const TPath::TChecker& checks);
    bool Failed() const {
        return Failure.has_value();
    }

    // Every path in a plan is relative to its one root. A request naming a path outside it
    // cannot be planned; Propose would have rejected it as nonexistent from this schemeshard's
    // point of view.
    std::optional<TDatabaseRelativePath> Relative(const TString& absolute);

    // A logical effect with the physical write its row implies, in one call.
    std::optional<TPlanEffectId> AddWrittenEffect(const TPath& path, EPlanEffect effect, EPlanRole role, EPlanOrigin origin,
        EPlanObservation expect = EPlanObservation::MustWrite);
    std::optional<TPlanEffectId> AddWrittenEffect(const TString& absolute, const TString& leafName, std::optional<TPathId> pathId,
        EPlanEffect effect, EPlanRole role, EPlanOrigin origin, EPlanObservation expect = EPlanObservation::MustWrite);

    // The directory that gains or loses a child: its row is bumped only for a directory or a
    // domain root.
    std::optional<TPlanEffectId> AddContainerEffect(const TPath& container, bool willBeDirectory = false);

    static TString Join(const TString& dir, const TString& name);
    static TString Leaf(const TString& absolute);

public:
    static std::optional<TPathId> PathIdOf(const TPath& path);

protected:
    TSchemeShard* const SS;
    TOperationPlanBuilder Builder;
    std::optional<TRejectedOperation> Failure;
};

} // namespace NKikimr::NSchemeShard
