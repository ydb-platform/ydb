#include "schemeshard_operation_planner_impl.h"

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>

namespace NKikimr::NSchemeShard {

namespace {

// The registry of planned operation types. Adding a type means adding its file and one row
// here; nothing else in the core switches on the type.
struct TPlannedOperationType {
    bool (TOperationPlanner::*Plan)(ui32 requestIdx, const NKikimrSchemeOp::TModifyScheme& tx);
    TPath (*Anchor)(TSchemeShard* ss, const NKikimrSchemeOp::TModifyScheme& tx);
};

const TPlannedOperationType* FindPlannedType(NKikimrSchemeOp::EOperationType type) {
    static const TPlannedOperationType createTable{&TOperationPlanner::PlanCreateTable, &TOperationPlanner::AnchorCreateTable};
    static const TPlannedOperationType dropTable{&TOperationPlanner::PlanDropTable, &TOperationPlanner::AnchorDropTable};

    switch (type) {
    case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable:
        return &createTable;
    case NKikimrSchemeOp::EOperationType::ESchemeOpDropTable:
        return &dropTable;
    default:
        return nullptr;
    }
}

} // namespace

bool IsPlannedOperationType(NKikimrSchemeOp::EOperationType type) {
    return FindPlannedType(type) != nullptr;
}

TOperationPlanner::TOperationPlanner(TSchemeShard* ss, TString databaseRoot)
    : SS(ss)
    , Builder(std::move(databaseRoot))
{
}

TOperationPlanResult TOperationPlanner::Run(const TVector<TTxTransaction>& transactions) {
    for (ui32 i = 0; i < transactions.size(); ++i) {
        const ui32 requestIdx = Builder.AddRequest();
        Y_ABORT_UNLESS(requestIdx == i);
        if (!PlanRequest(requestIdx, transactions[i])) {
            return GetFailure();
        }
    }
    return Builder.Seal();
}

bool TOperationPlanner::PlanRequest(ui32 requestIdx, const TTxTransaction& tx) {
    const auto* type = FindPlannedType(tx.GetOperationType());
    Y_ABORT_UNLESS(type, "operation type %d is not planned", static_cast<int>(tx.GetOperationType()));
    return (this->*type->Plan)(requestIdx, tx);
}

// The database root has to come from a path that exists. What a request anchors on may not:
// a create may name a directory this same operation is about to make, a drop may name a
// table that is already gone. Walking up to the nearest existing ancestor is enough, because
// domain membership is inherited. The schemeshard root always resolves, so this terminates
// with a real root.
TString TOperationPlanner::DeriveDatabaseRoot(TSchemeShard* ss, const TPath& anchor) {
    TPath probe = anchor;
    if (!probe.IsEmpty()) {
        probe.RiseUntilExisted();
    }
    if (probe.IsEmpty() || !probe.IsResolved()) {
        probe = TPath::Init(ss->RootPathId(), ss);
    }
    return probe.GetDomainPathString();
}

// The root is per operation, so it comes from the first request; every later request must be
// expressible against it or its planner fails.
TString TOperationPlanner::DeriveDatabaseRoot(TSchemeShard* ss, const TVector<TTxTransaction>& transactions) {
    Y_ABORT_UNLESS(!transactions.empty());
    const auto& tx = transactions.front();
    const auto* type = FindPlannedType(tx.GetOperationType());
    Y_ABORT_UNLESS(type, "operation type %d is not planned", static_cast<int>(tx.GetOperationType()));
    return DeriveDatabaseRoot(ss, type->Anchor(ss, tx));
}

bool TOperationPlanner::Fail(NKikimrScheme::EStatus status, TString reason) {
    Failure = TRejectedOperation{.Status = status, .Reason = std::move(reason)};
    return false;
}

bool TOperationPlanner::FailAt(const TPath::TChecker& checks) {
    Y_ABORT_UNLESS(!checks);
    return Fail(checks.GetStatus(), checks.GetError());
}

std::optional<TDatabaseRelativePath> TOperationPlanner::Relative(const TString& absolute) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute(Builder.GetDatabaseRoot(), absolute);
    if (conclusion.IsFail()) {
        Fail(NKikimrScheme::StatusPathDoesNotExist,
            TStringBuilder() << "Path " << absolute << " is outside the database " << Builder.GetDatabaseRoot());
        return std::nullopt;
    }
    return conclusion.DetachResult();
}

std::optional<TPlanEffectId> TOperationPlanner::AddWrittenEffect(const TString& absolute, const TString& leafName,
        std::optional<TPathId> pathId, EPlanEffect effect, EPlanRole role, EPlanOrigin origin, EPlanObservation expect)
{
    auto rel = Relative(absolute);
    if (!rel) {
        return std::nullopt;
    }
    const TPlanEffectId id = Builder.AddSchemaEffect(*rel, leafName, pathId, effect, role, origin);
    Builder.AddPhysicalWrite(*rel, leafName, pathId, expect, EPhysicalWriteReason::LogicalEffect, id);
    return id;
}

std::optional<TPlanEffectId> TOperationPlanner::AddWrittenEffect(const TPath& path, EPlanEffect effect, EPlanRole role,
        EPlanOrigin origin, EPlanObservation expect)
{
    return AddWrittenEffect(path.PathString(), path.LeafName(), PathIdOf(path), effect, role, origin, expect);
}

std::optional<TPlanEffectId> TOperationPlanner::AddContainerEffect(const TPath& container, bool willBeDirectory) {
    const bool bumpsDirAlterVersion = willBeDirectory
        || (container.IsResolved() && (container.Base()->IsDirectory() || container.Base()->IsDomainRoot()));
    return AddWrittenEffect(container, EPlanEffect::ChildrenChanged, EPlanRole::Container, EPlanOrigin::RequestNamed,
        bumpsDirAlterVersion ? EPlanObservation::MustWrite : EPlanObservation::MayWrite);
}

TString TOperationPlanner::Join(const TString& dir, const TString& name) {
    return CanonizePath(JoinPath({dir, name}));
}

TString TOperationPlanner::Leaf(const TString& absolute) {
    return TString(ExtractBase(absolute));
}

std::optional<TPathId> TOperationPlanner::PathIdOf(const TPath& path) {
    if (path.IsResolved() && !path.IsDeleted()) {
        return path.Base()->PathId;
    }
    return std::nullopt;
}

TOperationPlanResult PlanOperation(const TVector<NKikimrSchemeOp::TModifyScheme>& transactions, TSchemeShard* ss) {
    for (const auto& tx : transactions) {
        Y_ABORT_UNLESS(IsPlannedOperationType(tx.GetOperationType()));
    }
    TOperationPlanner planner(ss, TOperationPlanner::DeriveDatabaseRoot(ss, transactions));
    return planner.Run(transactions);
}

TOperationPlanResult PlanDropTableChildren(const TPath& table, TSchemeShard* ss) {
    TOperationPlanner planner(ss, TOperationPlanner::DeriveDatabaseRoot(ss, table));
    auto& builder = planner.GetBuilder();
    const ui32 requestIdx = builder.AddRequest();

    auto rel = TDatabaseRelativePath::FromAbsolute(builder.GetDatabaseRoot(), table.PathString());
    Y_ABORT_UNLESS(rel.IsSuccess(), "an existing table is inside its own database");
    const TPlanEffectId tableEffect = builder.AddReference(rel.DetachResult(), table.LeafName(),
        TOperationPlanner::PathIdOf(table), EPlanRole::Container, EPlanOrigin::RequestNamed);

    if (!planner.PlanDropTableChildren(requestIdx, table, tableEffect, EPlanOrigin::RequestImplied)) {
        return planner.GetFailure();
    }
    return builder.Seal();
}

} // namespace NKikimr::NSchemeShard
