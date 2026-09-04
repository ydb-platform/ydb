#include "schemeshard_operation_planner_impl.h"

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>

namespace NKikimr::NSchemeShard {

// The table named by the request, and the directory that loses it. Mirrors the checks the
// former CreateDropIndexedTable made before it built any part, including what its reject
// response carried for a table that is already going away.
// A drop by id anchors on the object; a drop by name on its WorkingDir.
TPath TOperationPlanner::AnchorDropTable(TSchemeShard* ss, const TTxTransaction& tx) {
    const auto& drop = tx.GetDrop();
    if (drop.HasId()) {
        return TPath::Init(ss->MakeLocalId(drop.GetId()), ss);
    }
    const TString& workingDir = tx.GetWorkingDir();
    return workingDir.empty() ? TPath(ss) : TPath::Resolve(workingDir, ss);
}

bool TOperationPlanner::PlanDropTable(ui32 requestIdx, const TTxTransaction& tx) {
    const auto& drop = tx.GetDrop();

    TPath table = drop.HasId()
        ? TPath::Init(SS->MakeLocalId(drop.GetId()), SS)
        : TPath::Resolve(tx.GetWorkingDir(), SS).Dive(drop.GetName());

    bool columnTable = false;
    {
        TPath::TChecker checks = table.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .NotDeleted();

        if (checks) {
            if (table.Base()->IsColumnTable()) {
                checks
                    .IsColumnTable()
                    .NotUnderDeleting()
                    .NotUnderOperation()
                    .IsCommonSensePath();
                columnTable = true;
            } else {
                checks
                    .IsTable()
                    .NotUnderDeleting()
                    .NotUnderOperation();
                if ((!table.Parent()->IsTableIndex() || !NTableIndex::IsBuildImplTable(table.LeafName())) && !tx.GetInternal()) {
                    checks.IsCommonSensePath();
                }
            }
        }

        if (!checks) {
            FailAt(checks);
            if (table.IsResolved() && table.Base()->IsTable() && (table.Base()->PlannedToDrop() || table.Base()->Dropped())) {
                Failure->PathDropTxId = table.Base()->DropTxId;
                Failure->PathId = table.Base()->PathId;
            }
            return false;
        }
    }

    const auto targetEffect = AddWrittenEffect(table, EPlanEffect::Drop, EPlanRole::Target, EPlanOrigin::RequestNamed);
    if (!targetEffect) {
        return false;
    }
    const auto containerEffect = AddContainerEffect(table.Parent());
    if (!containerEffect) {
        return false;
    }

    // DROP TABLE has no say in whether it drops a row or a column table; the object decides,
    // and a column table has nothing to cascade into.
    Builder.AddPart(requestIdx, columnTable ? EPlannedPartKind::DropColumnTable : EPlannedPartKind::DropTable,
        tx, TTargetPartBindings{*targetEffect, *containerEffect});
    if (columnTable) {
        return true;
    }

    return PlanDropTableChildren(requestIdx, table, *targetEffect, EPlanOrigin::RequestImplied);
}

// Everything beneath a dropped table goes with it: sequences, indexes and their impl tables,
// streams and their topics. Children of the table itself are implied by the request; what is
// beneath an impl table exists only because of the decomposition. Same order as the former
// CascadeDropTableChildren, so TxPartId assignment is unchanged.
bool TOperationPlanner::PlanDropTableChildren(ui32 requestIdx, const TPath& table, TPlanEffectId tableEffect, EPlanOrigin origin) {
    for (const auto& [childName, childPathId] : table.Base()->GetChildren()) {
        TPath child = table.Child(childName);
        {
            TPath::TChecker checks = child.Check();
            checks
                .NotEmpty()
                .IsResolved();

            if (checks) {
                if (child.IsDeleted()) {
                    continue;
                }
            }

            if (child.IsTableIndex()) {
                checks.IsTableIndex();
            } else if (child.IsCdcStream()) {
                checks.IsCdcStream();
            } else if (child.IsSequence()) {
                checks.IsSequence();
            }

            checks.NotDeleted()
                .NotUnderDeleting()
                .NotUnderOperation();

            if (!checks) {
                return FailAt(checks);
            }
        }
        Y_ABORT_UNLESS(child.Base()->PathId == childPathId);

        if (!child.IsSequence() && !child.IsTableIndex() && !child.IsCdcStream()) {
            continue;
        }

        const auto childEffect = AddWrittenEffect(child, EPlanEffect::Drop, EPlanRole::Target, origin);
        if (!childEffect) {
            return false;
        }

        if (child.IsSequence()) {
            auto dropSequence = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence);
            dropSequence.MutableDrop()->SetName(ToString(child->Name));
            Builder.AddPart(requestIdx, EPlannedPartKind::DropSequence, std::move(dropSequence), TTargetPartBindings{*childEffect, tableEffect});
            continue;
        } else if (child.IsTableIndex()) {
            auto dropIndex = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTableIndex);
            dropIndex.MutableDrop()->SetName(ToString(child.Base()->Name));
            Builder.AddPart(requestIdx, EPlannedPartKind::DropTableIndex, std::move(dropIndex), TTargetPartBindings{*childEffect, tableEffect});
        } else if (child.IsCdcStream()) {
            auto dropStream = TransactionTemplate(table.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropCdcStreamImpl);
            dropStream.MutableDrop()->SetName(ToString(child.Base()->Name));
            Builder.AddPart(requestIdx, EPlannedPartKind::DropCdcStreamImpl, std::move(dropStream), TTargetPartBindings{*childEffect, tableEffect});
        }

        for (const auto& [implName, implPathId] : child.Base()->GetChildren()) {
            Y_ABORT_UNLESS(NTableIndex::IsImplTable(implName)
                        || implName == "streamImpl"
                , "unexpected name %s", implName.c_str());

            TPath implPath = child.Child(implName);
            if (implPath.IsDeleted()) {
                continue;
            }

            {
                TPath::TChecker checks = implPath.Check();
                checks
                    .NotEmpty()
                    .IsResolved()
                    .NotUnderDeleting()
                    .NotUnderOperation();

                if (checks) {
                    if (implPath.Base()->IsTable()) {
                        checks
                            .IsTable()
                            .IsInsideTableIndexPath();
                    } else if (implPath.Base()->IsPQGroup()) {
                        checks
                            .IsPQGroup()
                            .IsInsideCdcStreamPath();
                    }
                }

                if (!checks) {
                    return FailAt(checks);
                }
            }
            Y_ABORT_UNLESS(implPath.Base()->PathId == implPathId);

            const auto implEffect = AddWrittenEffect(implPath, EPlanEffect::Drop, EPlanRole::Target, EPlanOrigin::PartDerived);
            if (!implEffect) {
                return false;
            }

            if (implPath.Base()->IsTable()) {
                auto dropIndexTable = TransactionTemplate(child.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
                dropIndexTable.MutableDrop()->SetName(ToString(implPath.Base()->Name));
                Builder.AddPart(requestIdx, EPlannedPartKind::DropTable, std::move(dropIndexTable), TTargetPartBindings{*implEffect, *childEffect});
                if (!PlanDropTableChildren(requestIdx, implPath, *implEffect, EPlanOrigin::PartDerived)) {
                    return false;
                }
            } else if (implPath.Base()->IsPQGroup()) {
                auto dropPQGroup = TransactionTemplate(child.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);
                dropPQGroup.MutableDrop()->SetName(ToString(implPath.Base()->Name));
                Builder.AddPart(requestIdx, EPlannedPartKind::DropPersQueueGroup, std::move(dropPQGroup), TTargetPartBindings{*implEffect, *childEffect});
            }
        }
    }

    return true;
}

} // namespace NKikimr::NSchemeShard
