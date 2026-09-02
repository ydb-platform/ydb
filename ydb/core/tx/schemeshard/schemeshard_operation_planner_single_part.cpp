#include "schemeshard_operation_planner_impl.h"

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>

namespace NKikimr::NSchemeShard {

namespace {

bool IsDropKind(EPlannedPartKind kind) {
    switch (kind) {
    case EPlannedPartKind::DropTable:
    case EPlannedPartKind::DropColumnTable:
    case EPlannedPartKind::DropTableIndex:
    case EPlannedPartKind::DropCdcStreamImpl:
    case EPlannedPartKind::DropSequence:
    case EPlannedPartKind::DropPersQueueGroup:
        return true;
    default:
        return false;
    }
}

// The name a transaction of this kind gives its target.
const TString& TargetName(EPlannedPartKind kind, const NKikimrSchemeOp::TModifyScheme& tx) {
    switch (kind) {
    case EPlannedPartKind::MkDir:
        return tx.GetMkDir().GetName();
    case EPlannedPartKind::CreateTable:
    case EPlannedPartKind::CopyTable:
        return tx.GetCreateTable().GetName();
    case EPlannedPartKind::CreateTableIndex:
    case EPlannedPartKind::CreateColumnTableLocalIndex:
        return tx.GetCreateTableIndex().GetName();
    case EPlannedPartKind::CopySequence:
        return tx.GetSequence().GetName();
    case EPlannedPartKind::DropTable:
    case EPlannedPartKind::DropColumnTable:
    case EPlannedPartKind::DropTableIndex:
    case EPlannedPartKind::DropCdcStreamImpl:
    case EPlannedPartKind::DropSequence:
    case EPlannedPartKind::DropPersQueueGroup:
        return tx.GetDrop().GetName();
    }
}

// The target of a drop may be named by id; everything else is WorkingDir plus a name.
TPath TargetOf(TSchemeShard* ss, EPlannedPartKind kind, const NKikimrSchemeOp::TModifyScheme& tx) {
    if (IsDropKind(kind) && tx.GetDrop().HasId()) {
        return TPath::Init(ss->MakeLocalId(tx.GetDrop().GetId()), ss);
    }
    return TPath::Resolve(tx.GetWorkingDir(), ss).Dive(TargetName(kind, tx));
}

} // namespace

TPath TOperationPlanner::AnchorSinglePart(TSchemeShard* ss, EPlannedPartKind kind, const TTxTransaction& tx) {
    if (IsDropKind(kind) && tx.GetDrop().HasId()) {
        return TargetOf(ss, kind, tx);
    }
    const TString& workingDir = tx.GetWorkingDir();
    return workingDir.empty() ? TPath(ss) : TPath::Resolve(workingDir, ss);
}

bool TOperationPlanner::PlanSinglePart(ui32 requestIdx, EPlannedPartKind kind, const TTxTransaction& tx) {
    TPath target = TargetOf(SS, kind, tx);
    if (target.IsEmpty()) {
        // A drop by an id nobody has. Propose would have said the same at its first check.
        return Fail(NKikimrScheme::StatusNameConflict, "path is empty");
    }
    // The container is WorkingDir, resolved the way the part always resolved it. It is not the
    // target's parent: a target naming the schemeshard root is its own parent, and a bad
    // WorkingDir must stay bad so that Propose reports it. Only a drop by id has no
    // WorkingDir to use.
    const TPath container = (IsDropKind(kind) && tx.GetDrop().HasId())
        ? target.Parent()
        : TPath::Resolve(tx.GetWorkingDir(), SS);
    if (container.IsEmpty()) {
        // "/" or "" as WorkingDir. The part's own first check on it would say the same.
        return Fail(NKikimrScheme::StatusPathDoesNotExist, "WorkingDir does not name a path");
    }

    const EPlanEffect effect = IsDropKind(kind) ? EPlanEffect::Drop : EPlanEffect::Create;
    const auto targetEffect = AddWrittenEffect(target, effect, EPlanRole::Target, EPlanOrigin::RequestNamed);
    if (!targetEffect) {
        return false;
    }
    const auto containerEffect = AddContainerEffect(container);
    if (!containerEffect) {
        return false;
    }

    switch (kind) {
    case EPlannedPartKind::CopyTable: {
        const TPath source = TPath::Resolve(tx.GetCreateTable().GetCopyFromTable(), SS);
        auto srcRel = Relative(source.PathString());
        if (!srcRel) {
            return false;
        }
        const TPlanEffectId sourceEffect = Builder.AddReference(*srcRel, source.LeafName(), PathIdOf(source),
            EPlanRole::Source, EPlanOrigin::RequestNamed);
        Builder.AddPhysicalWrite(*srcRel, source.LeafName(), PathIdOf(source),
            EPlanObservation::MustWrite, EPhysicalWriteReason::SourceStateFlip, sourceEffect);

        TVector<TPlanEffectId> dropStreams;
        for (const auto& streamName : tx.GetCreateTable().GetDropSrcCdcStream().GetStreamName()) {
            const auto drop = AddWrittenEffect(source.Child(streamName), EPlanEffect::Drop, EPlanRole::Source, EPlanOrigin::RequestNamed);
            if (!drop) {
                return false;
            }
            dropStreams.push_back(*drop);
        }
        Builder.AddPart(requestIdx, kind, tx, TCopyTablePartBindings{*targetEffect, *containerEffect, sourceEffect, std::move(dropStreams)});
        return true;
    }
    case EPlannedPartKind::CopySequence: {
        const TPath source = TPath::Resolve(tx.GetCopySequence().GetCopyFrom(), SS);
        auto srcRel = Relative(source.PathString());
        if (!srcRel) {
            return false;
        }
        const TPlanEffectId sourceEffect = Builder.AddReference(*srcRel, source.LeafName(), PathIdOf(source),
            EPlanRole::Source, EPlanOrigin::RequestNamed);
        Builder.AddPart(requestIdx, kind, tx, TTargetWithSourcePartBindings{*targetEffect, *containerEffect, sourceEffect});
        return true;
    }
    default:
        Builder.AddPart(requestIdx, kind, tx, TTargetPartBindings{*targetEffect, *containerEffect});
        return true;
    }
}

TOperationPlanResult PlanSinglePart(EPlannedPartKind kind, const NKikimrSchemeOp::TModifyScheme& tx, TSchemeShard* ss) {
    TOperationPlanner planner(ss, TOperationPlanner::DeriveDatabaseRoot(ss, TOperationPlanner::AnchorSinglePart(ss, kind, tx)));
    const ui32 requestIdx = planner.GetBuilder().AddRequest();
    if (!planner.PlanSinglePart(requestIdx, kind, tx)) {
        return planner.GetFailure();
    }
    return planner.GetBuilder().Seal();
}

} // namespace NKikimr::NSchemeShard
