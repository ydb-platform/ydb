#include "schemeshard_operation_plan.h"

namespace NKikimr::NSchemeShard {

TString TSealedOperationPlan::Absolute(const TDatabaseRelativePath& path) const {
    const TStringBuf relative = path.Value();
    if (relative == "/") {
        return DatabaseRoot;
    }
    return DatabaseRoot + relative;
}

TPlannedPathView TSealedOperationPlan::ViewOfEffect(TPlanEffectId id) const {
    const auto& effect = Effect(id);
    return TPlannedPathView{effect.Path, effect.LeafName, effect.PathId};
}

TPlannedPathView TSealedOperationPlan::ViewOfWrite(TPhysicalWriteId id) const {
    const auto& write = Write(id);
    return TPlannedPathView{write.Path, write.LeafName, write.PathId};
}

const TPartBlueprint* TSealedOperationPlan::FindPart(ui32 partIdx) const {
    for (const auto& part : Parts) {
        if (part.PartIdx == partIdx) {
            return &part;
        }
    }
    return nullptr;
}

TVector<const TLogicalPathEffect*> TSealedOperationPlan::SchemaEffectsForRecord() const {
    TVector<const TLogicalPathEffect*> selected;
    for (const auto& effect : LogicalEffects) {
        if (effect.IsSchemaEffect() && effect.Origin != EPlanOrigin::PartDerived) {
            selected.push_back(&effect);
        }
    }
    return selected;
}

TPlanEffectId TOperationPlanBuilder::AddSchemaEffect(TDatabaseRelativePath path, TString leafName,
        std::optional<TPathId> pathId, EPlanEffect effect, EPlanRole role, EPlanOrigin origin)
{
    const TPlanEffectId id = Plan.LogicalEffects.size();
    Plan.LogicalEffects.push_back(TLogicalPathEffect{
        .Id = id,
        .Path = std::move(path),
        .LeafName = std::move(leafName),
        .PathId = pathId,
        .Role = role,
        .Origin = origin,
        .Kind = TSchemaEffect{effect, std::nullopt},
    });
    return id;
}

TPlanEffectId TOperationPlanBuilder::AddReference(TDatabaseRelativePath path, TString leafName,
        std::optional<TPathId> pathId, EPlanRole role, EPlanOrigin origin)
{
    const TPlanEffectId id = Plan.LogicalEffects.size();
    Plan.LogicalEffects.push_back(TLogicalPathEffect{
        .Id = id,
        .Path = std::move(path),
        .LeafName = std::move(leafName),
        .PathId = pathId,
        .Role = role,
        .Origin = origin,
        .Kind = TReference{},
    });
    return id;
}

TPhysicalWriteId TOperationPlanBuilder::AddPhysicalWrite(TDatabaseRelativePath path, TString leafName,
        std::optional<TPathId> pathId, EPlanObservation expect, EPhysicalWriteReason reason,
        std::optional<TPlanEffectId> logicalEffect)
{
    const TPhysicalWriteId id = Plan.PhysicalWrites.size();
    Plan.PhysicalWrites.push_back(TPhysicalPathWrite{
        .Id = id,
        .Path = std::move(path),
        .LeafName = std::move(leafName),
        .PathId = pathId,
        .Expect = expect,
        .Reason = reason,
        .LogicalEffect = logicalEffect,
    });
    return id;
}

void TOperationPlanBuilder::Pair(TPlanEffectId a, TPlanEffectId b) {
    auto* first = std::get_if<TSchemaEffect>(&Plan.LogicalEffects.at(a).Kind);
    auto* second = std::get_if<TSchemaEffect>(&Plan.LogicalEffects.at(b).Kind);
    Y_ABORT_UNLESS(first && second, "only schema effects can be paired");
    first->Related = b;
    second->Related = a;
}

ui32 TOperationPlanBuilder::AddRequest() {
    const ui32 idx = Plan.Requests.size();
    Plan.Requests.push_back(TRequestSubplan{.RequestIdx = idx});
    return idx;
}

ui32 TOperationPlanBuilder::AddGeneratedDirPart(ui32 requestIdx, NKikimrSchemeOp::TModifyScheme tx, TMkDirPartBindings bindings) {
    const ui32 partIdx = Plan.Parts.size();
    Plan.Parts.push_back(TPartBlueprint{
        .PartIdx = partIdx,
        .RequestIdx = requestIdx,
        .Tx = std::move(tx),
        .Bindings = bindings,
    });
    Plan.Requests.at(requestIdx).GeneratedDirParts.push_back(partIdx);
    return partIdx;
}

ui32 TOperationPlanBuilder::AddPart(ui32 requestIdx, NKikimrSchemeOp::TModifyScheme tx, TPartBindings bindings) {
    const ui32 partIdx = Plan.Parts.size();
    Plan.Parts.push_back(TPartBlueprint{
        .PartIdx = partIdx,
        .RequestIdx = requestIdx,
        .Tx = std::move(tx),
        .Bindings = std::move(bindings),
    });
    Plan.Requests.at(requestIdx).Parts.push_back(partIdx);
    return partIdx;
}

std::shared_ptr<const TSealedOperationPlan> TOperationPlanBuilder::Seal() {
    auto sealed = std::make_shared<TSealedOperationPlan>(std::move(Plan));
    Plan = TSealedOperationPlan();
    return sealed;
}

} // namespace NKikimr::NSchemeShard
