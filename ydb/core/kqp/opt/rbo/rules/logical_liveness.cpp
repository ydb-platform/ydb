#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

using namespace NYql::NNodes;

bool AddLiveColumn(TInfoUnitSet& target, const TInfoUnit& iu) {
    return target.insert(iu).second;
}

bool AddColumnsToSet(TInfoUnitSet& target, const TVector<TInfoUnit>& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= AddLiveColumn(target, iu);
    }
    return changed;
}

class TLogicalLiveness: public ILivenessContext {
public:
    explicit TLogicalLiveness(TPlanProps& props)
        : Props(props)
        , LiveOut(props.LiveOut) {
    }

    void Run(TOpRoot& root) {
        LiveOut.clear();

        TVector<TInfoUnit> rootColumns;
        rootColumns.reserve(root.ColumnOrder.size());
        for (const auto& column : root.ColumnOrder) {
            rootColumns.emplace_back(column);
        }

        AddLiveColumns(root.GetInput(), rootColumns);
        Propagate();
    }

    const TInfoUnitSet& GetLiveOut(IOperator* op) const override {
        const auto it = LiveOut.find(op);
        Y_ENSURE(it != LiveOut.end(), "Liveness requested for an operator that has no live output");
        return it->second;
    }

    bool AddLiveColumns(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& columns) override {
        bool changed = false;
        auto& live = LiveOut[op.get()];
        for (const auto& column : columns) {
            changed |= AddLiveColumn(live, column);
        }
        if (changed) {
            Enqueue(op);
        }
        return changed;
    }

    bool AddLiveColumns(const TIntrusivePtr<IOperator>& op, const TInfoUnitSet& columns) override {
        bool changed = false;
        auto& live = LiveOut[op.get()];
        for (const auto& column : columns) {
            changed |= AddLiveColumn(live, column);
        }
        if (changed) {
            Enqueue(op);
        }
        return changed;
    }

    void AddExpressionDeps(const TExpression& expr, TInfoUnitSet& target) override {
        AddColumnsToSet(target, expr.GetInputIUs(false, true));

        for (const auto& iu : expr.GetInputIUs(true, false)) {
            if (!iu.IsSubplanContext()) {
                continue;
            }

            const auto it = Props.Subplans.PlanMap.find(iu);
            if (it == Props.Subplans.PlanMap.end()) {
                continue;
            }

            AddLiveColumns(CastOperator<IOperator>(it->second.Plan), CastOperator<IOperator>(it->second.Plan)->GetOutputIUs());
        }
    }

private:
    void Enqueue(const TIntrusivePtr<IOperator>& op) {
        if (Queued.insert(op.get()).second) {
            Queue.push_back(op);
        }
    }

    void Propagate() {
        for (size_t index = 0; index < Queue.size(); ++index) {
            auto op = Queue[index];
            Queued.erase(op.get());
            op->PropagateLiveness(*this);
        }
        Queue.clear();
    }

    TPlanProps& Props;
    THashMap<IOperator*, TInfoUnitSet>& LiveOut;
    THashSet<IOperator*> Queued;
    TVector<TIntrusivePtr<IOperator>> Queue;
};

TVector<TMapElement> KeepLiveMapElements(const TIntrusivePtr<TOpMap>& map, const TInfoUnitSet& liveOut) {
    TVector<TMapElement> newElements;
    newElements.reserve(map->MapElements.size());

    for (const auto& mapElement : map->MapElements) {
        if (mapElement.IsRename() || liveOut.contains(mapElement.GetElementName())) {
            newElements.push_back(mapElement);
        }
    }

    return newElements;
}

TVector<TInfoUnit> KeepLiveColumns(const TVector<TInfoUnit>& columns, const TInfoUnitSet& liveOut) {
    TVector<TInfoUnit> newColumns;
    newColumns.reserve(columns.size());

    for (const auto& column : columns) {
        if (liveOut.contains(column)) {
            newColumns.push_back(column);
        }
    }

    return newColumns;
}

void AddReadColumnByName(const TOpRead& read, const TString& columnName, TInfoUnitSet& requiredColumns) {
    for (const auto& outputIU : read.OutputIUs) {
        if (outputIU.GetFullName() == columnName || outputIU.GetColumnName() == columnName) {
            AddLiveColumn(requiredColumns, outputIU);
        }
    }
}

void AddOlapFilterConditionDeps(const TOpRead& read, const TExprNode::TPtr& node, TInfoUnitSet& requiredColumns) {
    if (!node) {
        return;
    }

    if (const auto maybeFilter = TMaybeNode<TKqpOlapFilter>(node)) {
        const auto filter = maybeFilter.Cast();
        AddOlapFilterConditionDeps(read, filter.Input().Ptr(), requiredColumns);
        AddOlapFilterConditionDeps(read, filter.Condition().Ptr(), requiredColumns);
        return;
    }

    if (const auto maybeColumnArg = TMaybeNode<TKqpOlapApplyColumnArg>(node)) {
        AddReadColumnByName(read, TString(maybeColumnArg.Cast().ColumnName().StringValue()), requiredColumns);
        return;
    }

    if (const auto maybeJsonValue = TMaybeNode<TKqpOlapJsonValue>(node)) {
        AddReadColumnByName(read, TString(maybeJsonValue.Cast().Column().StringValue()), requiredColumns);
        return;
    }

    if (const auto maybeJsonExists = TMaybeNode<TKqpOlapJsonExists>(node)) {
        AddReadColumnByName(read, TString(maybeJsonExists.Cast().Column().StringValue()), requiredColumns);
        return;
    }

    if (const auto maybeExists = TMaybeNode<TKqpOlapFilterExists>(node)) {
        AddReadColumnByName(read, TString(maybeExists.Cast().Column().StringValue()), requiredColumns);
        return;
    }

    if (const auto maybeUnaryOp = TMaybeNode<TKqpOlapFilterUnaryOp>(node)) {
        AddOlapFilterConditionDeps(read, maybeUnaryOp.Cast().Arg().Ptr(), requiredColumns);
        return;
    }

    if (const auto maybeBinaryOp = TMaybeNode<TKqpOlapFilterBinaryOp>(node)) {
        const auto binaryOp = maybeBinaryOp.Cast();
        AddOlapFilterConditionDeps(read, binaryOp.Left().Ptr(), requiredColumns);
        AddOlapFilterConditionDeps(read, binaryOp.Right().Ptr(), requiredColumns);
        return;
    }

    if (const auto maybeTernaryOp = TMaybeNode<TKqpOlapFilterTernaryOp>(node)) {
        const auto ternaryOp = maybeTernaryOp.Cast();
        AddOlapFilterConditionDeps(read, ternaryOp.First().Ptr(), requiredColumns);
        AddOlapFilterConditionDeps(read, ternaryOp.Second().Ptr(), requiredColumns);
        AddOlapFilterConditionDeps(read, ternaryOp.Third().Ptr(), requiredColumns);
        return;
    }

    if (const auto maybeNot = TMaybeNode<TKqpOlapNot>(node)) {
        AddOlapFilterConditionDeps(read, maybeNot.Cast().Value().Ptr(), requiredColumns);
        return;
    }

    if (TKqpOlapAnd::Match(node.Get()) || TKqpOlapOr::Match(node.Get()) || TKqpOlapXor::Match(node.Get())) {
        for (const auto& child : node->ChildrenList()) {
            AddOlapFilterConditionDeps(read, child, requiredColumns);
        }
        return;
    }

    if (const auto maybeAtom = TMaybeNode<TCoAtom>(node)) {
        AddReadColumnByName(read, TString(maybeAtom.Cast().StringValue()), requiredColumns);
    }
}

void AddReadLambdaDeps(const TOpRead& read, const TExprNode::TPtr& lambda, TInfoUnitSet& requiredColumns) {
    if (!lambda) {
        return;
    }

    const auto hasOlapProjections = FindNode(lambda, [](const TExprNode::TPtr& node) {
        return !!TMaybeNode<TKqpOlapProjections>(node);
    });
    if (hasOlapProjections) {
        AddColumnsToSet(requiredColumns, read.OutputIUs);
        return;
    }

    const auto members = FindNodes(lambda, [](const TExprNode::TPtr& node) {
        return !!TMaybeNode<TCoMember>(node);
    });
    for (const auto& member : members) {
        AddReadColumnByName(read, TString(TCoMember(member).Name().StringValue()), requiredColumns);
    }

    const auto filters = FindNodes(lambda, [](const TExprNode::TPtr& node) {
        return !!TMaybeNode<TKqpOlapFilter>(node);
    });
    for (const auto& filter : filters) {
        AddOlapFilterConditionDeps(read, filter, requiredColumns);
    }

    const auto applyArgs = FindNodes(lambda, [](const TExprNode::TPtr& node) {
        return !!TMaybeNode<TKqpOlapApplyColumnArg>(node);
    });
    for (const auto& applyArg : applyArgs) {
        AddOlapFilterConditionDeps(read, applyArg, requiredColumns);
    }
}

bool NarrowReadColumns(const TIntrusivePtr<TOpRead>& read, const TVector<TInfoUnit>& liveOutput) {
    TInfoUnitSet requiredColumns;
    AddColumnsToSet(requiredColumns, liveOutput);
    AddReadLambdaDeps(*read, read->OlapFilterLambda, requiredColumns);

    TVector<TString> newColumns;
    TVector<TInfoUnit> newOutputIUs;
    newColumns.reserve(read->Columns.size());
    newOutputIUs.reserve(read->OutputIUs.size());

    Y_ENSURE(read->Columns.size() == read->OutputIUs.size());
    for (size_t i = 0; i < read->OutputIUs.size(); ++i) {
        if (requiredColumns.contains(read->OutputIUs[i])) {
            newColumns.push_back(read->Columns[i]);
            newOutputIUs.push_back(read->OutputIUs[i]);
        }
    }

    if (newOutputIUs.empty() && read->GetTableStorageType() == NYql::EStorageType::ColumnStorage && !read->OutputIUs.empty()) {
        newColumns.push_back(read->Columns.front());
        newOutputIUs.push_back(read->OutputIUs.front());
    }

    if (newOutputIUs == read->OutputIUs) {
        return false;
    }

    read->Columns = std::move(newColumns);
    read->OutputIUs = std::move(newOutputIUs);
    return true;
}

bool PruneAggregateTraits(const TIntrusivePtr<TOpAggregate>& aggregate, const TVector<TInfoUnit>& liveOutput) {
    TInfoUnitSet liveOutputSet;
    AddColumnsToSet(liveOutputSet, liveOutput);

    TVector<TOpAggregationTraits> newTraits;
    newTraits.reserve(aggregate->AggregationTraitsList.size());
    for (const auto& traits : aggregate->AggregationTraitsList) {
        if (liveOutputSet.contains(traits.ResultColName)) {
            newTraits.push_back(traits);
        }
    }

    if (newTraits.size() == aggregate->AggregationTraitsList.size()) {
        return false;
    }

    aggregate->AggregationTraitsList = std::move(newTraits);
    return true;
}

bool CanOverrideOutput(EOperator kind) {
    switch (kind) {
        case EOperator::Map:
        case EOperator::Filter:
        case EOperator::Join:
        case EOperator::Aggregate:
        case EOperator::Limit:
        case EOperator::Sort:
        case EOperator::UnionAll:
            return true;
        default:
            return false;
    }
}

} // anonymous namespace

void ComputePlanLiveness(TOpRoot& root) {
    TLogicalLiveness(root.PlanProps).Run(root);
}

bool TPruneDeadMapElementsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(input);
    const auto liveIt = props.LiveOut.find(map.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    auto newElements = KeepLiveMapElements(map, liveIt->second);
    if (newElements.size() == map->MapElements.size()) {
        return false;
    }

    if (newElements.empty()) {
        input = map->GetInput();
    } else {
        map->MapElements = std::move(newElements);
    }

    return true;
}

TNarrowByLivenessStage::TNarrowByLivenessStage()
    : IRBOStage("Narrow by liveness") {
    Props = ERuleProperties::RequireParents;
}

void TNarrowByLivenessStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    Y_UNUSED(ctx);

    for (const auto& iter : root) {
        iter.Current->ClearOutputIUsOverride();
    }

    ComputePlanLiveness(root);

    THashMap<IOperator*, TVector<TInfoUnit>> liveOutputs;
    for (const auto& iter : root) {
        const auto liveIt = root.PlanProps.LiveOut.find(iter.Current.get());
        if (liveIt == root.PlanProps.LiveOut.end()) {
            continue;
        }

        liveOutputs[iter.Current.get()] = KeepLiveColumns(iter.Current->GetOutputIUs(), liveIt->second);
    }

    for (const auto& iter : root) {
        auto op = iter.Current;
        const auto outputIt = liveOutputs.find(op.get());
        if (outputIt == liveOutputs.end()) {
            continue;
        }

        const auto& liveOutput = outputIt->second;
        if (op->Kind == EOperator::Source) {
            NarrowReadColumns(CastOperator<TOpRead>(op), liveOutput);
            continue;
        }

        if (op->Kind == EOperator::Aggregate) {
            PruneAggregateTraits(CastOperator<TOpAggregate>(op), liveOutput);
        }

        if (!CanOverrideOutput(op->Kind)) {
            continue;
        }

        if (op->GetOutputIUs() != liveOutput) {
            op->SetOutputIUsOverride(liveOutput);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
