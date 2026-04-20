#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool IsValidLimit(const TExpression& expression) {
    return expression.Node && !!TMaybeNode<TCoUint64>(expression.Node->ChildPtr(1));
}

bool CanPropagateSortOverInput(const TIntrusivePtr<IOperator>& input) {
    const auto kind = input->GetKind();
    if (kind != EOperator::Map || !input->IsSingleConsumer()) {
        return false;
    }

    const auto nextStageId = *(input->Props.StageId);
    const auto prevStageId = *(CastOperator<TOpMap>(input)->GetInput()->Props.StageId);
    // Not pushing if the map is not single in the stage.
    if (nextStageId == prevStageId) {
        return false;
    }

    const auto& mapElements = CastOperator<TOpMap>(input)->GetMapElements();
    return std::all_of(mapElements.begin(), mapElements.end(), [](const TMapElement& mapElement) { return mapElement.IsRename(); });
}

bool CanPushSortToStage(const TIntrusivePtr<TOpSort>& sort, const TIntrusivePtr<IOperator>& input) {
    const auto sortStageId = *sort->Props.StageId;
    const auto inputStageId = *input->Props.StageId;
    return !(sortStageId == inputStageId || !input->IsSingleConsumer() ||
             (input->GetKind() == EOperator::Source && CastOperator<TOpRead>(input)->GetTableStorageType() == NYql::EStorageType::RowStorage));
}

bool IsSuitableToPropagateTopSortThroughStage(const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Sort) {
        return false;
    }

    const auto type = input->GetTypeAnn();
    Y_ENSURE(type);
    const auto items = type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems();
    if (!(items.size() && items.front()->GetItemType()->GetKind() != ETypeAnnotationKind::Pg)) {
        return false;
    }

    const auto sort = CastOperator<TOpSort>(input);
    return sort->IsTopSort() && sort->GetSortPhase() != EOpPhase::Final;
}

TIntrusivePtr<TOpLimit> EmitFinalAndIntermediateOperators(const TIntrusivePtr<TOpSort>& sort) {
    const auto limitCond = *sort->LimitCond;
    const auto pos = sort->Pos;
    const auto props = sort->Props;
    const auto sortElements = sort->GetSortElements();
    const auto intermediate = MakeIntrusive<TOpSort>(sort->GetInput(), pos, props, sortElements, limitCond, EOpPhase::Intermediate);
    return MakeIntrusive<TOpLimit>(intermediate, pos, props, limitCond, EOpPhase::Final);
}

bool CanPropagateOverConnection(const ui32 prevStageId, const ui32 currentStageId, TPlanProps& props) {
    const auto connections = props.StageGraph.GetConnections(prevStageId, currentStageId);
    Y_ENSURE(connections.size() == 1);
    const auto connection = connections.front();
    return (prevStageId != currentStageId && IsConnection<TUnionAllConnection>(connection));
}

void MaybePushToStageAndUpdateConnection(TIntrusivePtr<TOpSort>& sort, const TIntrusivePtr<IOperator>& input, TPlanProps& props) {
    const auto prevStageId = *(input->Props.StageId);
    const auto currentStageId = *(sort->Props.StageId);
    if (CanPropagateOverConnection(prevStageId, currentStageId, props)) {
        // Update conection type.
        props.StageGraph.UpdateConnection(prevStageId, currentStageId,
                                          MakeIntrusive<TMergeConnection>(sort->GetSortElements(), props.StageGraph.GetStorageType(prevStageId)));
        // Push to stage.
        sort->Props.StageId = prevStageId;
    }
}

void MaybeUpdateSortElements(TVector<TSortElement>& sortElements, const TVector<TMapElement>& mapElements) {
    THashMap<TString, TInfoUnit> map;
    for (const auto& mapElement : mapElements) {
        Y_ENSURE(mapElement.IsRename());
        map[mapElement.GetElementName().GetFullName()] = mapElement.GetRename();
    }

    for (auto& sortElement : sortElements) {
        const auto fullName = sortElement.SortColumn.GetFullName();
        const auto it = map.find(fullName);
        if (it != map.end()) {
            sortElement.SortColumn = it->second;
        }
    }
}

void StripAliasFromSortElements(TVector<TSortElement>& sortElements, const TString& alias) {
    for (auto& sortElement : sortElements) {
        const auto sortColName = sortElement.SortColumn;
        const auto sortAlias = sortColName.GetAlias();
        if (sortAlias == alias) {
            // strip alias
            sortElement.SortColumn = TInfoUnit(sortColName.GetColumnName());
        }
    }
}

bool NeedsToStripAliasFromSort(const TIntrusivePtr<IOperator>& input) {
    return input->GetKind() == EOperator::Source && CastOperator<TOpRead>(input)->NeedsMap();
}

bool CanPushSortToOlapRead(const TIntrusivePtr<TOpSort>& sort, const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, ui32& sortDirection) {
    if (input->GetKind() != EOperator::Source) {
        return false;
    }
    const auto& read = CastOperator<TOpRead>(input);
    // If not a columnstore or already set.
    if (read->GetTableStorageType() != NYql::EStorageType::ColumnStorage || read->Limit) {
        return false;
    }

    const auto& sortElements = sort->GetSortElements();
    std::for_each(sortElements.begin(), sortElements.end(), [&sortDirection](const TSortElement& sortElement) {
        sortDirection = sortElement.Ascending ? static_cast<ui32>(ESortDir::Asc) : static_cast<ui32>(ESortDir::Desc);
    });

    // All keys should have the same sort direction.
    if (sortDirection != ESortDir::Asc && sortDirection != ESortDir::Desc) {
        return false;
    }

    const auto tablePath = TExprBase(read->GetTable()).Cast<TKqpTable>().Path().StringValue();
    auto& kqpCtx = ctx.KqpCtx;
    auto table = kqpCtx.Tables->EnsureTableExists(kqpCtx.Cluster, tablePath, read->Pos, ctx.ExprCtx);
    if (!table) {
        return false;
    }

    auto metadataPtr = table->Metadata;
    if (!metadataPtr) {
        return false;
    }

    // Only keys are allowed.
    const auto& keyColumns = metadataPtr->KeyColumnNames;
    THashSet<TString> keys(keyColumns.begin(), keyColumns.end());
    for (const auto& sortElement : sortElements) {
        if (!keys.contains(sortElement.SortColumn.GetColumnName())) {
            return false;
        }
    }

    return IsValidLimit(*sort->LimitCond);
}

} // namespace

TIntrusivePtr<IOperator> TPropagateTopSortThroughStageRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (!IsSuitableToPropagateTopSortThroughStage(input)) {
        return input;
    }

    auto sort = CastOperator<TOpSort>(input);
    if (sort->GetSortPhase() == EOpPhase::Undefined) {
        return EmitFinalAndIntermediateOperators(sort);
    }
    Y_ENSURE(sort->GetSortPhase() == EOpPhase::Intermediate);
    const auto sortInput = sort->GetInput();
    ui32 sortDirecion = ESortDir::None;

    if (CanPushSortToStage(sort, sortInput)) {
        if (NeedsToStripAliasFromSort(sortInput)) {
            StripAliasFromSortElements(sort->GetSortElements(), CastOperator<TOpRead>(sortInput)->Alias);
        }
        MaybePushToStageAndUpdateConnection(sort, sortInput, props);
        return MakeIntrusive<TOpSort>(sortInput, sort->Pos, sort->Props, sort->GetSortElements(), sort->LimitCond, EOpPhase::Intermediate);
    } else if (CanPropagateSortOverInput(sortInput)) {
        const auto map = CastOperator<TOpMap>(sortInput);
        TVector<TSortElement> sortElements = sort->GetSortElements();
        const auto mapElements = map->GetMapElements();
        // If map renames a sort element, update it.
        MaybeUpdateSortElements(sortElements, mapElements);
        const auto propagatedSort = MakeIntrusive<TOpSort>(map->GetInput(), sort->Pos, sort->Props, sortElements, sort->LimitCond, EOpPhase::Intermediate);
        const auto newMap = MakeIntrusive<TOpMap>(propagatedSort, map->Pos, map->Props, mapElements, map->Project, map->Ordered);
        return MakeIntrusive<TOpSort>(newMap, sort->Pos, sort->Props, sort->GetSortElements(), sort->LimitCond, EOpPhase::Final);
    } else if (CanPushSortToOlapRead(sort, sortInput, ctx, sortDirecion)) {
        auto read = CastOperator<TOpRead>(sortInput);
        const auto limitCond = sort->LimitCond->Node->ChildPtr(1);
        return MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->OutputIUs, read->StorageType, read->TableCallable, read->OlapFilterLambda, limitCond,
                                      read->GetRanges(), read->OriginalPredicate, static_cast<ESortDir>(sortDirecion), read->Props, read->Pos);
    }

    return input;
}
} // namespace NKqp
} // namespace NKikimr