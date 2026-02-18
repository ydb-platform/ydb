#include "kqp_operator.h"
#include "kqp_expression.h"
#include "kqp_rbo_utils.h"
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

/**
 * Base class Operator methods
 */

void IOperator::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                          const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(renameMap);
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
}

const TTypeAnnotationNode* IOperator::GetIUType(const TInfoUnit& iu) {
    auto structType = Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    return structType->FindItemType(iu.GetFullName());
}

void IOperator::ReplaceChild(const TIntrusivePtr<IOperator> oldChild, const TIntrusivePtr<IOperator> newChild) {
    for (size_t i = 0; i < Children.size(); i++) {
        if (Children[i] == oldChild) {
            Children[i] = newChild;
            return;
        }
    }
    Y_ENSURE(false, "Did not find a child to replace");
}

/**
 * EmptySource operator methods
 */

TString TOpEmptySource::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx); 
    return "EmptySource"; 
}

/**
 * OpRead operator methods
 */

TOpRead::TOpRead(TExprNode::TPtr node)
    : IOperator(EOperator::Source, node->Pos()) {
    auto opSource = TKqpOpRead(node);

    const auto alias = opSource.Alias().StringValue();
    for (const auto& column : opSource.Columns()) {
        Columns.push_back(column.StringValue());
        OutputIUs.push_back(TInfoUnit(alias, column.StringValue()));
    }

    Alias = alias;
    StorageType = opSource.SourceType().StringValue() == "Row" ? NYql::EStorageType::RowStorage : NYql::EStorageType::ColumnStorage;
    TableCallable = opSource.Table().Ptr();
}

TOpRead::TOpRead(const TString& alias, const TVector<TString>& columns, const TVector<TInfoUnit>& outputIUs, const NYql::EStorageType storageType,
                 const TExprNode::TPtr& tableCallable, const TExprNode::TPtr& olapFilterLambda, TPositionHandle pos)
    : IOperator(EOperator::Source, pos)
    , Alias(alias)
    , Columns(columns)
    , OutputIUs(outputIUs)
    , StorageType(storageType)
    , TableCallable(tableCallable)
    , OlapFilterLambda(olapFilterLambda) {
}

TVector<TInfoUnit> TOpRead::GetOutputIUs() {
    return OutputIUs;
}

bool TOpRead::NeedsMap() {
    for (size_t i=0; i<Columns.size(); i++) {
        if (TInfoUnit("", Columns[i]) != OutputIUs[i]) {
            return true;
        }
    }
    return false;
}

void TOpRead::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                        const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);

    for (auto& column : OutputIUs) {
        const auto it = renameMap.find(column);
        if (it != renameMap.end()) {
            column = it->second;
        }
    }
}

NYql::EStorageType TOpRead::GetTableStorageType() const {
    return StorageType;
}

TString TOpRead::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    auto res = TStringBuilder();
    res << "Read (" << TKqpTable(TableCallable).Path().StringValue() << "," << Alias << ", [";

    for (size_t i = 0; i < Columns.size(); i++) {
        res << Columns[i] << "->" << OutputIUs[i].GetFullName();
        if (i != Columns.size() - 1) {
            res << ", ";
        }
    }
    res << "])";
    const TString storageType = StorageType == NYql::EStorageType::RowStorage ? "Row" : "Column";
    res << " (StorageType: " << storageType << ")";
    if (OlapFilterLambda)
        res << " OlapFilter: (" << PrintRBOExpression(OlapFilterLambda, ctx) << ")";
    return res;
}

TMapElement::TMapElement(const TInfoUnit& elementName, const TExpression& expr)
    : ElementName(elementName)
    , Expr(expr) {
}

TMapElement::TMapElement(const TInfoUnit& elementName, const TInfoUnit& rename, TPositionHandle pos, TExprContext* ctx, TPlanProps* props)
    : ElementName(elementName)
    , Expr(MakeColumnAccess(rename, pos, ctx, props)) {
}

bool TMapElement::IsRename() const {
    return Expr.IsColumnAccess();
}

TInfoUnit TMapElement::GetElementName() const {
    return ElementName;
}

TExpression TMapElement::GetExpression() const {
    return Expr;
}

TExpression& TMapElement::GetExpressionRef() {
    return Expr;
}

TInfoUnit TMapElement::GetRename() const {
    auto IUs = Expr.GetInputIUs(true);
    Y_ENSURE(IUs.size()==1, "No or multiple column references in rename");
    return IUs[0];
}

void TMapElement::SetExpression(TExpression expr) {
    Expr = expr;
}

/**
 * OpMap operator methods
 */
TOpMap::TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TMapElement>& mapElements, bool project, bool ordered)
    : IUnaryOperator(EOperator::Map, pos, input)
    , MapElements(mapElements)
    , Project(project)
    , Ordered(ordered) {
}

TVector<TInfoUnit> TOpMap::GetOutputIUs() {
    TVector<TInfoUnit> res;
    if (!Project) {
        res = GetInput()->GetOutputIUs();
    }

    for (const auto &mapElement : MapElements) {
        res.push_back(mapElement.GetElementName());
    }

    return res;
}

TVector<TInfoUnit> TOpMap::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);

    TVector<TInfoUnit> result;

    for (auto expr : GetExpressions()) {
        auto usedIUs = expr.get().GetInputIUs(false, true);
        AddUnique<TInfoUnit>(usedIUs, result);
    }

    return result;
}

TVector<std::reference_wrapper<TExpression>> TOpMap::GetExpressions() {
    TVector<std::reference_wrapper<TExpression>> result;
    for (auto& mapElement : MapElements) {
        result.push_back(mapElement.GetExpressionRef());
    }
    return result;
}

TVector<std::reference_wrapper<TExpression>> TOpMap::GetComplexExpressions() {
    TVector<std::reference_wrapper<TExpression>> result;
    for (auto& mapElement : MapElements) {
        if (!mapElement.IsRename()) {
            result.push_back(mapElement.GetExpressionRef());
        }
    }
    return result;
}

TVector<TInfoUnit> TOpMap::GetSubplanIUs(TPlanProps& props) {
    Y_UNUSED(props);
    TVector<TInfoUnit> subplanIUs;
    TVector<TInfoUnit> res;

    for (const auto& mapElement : MapElements) {
        auto vars = mapElement.GetExpression().GetInputIUs(true, false);
        for (const auto& iu : vars) {
            if (iu.IsSubplanContext()) {
                subplanIUs.push_back(iu);
            }
        }
    }

    AddUnique<TInfoUnit>(res, subplanIUs);
    return res;
}

// Returns explicit renames as pairs of <to, from>

TVector<std::pair<TInfoUnit, TInfoUnit>> TOpMap::GetRenames() const {
    TVector<std::pair<TInfoUnit, TInfoUnit>> result;
    for (const auto& mapElement : MapElements) {
        if (mapElement.IsRename()) {
            result.push_back(std::make_pair(mapElement.GetElementName(), mapElement.GetRename()));
        }
    }
    return result;
}

// Add simple transformations that preserve both key and column statistics properties
// Currently only pg transformations FromPg and ToPg are supported
// Return pairs of renames <to, from>

TVector<std::pair<TInfoUnit, TInfoUnit>> TOpMap::GetRenamesWithTransforms(TPlanProps& props) const {
    Y_UNUSED(props);
    auto result = GetRenames();

    for (const auto &mapElement : MapElements) {
        if (!mapElement.IsRename()) {
            auto expr = mapElement.GetExpression();
            auto node = expr.Node;

            if (node->IsCallable("ToPg") || node->IsCallable("FromPg")) {
                if (node->ChildPtr(0)->IsCallable("Member")) {
                    auto transformIUs = expr.GetInputIUs();
                    if (transformIUs.size() == 1) {
                        result.push_back(std::make_pair(mapElement.GetElementName(), transformIUs[0]));
                    }
                }
            }
        }
    }

    return result;
}

void TOpMap::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                       const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    TVector<TMapElement> newMapElements;

    for (const auto& el : MapElements) {
        TInfoUnit newIU = el.GetElementName();
        const auto it = renameMap.find(newIU);
        if (it != renameMap.end()) {
            newIU = it->second;
        }

        if (el.IsRename()) {
            auto expr = el.GetExpression();
            auto from = el.GetRename();
            if (renameMap.contains(from) && !stopList.contains(from)) {
                from = renameMap.at(from);
            }
            newMapElements.emplace_back(newIU, MakeColumnAccess(from, Pos, expr.Ctx, expr.PlanProps));
        } else {
            auto expr = el.GetExpression();
            auto newBody = expr.ApplyRenames(renameMap);
            newMapElements.emplace_back(newIU, newBody);
        }
    }
    MapElements = std::move(newMapElements);
}

void TOpMap::ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) {
    TOptimizeExprSettings settings(&ctx.TypeCtx);
    for (size_t i = 0; i < MapElements.size(); i++) {
        if (!MapElements[i].IsRename()) {
            auto expr = MapElements[i].GetExpression();
            MapElements[i].SetExpression(expr.ApplyReplaceMap(map, ctx));
        }
    }
}

TString TOpMap::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);

    auto res = TStringBuilder();
    res << "Map [";
    for (size_t i = 0, e = MapElements.size(); i < e; i++) {
        const auto& mapElement = MapElements[i];
        const auto& k = mapElement.GetElementName();

        if (mapElement.IsRename()) {
            res << mapElement.GetRename().GetFullName();
        } else {
            res << mapElement.GetExpression().ToString();
        }
        res << "->" << k.GetFullName();

        if (i != e - 1) {
            res << ", ";
        }
    }
    res << "]";
    if (Project) {
        res << " Project";
    }
    return res;
}

/**
 * OpAddDependencies methods
 */
TOpAddDependencies::TOpAddDependencies(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TInfoUnit>& columns,
                                       const TVector<const TTypeAnnotationNode*>& types)
    : IUnaryOperator(EOperator::AddDependencies, pos, input)
    , Dependencies(columns)
    , Types(types) {
}

TVector<TInfoUnit> TOpAddDependencies::GetOutputIUs() {
    auto ius = GetInput()->GetOutputIUs();
    ius.insert(ius.end(), Dependencies.begin(), Dependencies.end());
    return ius;
}

TString TOpAddDependencies::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    
    auto res = TStringBuilder();
    res << "Correlated [";
    for (size_t i=0; i<Dependencies.size(); i++) {
        res << Dependencies[i].GetFullName();
        if (i!=Dependencies.size()-1) {
            res << ",";
        }
    }
    res << "]";
    return res;
}

/**
 * OpProject methods
 */

TOpProject::TOpProject(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TInfoUnit>& projectList)
    : IUnaryOperator(EOperator::Project, pos, input), ProjectList(projectList) {}

TVector<TInfoUnit> TOpProject::GetOutputIUs() {
    TVector<TInfoUnit> res;

    for (const auto& projection : ProjectList) {
        res.push_back(projection);
    }
    return res;
}

void TOpProject::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                           const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);

    for (auto& projection : ProjectList) {
        auto it = renameMap.find(projection);
        if (it != renameMap.end()) {
            projection = it->second;
        }
    }
}

TString TOpProject::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    auto res = TStringBuilder();
    res << "Project [";
    for (size_t i = 0; i < ProjectList.size(); i++) {
        const auto& iu = ProjectList[i];
        res << iu.GetFullName();
        if (i != ProjectList.size()-1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

/**
 * OpFilter operator methods
 */

TOpFilter::TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& filterExpr)
    : IUnaryOperator(EOperator::Filter, pos, input)
    , FilterExpr(filterExpr) {
}

TVector<TInfoUnit> TOpFilter::GetOutputIUs() { return GetInput()->GetOutputIUs(); }

void TOpFilter::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx, const THashSet<TInfoUnit, TInfoUnit::THashFunction> &stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
    FilterExpr = FilterExpr.ApplyRenames(renameMap);
}

TVector<std::reference_wrapper<TExpression>> TOpFilter::GetExpressions() {
    return {FilterExpr};
}

void TOpFilter::ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext & ctx) {
    TOptimizeExprSettings settings(&ctx.TypeCtx);
    FilterExpr.ApplyReplaceMap(map, ctx);
}

TVector<TInfoUnit> TOpFilter::GetFilterIUs(TPlanProps& props) const {
    Y_UNUSED(props);
    return FilterExpr.GetInputIUs(true, true);
}

TVector<TInfoUnit> TOpFilter::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);
    return FilterExpr.GetInputIUs(false, true);
}

TVector<TInfoUnit> TOpFilter::GetSubplanIUs(TPlanProps& props) {
    TVector<TInfoUnit> res;
    for (const auto& iu : GetFilterIUs(props)) {
        if (iu.IsSubplanContext()) {
            res.push_back(iu);
        }
    }
    return res;
}

TString TOpFilter::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    return TStringBuilder() << "Filter :" << FilterExpr.ToString();
}

/**
 * OpJoin operator methods
 */

TOpJoin::TOpJoin(TIntrusivePtr<IOperator> leftInput, TIntrusivePtr<IOperator> rightInput, TPositionHandle pos, TString joinKind,
                 const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys)
    : IBinaryOperator(EOperator::Join, pos, leftInput, rightInput), JoinKind(joinKind), JoinKeys(joinKeys) {}

TVector<TInfoUnit> TOpJoin::GetOutputIUs() {
    TVector<TInfoUnit> res;

    auto leftInputIUs = GetLeftInput()->GetOutputIUs();
    auto rightInputIUs = GetRightInput()->GetOutputIUs();

    if (JoinKind == "LeftOnly" || JoinKind == "LeftSemi") {
        rightInputIUs = {};
    }
    if (JoinKind == "RightOnly" || JoinKind == "RightSemi") {
        leftInputIUs = {};
    }

    res.insert(res.end(), leftInputIUs.begin(), leftInputIUs.end());
    res.insert(res.end(), rightInputIUs.begin(), rightInputIUs.end());
    return res;
}

TVector<TInfoUnit> TOpJoin::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);
    TVector<TInfoUnit> result;
    for (const auto& [leftKey, rightKey]: JoinKeys) {
        result.push_back(leftKey);
        result.push_back(rightKey);
    }
    return result;
}

void TOpJoin::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                        const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);

    for (auto& k : JoinKeys) {
        if (renameMap.contains(k.first)) {
            k.first = renameMap.at(k.first);
        }
        if (renameMap.contains(k.second)) {
            k.second = renameMap.at(k.second);
        }
    }
}

const THashMap<EJoinAlgoType,TString> AlgoNames = {
    {EJoinAlgoType::LookupJoin, "Lookup"},
    {EJoinAlgoType::LookupJoinReverse, "ReverseLookup"},
    {EJoinAlgoType::MapJoin, "Map"},
    {EJoinAlgoType::GraceJoin, "Shuffle"}};

TString TOpJoin::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << "Join, Kind: " << JoinKind;
    if (Props.JoinAlgo.has_value()) {
        res << ", Algo: " << AlgoNames.at(*Props.JoinAlgo);
    }
    res << " [";
    for (size_t i = 0; i < JoinKeys.size(); i++) {
        auto [x,y] = JoinKeys[i];
        res << x.GetFullName() + "=" + y.GetFullName();
        if (i != JoinKeys.size() - 1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

/**
 * OpUnionAll operator methods
 */

TOpUnionAll::TOpUnionAll(TIntrusivePtr<IOperator> leftInput, TIntrusivePtr<IOperator> rightInput, TPositionHandle pos, bool ordered)
    : IBinaryOperator(EOperator::UnionAll, pos, leftInput, rightInput), Ordered(ordered) {}

TVector<TInfoUnit> TOpUnionAll::GetOutputIUs() {
    return GetLeftInput()->GetOutputIUs();
}

TString TOpUnionAll::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx); 
    return "UnionAll";
}

/**
 * OpLimit operator methods
 */

TOpLimit::TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond)
    : IUnaryOperator(EOperator::Limit, pos, input), LimitCond(limitCond) {}

TVector<TInfoUnit> TOpLimit::GetOutputIUs() { return GetInput()->GetOutputIUs(); }

void TOpLimit::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx, const THashSet<TInfoUnit, TInfoUnit::THashFunction> &stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
    LimitCond.ApplyRenames(renameMap);
}

TString TOpLimit::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    return TStringBuilder() << "Limit: " << LimitCond.ToString(); 
}

/**
 * Sort operator
 * FIXME: This is temporary, we want to get enforcers working
 */
TOpSort::TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TSortElement>& sortElements, std::optional<TExpression> limitCond)
    : IUnaryOperator(EOperator::Sort, pos, input), SortElements(sortElements), LimitCond(limitCond) {}

TVector<TInfoUnit> TOpSort::GetOutputIUs() { return GetInput()->GetOutputIUs(); }

TVector<TInfoUnit> TOpSort::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);
    TVector<TInfoUnit> result;
    for (const auto& element : SortElements) {
        result.push_back(element.SortColumn);
    }
    return result;
}

void TOpSort::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx, const THashSet<TInfoUnit, TInfoUnit::THashFunction> &stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
    TVector<TSortElement> newSortElements;
    for (const auto& element : SortElements) {
        TInfoUnit newIU(element.SortColumn);

        const auto it = renameMap.find(newIU);
        if (it != renameMap.end()) {
            newIU = it->second;
        }

        auto sortElement = TSortElement(element);
        sortElement.SortColumn = newIU;
        newSortElements.push_back(sortElement);
    }

    if (LimitCond.has_value()) {
        LimitCond->ApplyRenames(renameMap);
    }
}

TString TOpSort::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << "Sort: [";
    for (size_t i = 0; i < SortElements.size(); i++) {
        auto el = SortElements[i];
        res << el.SortColumn.GetFullName() << " ";
        if (el.Ascending) {
            res << "asc";
        } else {
            res << "desc";
        }

        if (i != SortElements.size() - 1) {
            res << ", ";
        }
    }
    res << "]";
    if (LimitCond.has_value()) {
        res << ", Limit: " << LimitCond->ToString();
    }
    
    return res;
}

/**
 * OpAggregate operator methods
 */

TOpAggregate::TOpAggregate(TIntrusivePtr<IOperator> input, const TVector<TOpAggregationTraits>& aggTraitsList, const TVector<TInfoUnit>& keyColumns,
                           const EAggregationPhase aggPhase, bool distinctAll, TPositionHandle pos)
    : IUnaryOperator(EOperator::Aggregate, pos, input)
    , AggregationTraitsList(aggTraitsList)
    , KeyColumns(keyColumns)
    , AggregationPhase(aggPhase)
    , DistinctAll(distinctAll) {
}

TVector<TInfoUnit> TOpAggregate::GetOutputIUs() {
    // We assume that aggregation returns column is order [keys, states]
    TVector<TInfoUnit> outputIU = KeyColumns;
    for (const auto& aggTraits : AggregationTraitsList) {
        outputIU.push_back(aggTraits.ResultColName);
    }
    return outputIU;
}

TVector<TInfoUnit> TOpAggregate::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);
    TVector<TInfoUnit> usedIUs = KeyColumns;
    for (const auto& aggTraits : AggregationTraitsList) {
        usedIUs.push_back(aggTraits.OriginalColName);
    }
    return usedIUs;
}

void TOpAggregate::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                             const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);

    for (auto& column : KeyColumns) {
        const auto it = renameMap.find(column);
        if (it != renameMap.end()) {
            column = it->second;
        }
    }
    for (auto& trait : AggregationTraitsList) {
        if (renameMap.contains(trait.OriginalColName) && !stopList.contains(trait.OriginalColName)) {
            trait.OriginalColName = renameMap.at(trait.OriginalColName);
        }
        if (renameMap.contains(trait.ResultColName)) {
            trait.ResultColName = renameMap.at(trait.ResultColName);
        }
    }
}

TString TOpAggregate::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);

    TStringBuilder strBuilder;
    strBuilder << "Aggregate [";
    for (ui32 i = 0; i < AggregationTraitsList.size(); ++i) {
        strBuilder << AggregationTraitsList[i].AggFunction << "(" << AggregationTraitsList[i].OriginalColName.GetFullName() << ") as "
                   << AggregationTraitsList[i].ResultColName.GetFullName();
        if (i + 1 != AggregationTraitsList.size()) {
            strBuilder << ", ";
        }
    }

    strBuilder << " [";
    for (ui32 i = 0; i < KeyColumns.size(); ++i) {
        strBuilder << KeyColumns[i].GetFullName();
        if (i + 1 != KeyColumns.size()) {
            strBuilder << ", ";
        }
    }
    strBuilder << "]] ";
    if (AggregationPhase == EAggregationPhase::Intermediate) {
        strBuilder << "Initial";
    } else {
        strBuilder << "Final";
    }
    return strBuilder;
}

/***
 * OpCBOTree operator methods
 */
TOpCBOTree::TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TPositionHandle pos) :
    IOperator(EOperator::CBOTree, pos),
    TreeRoot(treeRoot),
    TreeNodes({treeRoot}) 
{
    Children = treeRoot->Children;
}

TOpCBOTree::TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TVector<TIntrusivePtr<IOperator>> treeNodes, TPositionHandle pos) :
    IOperator(EOperator::CBOTree, pos),
    TreeRoot(treeRoot),
    TreeNodes({treeNodes}) 
{
    for (const auto& n : treeNodes) {
        for (const auto& c : n->Children) {
            if (std::find(treeNodes.begin(), treeNodes.end(), c) == treeNodes.end()) {
                Children.push_back(c);
            }
        }
    }
}

void TOpCBOTree::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap, TExprContext &ctx, const THashSet<TInfoUnit, TInfoUnit::THashFunction> &stopList) {
    for (auto op : TreeNodes) {
        op->RenameIUs(renameMap, ctx, stopList);
    }
}

TString TOpCBOTree::ToString(TExprContext& ctx) {
    TStringBuilder res;
    res << "CBO Tree: [";
    for (size_t i=0; i < TreeNodes.size(); i++) {
        res << TreeNodes[i]->ToString(ctx);
        if (i != TreeNodes.size()-1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

/**
 * OpRoot operator methods
 */

TOpRoot::TOpRoot(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TString>& columnOrder)
    : IUnaryOperator(EOperator::Root, pos, input)
    , ColumnOrder(columnOrder) {
}

TVector<TInfoUnit> TOpRoot::GetOutputIUs() {
    return GetInput()->GetOutputIUs();
}

void TOpRoot::ComputeParentsRec(TIntrusivePtr<IOperator> op, TIntrusivePtr<IOperator> parent, ui32 parentChildIndex) const {
    if (parent) {
        const auto parentEntry = std::make_pair(parent.get(), parentChildIndex);
        const auto it = std::find_if(op->Parents.begin(), op->Parents.end(), [&parentEntry](const std::pair<IOperator*, ui32>& opParent) {
            return opParent.first == parentEntry.first && opParent.second == parentEntry.second;
        });
        if (it == op->Parents.end()) {
            op->Parents.push_back(parentEntry);
        }
    }
    for (size_t childIndex = 0; childIndex < op->Children.size(); ++childIndex) {
        ComputeParentsRec(op->Children[childIndex], op, childIndex);
    }
}

void TOpRoot::ComputeParents() {
    for (auto it : *this) {
        it.Current->Parents.clear();
    }
    TIntrusivePtr<TOpRoot> noParent = nullptr;
    ComputeParentsRec(GetInput(), noParent, 0);

    const auto subPlans = PlanProps.Subplans.Get();
    for (const auto& subPlan : subPlans) {
        ComputeParentsRec(CastOperator<IOperator>(subPlan.Plan), noParent, 0);
    }
}

TString TOpRoot::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    return "Root";
}

TString TOpRoot::PlanToString(TExprContext& ctx, ui32 printOptions) {
    auto builder = TStringBuilder();
    for (const auto& [iu, subplan] : PlanProps.Subplans.PlanMap) {
        builder << "Subplan binding to " << iu.GetFullName() << ":\n";
        PlanToStringRec(CastOperator<IOperator>(subplan.Plan), ctx, builder, 0, printOptions);
    }
    PlanToStringRec(GetInput(), ctx, builder, 0, printOptions);
    return builder;
}

void TOpRoot::PlanToStringRec(TIntrusivePtr<IOperator> op, TExprContext& ctx, TStringBuilder& builder, int tabs, ui32 printOptions) const {
    TStringBuilder tabString;
    for (int i = 0; i < tabs; i++) {
        tabString << "  ";
    }

    builder << tabString << op->ToString(ctx) << "\n";
    if (printOptions & (EPrintPlanOptions::PrintBasicMetadata | EPrintPlanOptions::PrintFullMetadata) && op->Props.Metadata.has_value()) {
        builder << tabString << " ";
        builder << op->Props.Metadata->ToString(printOptions) << "\n";
    }

    if (printOptions & (EPrintPlanOptions::PrintBasicStatistics | EPrintPlanOptions::PrintFullStatistics) && op->Props.Statistics.has_value()) {
        builder << tabString << " ";
        builder << op->Props.Statistics->ToString(printOptions);
        builder << ", Cost: " << (op->Props.Cost.has_value() ? std::to_string(*op->Props.Cost) : "None") << "\n";
    }

    for (auto c : op->Children) {
        PlanToStringRec(c, ctx, builder, tabs + 1, printOptions);
    }
}

} // namespace NKqp
} // namespace NKikimr