#include "kqp_operator.h"
#include "kqp_expression.h"
#include "kqp_rbo_utils.h"
#include <yql/essentials/core/yql_expr_optimize.h>

#include <algorithm>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

namespace {

const TString IgnoreArgPrefix = "__kqp_rbo_ignore_arg_";

bool IsGeneratedIgnoreName(const TInfoUnit& iu) {
    return iu.GetAlias().empty() && iu.GetColumnName().StartsWith(IgnoreArgPrefix);
}

TString FormatMapElementName(const TInfoUnit& iu) {
    return IsGeneratedIgnoreName(iu) ? "_" : iu.GetFullName();
}

TString FormatSortElements(const TVector<TSortElement>& sortElements) {
    TStringBuilder result;
    for (size_t i = 0; i < sortElements.size(); ++i) {
        if (i != 0) {
            result << ", ";
        }

        result << sortElements[i].ToString();
    }
    return result;
}

bool AddInfoUnit(TInfoUnitSet& target, const TInfoUnit& iu) {
    return target.insert(iu).second;
}

void AddInfoUnits(TInfoUnitSet& target, const TVector<TInfoUnit>& ius) {
    for (const auto& iu : ius) {
        AddInfoUnit(target, iu);
    }
}

TInfoUnitSet MakeInfoUnitSet(const TVector<TInfoUnit>& ius) {
    TInfoUnitSet result;
    AddInfoUnits(result, ius);
    return result;
}

} // namespace

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

NJson::TJsonValue IOperator::ToJson(ui32 explainFlags)
{
    Y_UNUSED(explainFlags);
    auto res = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
    res["Name"] = GetExplainName();
    return res;
}

void IOperator::PropagateLiveness(ILivenessContext& ctx) {
    Y_UNUSED(ctx);
}

void IUnaryOperator::PropagateLiveness(ILivenessContext& ctx) {
    const TInfoUnitSet liveOut = ctx.GetLiveOut(this);
    TInfoUnitSet inputLive;
    for (const auto& iu : GetInput()->GetOutputIUs()) {
        if (liveOut.contains(iu)) {
            AddInfoUnit(inputLive, iu);
        }
    }
    ctx.AddLiveColumns(GetInput(), inputLive);
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
                 const TExprNode::TPtr& tableCallable, const TExprNode::TPtr& olapFilterLambda, const TExprNode::TPtr& limit, const TExprNode::TPtr& ranges,
                 const std::optional<TExpression>& originalPredicate, const ESortDir sortDir, const TPhysicalOpProps& props, TPositionHandle pos)
    : IOperator(EOperator::Source, pos, props)
    , Alias(alias)
    , Columns(columns)
    , OutputIUs(outputIUs)
    , StorageType(storageType)
    , TableCallable(tableCallable)
    , OlapFilterLambda(olapFilterLambda)
    , Limit(limit)
    , Ranges(ranges)
    , OriginalPredicate(originalPredicate)
    , SortDir(sortDir) {
}

TVector<TInfoUnit> TOpRead::GetOutputIUs() {
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }
    return OutputIUs;
}

void TOpRead::PropagateLiveness(ILivenessContext& ctx) {
    Y_UNUSED(ctx);
}

bool TOpRead::NeedsMap() const {
    for (size_t i = 0; i < Columns.size(); i++) {
        if (TInfoUnit("", Columns[i]) != OutputIUs[i]) {
            return true;
        }
    }
    return false;
}

// FIXME: why does this function belong to op read?
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

static std::optional<TString> GetUint64Literal(const TExprNode::TPtr& node) {
    if (!node) {
        return std::nullopt;
    }

    if (auto maybeUint64 = TExprBase(node).Maybe<TCoUint64>()) {
        return TString(maybeUint64.Cast().Literal().Cast<TCoAtom>().Value());
    }

    return std::nullopt;
}

NJson::TJsonValue TOpRead::ToJson(ui32 explainFlags) {
    auto res = IOperator::ToJson(explainFlags);

    // Tables are usually named in a path-like fashion, like "/<path>/<name>".
    // In such case we extract the name of a table from a path,
    // fallback to the whole name otherwise.
    auto path = TKqpTable(TableCallable).Path().StringValue();
    auto slash = path.rfind('/');
    res["Table"] = (slash == TString::npos) ? path : path.substr(slash + 1);

    NJson::TJsonValue readColumns(NJson::EJsonValueType::JSON_ARRAY);
    for (const auto& column : Columns) {
        readColumns.AppendValue(column);
    }
    res["ReadColumns"] = readColumns;
    res["Storage"] = StorageType == NYql::EStorageType::RowStorage ? "Row" : "Column";

    if (SortDir != ESortDir::None) {
        res["SortDirection"] = SortDir == ESortDir::Asc ? "asc" : "desc";
    }
    if (const auto limit = GetUint64Literal(Limit)) {
        res["Limit"] = *limit;
    }

    if (OriginalPredicate) {
        res["Predicate"] = OriginalPredicate->ToExplainString();
    }

    return res;
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
    if (OlapFilterLambda) {
        res << " OlapFilter: (" << PrintRBOExpression(OlapFilterLambda, ctx) << ")";
    }
    if (Ranges) {
        res << " Ranges: (" << PrintRBOExpression(Ranges, ctx) << ")";
    }
    if (SortDir != ESortDir::None) {
        res << " Sort direction: (" << ((SortDir == ESortDir::Asc) ? "ASC" : "DESC");
        res << ")";
    }
    if (OriginalPredicate.has_value()) {
        res << " Original predicate: (" << PrintRBOExpression(OriginalPredicate->Node, ctx) << ")";
    }

    return res;
}

TMapElement::TMapElement(const TInfoUnit& elementName, const TExpression& expr, bool isRename)
    : ElementName(elementName)
    , Expr(expr)
    , Rename(isRename) {
    Y_ENSURE(!Rename || Expr.IsColumnAccess(), "Rename map element must be a plain column access");
}

TMapElement::TMapElement(const TInfoUnit& elementName, const TInfoUnit& rename, TPositionHandle pos, TExprContext* ctx, TPlanProps* props,
                         bool isRename)
    : ElementName(elementName)
    , Expr(MakeColumnAccess(rename, pos, ctx, props))
    , Rename(isRename) {
}

bool TMapElement::IsRename() const {
    return Rename;
}

void TMapElement::SetIsRename(bool isRename) {
    Y_ENSURE(!isRename || Expr.IsColumnAccess(), "Rename map element must be a plain column access");
    Rename = isRename;
}

bool TMapElement::IsColumnAccess() const {
    return Expr.IsColumnAccess();
}

TInfoUnit TMapElement::GetElementName() const {
    return ElementName;
}

void TMapElement::SetElementName(const TInfoUnit& elementName) {
    ElementName = elementName;
}

TExpression TMapElement::GetExpression() const {
    return Expr;
}

TExpression& TMapElement::GetExpressionRef() {
    return Expr;
}

TInfoUnit TMapElement::GetRename() const {
    Y_ENSURE(Rename, "Map element is not a semantic rename");
    return GetColumnAccess();
}

TInfoUnit TMapElement::GetColumnAccess() const {
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
TOpMap::TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TMapElement>& mapElements, bool ordered)
    : IUnaryOperator(EOperator::Map, pos, input)
    , MapElements(mapElements)
    , Ordered(ordered) {
}

TOpMap::TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TVector<TMapElement>& mapElements,
               bool ordered)
    : IUnaryOperator(EOperator::Map, pos, props, input)
    , MapElements(mapElements)
    , Ordered(ordered) {
}

TVector<TInfoUnit> TOpMap::GetOutputIUs() {
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }

    TVector<TInfoUnit> res = GetInput()->GetOutputIUs();
    THashSet<TInfoUnit, TInfoUnit::THashFunction> renameSources;

    for (const auto& mapElement : MapElements) {
        if (mapElement.IsRename()) {
            renameSources.insert(mapElement.GetRename());
        }
    }

    if (!renameSources.empty()) {
        TVector<TInfoUnit> kept;
        kept.reserve(res.size());
        for (const auto& iu : res) {
            if (!renameSources.contains(iu)) {
                kept.push_back(iu);
            }
        }
        res = std::move(kept);
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

void TOpMap::PropagateLiveness(ILivenessContext& ctx) {
    const TInfoUnitSet liveOut = ctx.GetLiveOut(this);
    auto input = GetInput();
    TInfoUnitSet inputLive;
    TInfoUnitSet renameSources;

    for (const auto& mapElement : MapElements) {
        if (mapElement.IsRename()) {
            renameSources.insert(mapElement.GetRename());
            // Renames are not pruned in this stage, so their source must stay available.
            ctx.AddExpressionDeps(mapElement.GetExpression(), inputLive);
        }
    }

    for (const auto& iu : input->GetOutputIUs()) {
        if (!renameSources.contains(iu) && liveOut.contains(iu)) {
            AddInfoUnit(inputLive, iu);
        }
    }

    for (const auto& mapElement : MapElements) {
        if (!mapElement.IsRename() && liveOut.contains(mapElement.GetElementName())) {
            ctx.AddExpressionDeps(mapElement.GetExpression(), inputLive);
        }
    }

    ctx.AddLiveColumns(input, inputLive);
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

// Both a := b and a <- b let a inherit key, shuffle, and lineage properties from b.
// a := b keeps b visible, while a <- b removes the old b binding from Map output.
// Pg FromPg/ToPg wrappers over a column are treated the same for property propagation.
TVector<std::pair<TInfoUnit, TInfoUnit>> TOpMap::GetPropertyPreservingMappings(TPlanProps& props) const {
    Y_UNUSED(props);
    auto result = GetRenames();

    for (const auto &mapElement : MapElements) {
        if (!mapElement.IsRename()) {
            if (mapElement.IsColumnAccess()) {
                result.push_back(std::make_pair(mapElement.GetElementName(), mapElement.GetColumnAccess()));
                continue;
            }

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
            newMapElements.emplace_back(newIU, MakeColumnAccess(from, Pos, expr.Ctx, expr.PlanProps), true);
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
        const auto k = FormatMapElementName(mapElement.GetElementName());

        TString fromName;
        if (mapElement.IsRename()) {
            fromName = mapElement.GetRename().GetFullName();
        } else {
            fromName = mapElement.GetExpression().ToString();
        }
        res << k << (mapElement.IsRename() ? " <- " : " := ") << fromName;

        if (i != e - 1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

NJson::TJsonValue TOpMap::ToJson(ui32 explainFlags) {
    auto res = IOperator::ToJson(explainFlags);

    TStringBuilder name;
    name << "Map [";
    for (size_t i = 0, e = MapElements.size(); i < e; ++i) {
        const auto& mapElement = MapElements[i];
        name << FormatMapElementName(mapElement.GetElementName()) << (mapElement.IsRename() ? " <- " : " := ");
        if (mapElement.IsRename()) {
            name << mapElement.GetRename().GetFullName();
        } else {
            name << mapElement.GetExpression().ToExplainString();
        }

        if (i + 1 != e) {
            name << ", ";
        }
    }
    name << "]";

    res["Name"] = name;
    return res;
}

bool TOpMap::HasRenames() const {
    return std::any_of(MapElements.begin(), MapElements.end(), [](const TMapElement& element) {
        return element.IsRename();
    });
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

TOpAddDependencies::TOpAddDependencies(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>>& pairs)
    : IUnaryOperator(EOperator::AddDependencies, pos, input) {
        for (const auto & [iu, type] : pairs) {
            Dependencies.push_back(iu);
            Types.push_back(type);
        }
}

TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>> TOpAddDependencies::GetDependencyPairs() {
    TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>> result;
    for (size_t i=0; i<Dependencies.size(); i++) {
        result.push_back(std::make_pair(Dependencies[i], Types[i]));
    }
    return result;
}

void TOpAddDependencies::SetDependencyPairs(const TVector<std::pair<TInfoUnit, const TTypeAnnotationNode*>>& pairs) {
    Dependencies.clear();
    Types.clear();
    for (const auto & [iu, type] : pairs) {
        Dependencies.push_back(iu);
        Types.push_back(type);
    }
}

TVector<TInfoUnit> TOpAddDependencies::GetOutputIUs() {
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }

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
 * OpFilter operator methods
 */

TOpFilter::TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& filterExpr)
    : IUnaryOperator(EOperator::Filter, pos, input)
    , FilterExpr(filterExpr) {
}

TOpFilter::TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& filterExpr)
    : IUnaryOperator(EOperator::Filter, pos, props, input)
    , FilterExpr(filterExpr) {
}

TVector<TInfoUnit> TOpFilter::GetOutputIUs() {
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }
    return GetInput()->GetOutputIUs();
}

void TOpFilter::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                          const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
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

void TOpFilter::PropagateLiveness(ILivenessContext& ctx) {
    TInfoUnitSet inputLive = ctx.GetLiveOut(this);
    ctx.AddExpressionDeps(FilterExpr, inputLive);
    ctx.AddLiveColumns(GetInput(), inputLive);
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

NJson::TJsonValue TOpFilter::ToJson(ui32 explainFlags) {
    auto res = IOperator::ToJson(explainFlags);
    res["Predicate"] = FilterExpr.ToExplainString();
    return res;
}

/**
 * OpJoin operator methods
 */

TOpJoin::TOpJoin(TIntrusivePtr<IOperator> leftInput, TIntrusivePtr<IOperator> rightInput, TPositionHandle pos, TString joinKind,
                 const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys)
    : IBinaryOperator(EOperator::Join, pos, leftInput, rightInput), JoinKind(joinKind), JoinKeys(joinKeys) {}

TOpJoin::TOpJoin(TIntrusivePtr<IOperator> leftInput, TIntrusivePtr<IOperator> rightInput, TPositionHandle pos, TString joinKind,
                 const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys, const TVector<TExpression>& joinFilters)
    : IBinaryOperator(EOperator::Join, pos, leftInput, rightInput), JoinKind(joinKind), JoinKeys(joinKeys), JoinFilters(joinFilters) {}

TVector<TInfoUnit> TOpJoin::GetOutputIUs() {
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }

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

    for (const auto & f : JoinFilters) {
        auto filterIUs = f.GetInputIUs(true, true);
        result.insert(result.end(), filterIUs.begin(), filterIUs.end());
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

    if (JoinFilters.size()) {
        for (auto& filter : JoinFilters) {
            filter = filter.ApplyRenames(renameMap);
        }
    }
}

TVector<std::reference_wrapper<TExpression>> TOpJoin::GetExpressions() {
    TVector<std::reference_wrapper<TExpression>> result;
    for (auto & expr : JoinFilters) {
        result.push_back(expr);
    }
    return result;
}

void TOpJoin::PropagateLiveness(ILivenessContext& ctx) {
    const TInfoUnitSet liveOut = ctx.GetLiveOut(this);
    const auto leftInput = GetLeftInput();
    const auto rightInput = GetRightInput();
    const auto leftOutput = MakeInfoUnitSet(leftInput->GetOutputIUs());
    const auto rightOutput = MakeInfoUnitSet(rightInput->GetOutputIUs());

    TInfoUnitSet leftLive;
    TInfoUnitSet rightLive;

    const bool outputsLeft = JoinKind != "RightOnly" && JoinKind != "RightSemi";
    const bool outputsRight = JoinKind != "LeftOnly" && JoinKind != "LeftSemi";

    if (outputsLeft) {
        for (const auto& iu : leftOutput) {
            if (liveOut.contains(iu)) {
                AddInfoUnit(leftLive, iu);
            }
        }
    }

    if (outputsRight) {
        for (const auto& iu : rightOutput) {
            if (liveOut.contains(iu)) {
                AddInfoUnit(rightLive, iu);
            }
        }
    }

    for (const auto& [leftKey, rightKey] : JoinKeys) {
        AddInfoUnit(leftLive, leftKey);
        AddInfoUnit(rightLive, rightKey);
    }

    for (const auto& filter : JoinFilters) {
        TInfoUnitSet filterDeps;
        ctx.AddExpressionDeps(filter, filterDeps);
        for (const auto& iu : filterDeps) {
            if (leftOutput.contains(iu)) {
                AddInfoUnit(leftLive, iu);
            }
            if (rightOutput.contains(iu)) {
                AddInfoUnit(rightLive, iu);
            }
        }
    }

    ctx.AddLiveColumns(leftInput, leftLive);
    ctx.AddLiveColumns(rightInput, rightLive);
}

TString GetJoinAlgoName(NKqp::EJoinAlgoType joinAlgo) {
    switch (joinAlgo) {
        case NKqp::EJoinAlgoType::Undefined:
            return "Undefined";
        case NKqp::EJoinAlgoType::LookupJoin:
            return "Lookup";
        case NKqp::EJoinAlgoType::LookupJoinReverse:
            return "ReverseLookup";
        case NKqp::EJoinAlgoType::MapJoin:
            return "Map";
        case NKqp::EJoinAlgoType::GraceJoin:
            return "Shuffle";
        case NKqp::EJoinAlgoType::ReverseBlockJoin:
            return "ReverseBlock";
        case NKqp::EJoinAlgoType::StreamLookupJoin:
            return "StreamLookup";
        case NKqp::EJoinAlgoType::MergeJoin:
            return "Merge";
    }
    Y_ENSURE(false, "Unknown join algo type");
    return "Unknown";
}

TString GetExplainJoinAlgoName(NKqp::EJoinAlgoType joinAlgo) {
    if (joinAlgo == NKqp::EJoinAlgoType::GraceJoin) {
        return "Grace";
    }
    return GetJoinAlgoName(joinAlgo);
}

TString TOpJoin::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << "Join, Kind: " << JoinKind;
    if (Props.JoinAlgo.has_value()) {
        res << ", Algo: " << GetJoinAlgoName(*Props.JoinAlgo);
    }
    res << " [";
    for (size_t i = 0; i < JoinKeys.size(); i++) {
        auto [x,y] = JoinKeys[i];
        res << x.GetFullName() + "=" + y.GetFullName();
        if (i != JoinKeys.size() - 1) {
            res << ", ";
        }
    }
    res << "], Filters: [";
    for (size_t i = 0; i < JoinFilters.size(); i++) {
        res << JoinFilters[i].ToString();
        if (i != JoinFilters.size() - 1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

static TString FormatJoinKeys(const TVector<std::pair<TInfoUnit, TInfoUnit>>& joinKeys) {
    TStringBuilder result;
    for (size_t i = 0; i < joinKeys.size(); ++i) {
        if (i != 0) {
            result << ", ";
        }

        const auto& [leftKey, rightKey] = joinKeys[i];
        result << leftKey.GetFullName() << " = " << rightKey.GetFullName();
    }
    return result;
}

NJson::TJsonValue TOpJoin::ToJson(ui32 explainFlags) {
    auto res = IOperator::ToJson(explainFlags);
    const auto joinAlgo = Props.JoinAlgo.value_or(NKqp::EJoinAlgoType::Undefined);
    const auto joinAlgoName = GetExplainJoinAlgoName(joinAlgo);

    if (JoinKind == "Cross") {
        res["Name"] = "CrossJoin";
    } else {
        res["Name"] = TStringBuilder() << JoinKind << "Join (" << joinAlgoName << ")";
    }
    res["JoinKind"] = JoinKind;
    res["JoinAlgo"] = joinAlgoName;
    if (!JoinKeys.empty()) {
        res["Condition"] = FormatJoinKeys(JoinKeys);
    }
    if (!JoinFilters.empty()) {
        NJson::TJsonValue filters(NJson::EJsonValueType::JSON_ARRAY);
        for (const auto& filter : JoinFilters) {
            filters.AppendValue(filter.ToExplainString());
        }
        res["Filters"] = filters;
    }

    return res;
}

/**
 * OpUnionAll operator methods
 */

TOpUnionAll::TOpUnionAll(TIntrusivePtr<IOperator> leftInput, TIntrusivePtr<IOperator> rightInput, TPositionHandle pos, bool ordered)
    : IBinaryOperator(EOperator::UnionAll, pos, leftInput, rightInput), Ordered(ordered) {}

TVector<TInfoUnit> TOpUnionAll::GetOutputIUs() {
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }
    return GetLeftInput()->GetOutputIUs();
}

void TOpUnionAll::PropagateLiveness(ILivenessContext& ctx) {
    const TInfoUnitSet liveOut = ctx.GetLiveOut(this);
    ctx.AddLiveColumns(GetLeftInput(), liveOut);
    ctx.AddLiveColumns(GetRightInput(), liveOut);
}

TString TOpUnionAll::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx); 
    return "UnionAll";
}

NJson::TJsonValue TOpUnionAll::ToJson(ui32 explainFlags) {
    auto res = IOperator::ToJson(explainFlags);
    res["Ordered"] = Ordered;
    return res;
}

/**
 * OpLimit operator methods
 */

TOpLimit::TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond, const EOpPhase limitPhase)
    : IUnaryOperator(EOperator::Limit, pos, input)
    , LimitCond(limitCond)
    , LimitPhase(limitPhase) {
}

TOpLimit::TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& limitCond, const TExpression& offsetCond, const EOpPhase limitPhase)
    : IUnaryOperator(EOperator::Limit, pos, input)
    , LimitCond(limitCond)
    , OffsetCond(offsetCond)
    , LimitPhase(limitPhase) {
}

TOpLimit::TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& limitCond, const EOpPhase limitPhase)
    : IUnaryOperator(EOperator::Limit, pos, props, input)
    , LimitCond(limitCond)
    , LimitPhase(limitPhase) {
}

TOpLimit::TOpLimit(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& limitCond,
                   const std::optional<TExpression> offsetCond, const EOpPhase limitPhase)
    : IUnaryOperator(EOperator::Limit, pos, props, input)
    , LimitCond(limitCond)
    , OffsetCond(offsetCond)
    , LimitPhase(limitPhase) {
}

TVector<TInfoUnit> TOpLimit::GetOutputIUs() {
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }
    return GetInput()->GetOutputIUs();
}

void TOpLimit::PropagateLiveness(ILivenessContext& ctx) {
    TInfoUnitSet inputLive = ctx.GetLiveOut(this);
    ctx.AddExpressionDeps(LimitCond, inputLive);
    if (OffsetCond) {
        ctx.AddExpressionDeps(*OffsetCond, inputLive);
    }
    ctx.AddLiveColumns(GetInput(), inputLive);
}

void TOpLimit::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                         const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
    LimitCond = LimitCond.ApplyRenames(renameMap);
    if (OffsetCond) {
        OffsetCond = OffsetCond->ApplyRenames(renameMap);
    }
}

TString TOpLimit::ToString(TExprContext& ctx) {
    Y_UNUSED(ctx);
    TStringBuilder builder;
    builder << "Limit: " << LimitCond.ToString() << " ";
    if (OffsetCond.has_value()) {
        builder << "Offset: " << OffsetCond->ToString() << " ";
    }
    builder << "Phase: " << ToStringPhase(LimitPhase);
    return builder;
}

NJson::TJsonValue TOpLimit::ToJson(ui32 explainFlags) {
    auto res = IOperator::ToJson(explainFlags);
    res["Limit"] = LimitCond.ToExplainString();
    if (OffsetCond) {
        res["Offset"] = OffsetCond->ToExplainString();
    }
    if (LimitPhase != EOpPhase::Undefined) {
        res["Phase"] = ToStringPhase(LimitPhase);
    }
    return res;
}

TVector<std::reference_wrapper<TExpression>> TOpLimit::GetExpressions() {
    return {LimitCond};
}

/**
 * Sort operator
 * FIXME: This is temporary, we want to get enforcers working
 */
TOpSort::TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TVector<TSortElement>& sortElements, std::optional<TExpression> limitCond)
    : IUnaryOperator(EOperator::Sort, pos, input)
    , SortElements(sortElements)
    , LimitCond(limitCond) {
}

TOpSort::TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TVector<TSortElement>& sortElements,
                 std::optional<TExpression> limitCond, const EOpPhase sortPhase)
    : IUnaryOperator(EOperator::Sort, pos, props, input)
    , SortElements(sortElements)
    , LimitCond(limitCond)
    , SortPhase(sortPhase) {
}

TVector<TInfoUnit> TOpSort::GetOutputIUs() {
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }
    return GetInput()->GetOutputIUs();
}

TVector<TInfoUnit> TOpSort::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);
    TVector<TInfoUnit> result;
    for (const auto& element : SortElements) {
        result.push_back(element.SortColumn);
    }
    return result;
}

void TOpSort::PropagateLiveness(ILivenessContext& ctx) {
    TInfoUnitSet inputLive = ctx.GetLiveOut(this);
    for (const auto& sortElement : SortElements) {
        AddInfoUnit(inputLive, sortElement.SortColumn);
    }
    if (LimitCond) {
        ctx.AddExpressionDeps(*LimitCond, inputLive);
    }
    ctx.AddLiveColumns(GetInput(), inputLive);
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
        LimitCond = LimitCond->ApplyRenames(renameMap);
    }
    SortElements = std::move(newSortElements);
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

    res << " Phase: " << ToStringPhase(SortPhase);
    
    return res;
}

NJson::TJsonValue TOpSort::ToJson(ui32 explainFlags) {
    auto res = IOperator::ToJson(explainFlags);
    if (IsTopSort()) {
        res["TopSortBy"] = FormatSortElements(SortElements);
        if (LimitCond) {
            res["Limit"] = LimitCond->ToExplainString();
        }
    } else {
        res["SortBy"] = FormatSortElements(SortElements);
    }
    if (SortPhase != EOpPhase::Undefined) {
        res["Phase"] = ToStringPhase(SortPhase);
    }
    return res;
}

/**
 * OpAggregate operator methods
 */
TOpAggregate::TOpAggregate(TIntrusivePtr<IOperator> input, const TVector<TOpAggregationTraits>& aggTraitsList, const TVector<TInfoUnit>& keyColumns,
                           const EOpPhase aggPhase, bool distinctAll, TPositionHandle pos)
    : IUnaryOperator(EOperator::Aggregate, pos, input)
    , AggregationTraitsList(aggTraitsList)
    , KeyColumns(keyColumns)
    , AggregationPhase(aggPhase)
    , DistinctAll(distinctAll) {
}

TOpAggregate::TOpAggregate(TIntrusivePtr<IOperator> input, const TVector<TOpAggregationTraits>& aggTraitsList, const TVector<TInfoUnit>& keyColumns,
                           const EOpPhase aggPhase, bool distinctAll, const TPhysicalOpProps& props, TPositionHandle pos)
    : IUnaryOperator(EOperator::Aggregate, pos, props, input)
    , AggregationTraitsList(aggTraitsList)
    , KeyColumns(keyColumns)
    , AggregationPhase(aggPhase)
    , DistinctAll(distinctAll) {
}

TVector<TInfoUnit> TOpAggregate::GetOutputIUs() {
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }

    // We assume that aggregation returns column is order [keys, states].
    // DistinctAll uses keys only as physical aggregation state and returns distinct states.
    TVector<TInfoUnit> outputIU;
    if (!DistinctAll) {
        outputIU = KeyColumns;
    }
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

void TOpAggregate::PropagateLiveness(ILivenessContext& ctx) {
    TInfoUnitSet inputLive;
    AddInfoUnits(inputLive, KeyColumns);
    for (const auto& traits : AggregationTraitsList) {
        AddInfoUnit(inputLive, traits.OriginalColName);
    }
    ctx.AddLiveColumns(GetInput(), inputLive);
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
        strBuilder << AggregationTraitsList[i].ResultColName.GetFullName() << ": " << AggregationTraitsList[i].AggFunction << "("
                   << AggregationTraitsList[i].OriginalColName.GetFullName() << ")";
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
    strBuilder << ToStringPhase(AggregationPhase);
    return strBuilder;
}

static TString FormatInfoUnits(const TVector<TInfoUnit>& infoUnits) {
    TStringBuilder result;
    for (size_t i = 0; i < infoUnits.size(); ++i) {
        if (i != 0) {
            result << ", ";
        }
        result << infoUnits[i].GetFullName();
    }
    return result;
}

NJson::TJsonValue TOpAggregate::ToJson(ui32 explainFlags) {
    auto res = IOperator::ToJson(explainFlags);

    if (!KeyColumns.empty()) {
        res["GroupBy"] = FormatInfoUnits(KeyColumns);
    }

    if (!AggregationTraitsList.empty()) {
        TStringBuilder aggregation;
        aggregation << "{";
        for (size_t i = 0; i < AggregationTraitsList.size(); ++i) {
            if (i != 0) {
                aggregation << ", ";
            }
            aggregation << AggregationTraitsList[i].ResultColName.GetFullName()
                << ": " << AggregationTraitsList[i].AggFunction
                << "(" << AggregationTraitsList[i].OriginalColName.GetFullName() << ")";
        }
        aggregation << "}";
        res["Aggregation"] = aggregation;
    }

    res["Phase"] = ToStringPhase(AggregationPhase);
    if (DistinctAll) {
        res["Distinct"] = "All";
    }

    return res;
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

void TOpCBOTree::PropagateLiveness(ILivenessContext& ctx) {
    for (const auto& child : Children) {
        ctx.AddLiveColumns(child, child->GetOutputIUs());
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
    if (const auto& outputIUs = GetOutputIUsOverride()) {
        return *outputIUs;
    }
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

    builder << tabString << op->ToString(ctx);
    if (op->Props.StageId.has_value()) {
        builder << " StageId: " << *op->Props.StageId;
    }
    builder << "\n";

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

TOpIterator::TOpIterator(TOpRoot* ptr) {
    if (!ptr) {
        CurrElement = -1;
        return;
    }

    std::unordered_set<IOperator*> visited;
    for (const auto& subplan : ptr->PlanProps.Subplans.Get()) {
        BuildDfsList(CastOperator<IOperator>(subplan.Plan), nullptr, size_t(0), visited, std::make_shared<TInfoUnit>(subplan.IU));
    }
    auto child = ptr->GetInput();
    BuildDfsList(child, {}, size_t(0), visited, nullptr);
    CurrElement = 0;
}

TOpIterator::TOpIterator(TIntrusivePtr<IOperator> op, TIntrusivePtr<IOperator> parent) {
    std::unordered_set<IOperator*> visited;
    BuildDfsList(op, parent, size_t(0), visited, nullptr);
    CurrElement = 0;
}

TOpIterator::TOpIterator(TIntrusivePtr<IOperator> op, TIntrusivePtr<IOperator> parent, TPlanProps* props) {
    PlanProps = props;
    std::unordered_set<IOperator*> visited;
    BuildDfsList(op, parent, size_t(0), visited, nullptr, true);
    CurrElement = 0;
}

TOpIterator::TIteratorItem TOpIterator::operator*() const {
    return DfsList[CurrElement];
}

TOpIterator& TOpIterator::operator++() {
    if (CurrElement >= 0) {
        CurrElement++;
    }
    if (CurrElement == DfsList.size()) {
        CurrElement = -1;
    }
    return *this;
}

TOpIterator TOpIterator::operator++(int) {
    TOpIterator tmp = *this;
    ++(*this);
    return tmp;
}

void TOpIterator::BuildDfsList(TIntrusivePtr<IOperator> current, TIntrusivePtr<IOperator> parent, size_t childIdx, std::unordered_set<IOperator*>& visited,
                          std::shared_ptr<TInfoUnit> subplanIU, bool recurseIntoSubplans) {

    if(recurseIntoSubplans) {
        auto subplanIUs = current->GetSubplanIUs(*PlanProps);
        for (const auto & iu : subplanIUs) {
            const auto & subplan = PlanProps->Subplans.PlanMap.at(iu);
            BuildDfsList(CastOperator<IOperator>(subplan.Plan), nullptr, 0, visited, std::make_shared<TInfoUnit>(iu), true);
        }
    }

    const auto& children = current->GetChildren();
    for (size_t idx = 0, e = children.size(); idx < e; ++idx) {
        BuildDfsList(children[idx], current, idx, visited, subplanIU, recurseIntoSubplans);
    }
    if (!visited.contains(current.get())) {
        DfsList.push_back(TOpIterator::TIteratorItem(current, parent, childIdx, subplanIU));
    }
    visited.insert(current.get());
}

TString ToStringPhase(EOpPhase phase) {
    switch (phase) {
#define X(name) case EOpPhase::name: return #name;
        PHASE_ENUM(X)
#undef X
    }
}

} // namespace NKqp
} // namespace NKikimr
