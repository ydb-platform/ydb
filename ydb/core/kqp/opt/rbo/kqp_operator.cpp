#include "kqp_operator.h"
#include "kqp_expression.h"
#include "kqp_rbo_utils.h"
#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/opt/rbo/kqp_olap_expr_inspection.h>
#include <yql/essentials/core/yql_expr_optimize.h>

#include <algorithm>
#include <limits>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

namespace {

template <typename TRange>
TString FormatIds(const TRange& ids, const TInfoUnitRegistry* registry = nullptr, bool debug = false) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto id : ids) {
        result << separator << (registry ? (debug ? registry->GetDebugName(id) : registry->GetDisplayName(id)) : ::ToString(id));
        separator = ", ";
    }
    return result;
}

TString FormatSortElements(const TSortIUs& keys, const TInfoUnitRegistry& registry) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto& [id, order] : keys.Items()) {
        result << separator << registry.GetDisplayName(id) << (order.Ascending ? " asc" : " desc")
               << (order.NullsFirst ? " nulls first" : " nulls last");
        separator = ", ";
    }
    return result;
}

} // namespace

/**
 * Base class Operator methods
 */

const TTypeAnnotationNode* IOperator::GetIUType(TInfoUnitId iu, TExprContext& ctx) const {
    auto structType = Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    return structType->FindItemType(ctx.GetIndexAsString(iu));
}

TUnorderedIUs IOperator::GetSubplanIUs(const TSubplans& subplans) const {
    return subplans.CallsIn(GetUniqueRawInputIUs());
}

void IOperator::ReplaceChild(const TIntrusivePtr<IOperator> oldChild, const TIntrusivePtr<IOperator> newChild) {
    for (size_t i = 0; i < GetChildCount(); i++) {
        if (GetChild(i) == oldChild) {
            SetChild(i, newChild);
            return;
        }
    }
    Y_ENSURE(false, "Did not find a child to replace");
}

NJson::TJsonValue IOperator::ToJson(ui32 explainFlags, const TInfoUnitRegistry&)
{
    Y_UNUSED(explainFlags);
    auto res = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
    res["Name"] = GetExplainName();
    return res;
}

// To get output IUs we check whether they're already computed in Props and return them.
// Otherwise we compute and cache missing output IUs for this subtree.
const TUnorderedIUs& IOperator::GetOutputIUs() {
    if (!Props.OutputIUs.has_value()) {
        ComputeOutputIUsSubtree();
        Y_ENSURE(Props.OutputIUs.has_value(), "Computation of output IUs failed for " << GetExplainName());
    }
    return Props.OutputIUs.value();
}

void IOperator::BindExpressionPlanProps(TPlanProps* props) {
    for (const auto& expression : GetExpressions()) {
        expression.get().BindPlanProps(props);
    }
}

void IOperator::ComputeOutputIUsSubtree() {
    for (auto op : GetChildren()) {
        for (const auto& item : IterateSubtree(op)) {
            if (!item.Current->Props.OutputIUs.has_value()) {
                item.Current->ComputeOutputIUs();
            }
        }
    }

    ComputeOutputIUs();
}

/**
 * Replicate and output-port methods
 */

TReplicate::TReplicate(TIntrusivePtr<IOperator> input, TPositionHandle pos, TInfoUnitRegistry& registry)
    : Pos(pos)
    , Input_(std::move(input))
    , Registry_(&registry)
{
    Y_ENSURE(Input_);
}

TIntrusivePtr<TReplicate> TReplicate::Create(TIntrusivePtr<IOperator> input, TPositionHandle pos, TInfoUnitRegistry& registry) {
    return TIntrusivePtr<TReplicate>(new TReplicate(std::move(input), pos, registry));
}

TIntrusivePtr<TOpReplicate> TReplicate::AddOutput() {
    Y_ENSURE(NextOutputIndex_ < std::numeric_limits<ui32>::max(), "Too many Replicate outputs");
    auto output = TIntrusivePtr<TOpReplicate>(new TOpReplicate(this, NextOutputIndex_));
    output->GetOutputIUs();
    ++NextOutputIndex_;
    return output;
}

TOpReplicate::TOpReplicate(TIntrusivePtr<TReplicate> input, ui32 index)
    : IUnaryOperator(EOperator::Replicate, input->Pos)
    , Replicate_(std::move(input))
    , Index_(index)
{
    if (IsPrimary()) {
        Type = GetInput()->Type;
    }
}

bool TOpReplicate::TryCollapse(TIntrusivePtr<IOperator>& slot, TExprContext& ctx, TPlanProps& props) {
    if (slot->Kind != EOperator::Replicate) {
        return false;
    }
    auto& port = CastOperator<TOpReplicate>(*slot);
    auto& hub = port.GetReplicate();
    const auto& outputs = hub.GetOutputs();
    if (outputs.size() != 1 || outputs.front() != &port) {
        return false;
    }
    TMapIUs copies;
    if (!port.IsPrimary()) {
        const auto& bindings = port.GetRebindings();
        for (const auto source : hub.GetInput()->GetOutputIUs()) {
            copies.Add(*bindings.Find(source), MakeColumnAccess(source, port.Pos, &ctx, &props));
        }
    }
    auto replacement = hub.GetInput();
    if (!copies.Keys().Empty()) {
        replacement = MakeIntrusive<TOpMap>(std::move(replacement), port.Pos, std::move(copies));
    }
    slot = std::move(replacement);
    return true;
}

void TOpReplicate::RefreshBindings() {
    if (IsPrimary()) {
        return;
    }
    auto& hub = GetReplicate();
    const auto& inputs = GetInput()->GetOutputIUs();
    if (Props.OutputIUs && InputIUs_ == inputs) {
        return;
    }

    TVector<TInfoUnitId> outputs;
    outputs.reserve(inputs.Size());
    for (const auto input : inputs) {
        auto* output = Rebindings_.Find(input);
        if (!output) {
            // Copy the label before registry growth can invalidate its storage.
            output = &Rebindings_.Add(input, hub.Registry_->AddCopy(input));
        }
        outputs.push_back(*output);
    }
    TUnorderedIUs outputIUs;
    outputIUs.Assign(outputs);
    InputIUs_ = inputs;
    Props.OutputIUs = std::move(outputIUs);
}

const TUnorderedIUs& TOpReplicate::GetOutputIUs() {
    if (IsPrimary()) {
        return GetInput()->GetOutputIUs();
    }
    RefreshBindings();
    return *Props.OutputIUs;
}

const TMappedIUs<TInfoUnitId>& TOpReplicate::GetRebindings() {
    RefreshBindings();
    return Rebindings_;
}

TUnorderedIUs TOpReplicate::MapToInput(const TUnorderedIUs& outputIUs) {
    Y_ENSURE(outputIUs.IsSubsetOf(GetOutputIUs()), "IU is not a current Replicate output");
    if (IsPrimary()) {
        return outputIUs;
    }
    TVector<TInfoUnitId> inputs;
    inputs.reserve(outputIUs.Size());
    for (const auto input : InputIUs_) {
        if (outputIUs.Contains(*Rebindings_.Find(input))) {
            inputs.push_back(input);
        }
    }
    TUnorderedIUs result;
    result.Assign(inputs);
    return result;
}

TSubstitutions TOpReplicate::RebindInputs(const TSubstitutions& substitutions) {
    TSubstitutions result;
    if (IsPrimary() || substitutions.Keys().Empty()) {
        return result; // Producer substitutions already apply to this port.
    }
    // Do not refresh against the partially rewritten producer. Existing entries
    // are the old correspondence, including bindings hidden by earlier pruning.
    TMappedIUs<TInfoUnitId> rebound;
    for (const auto source : Rebindings_.Keys()) {
        const auto input = Substitute(source, substitutions);
        const auto output = *Rebindings_.Find(source);
        auto* representative = rebound.Find(input);
        if (!representative) {
            // Prefer an unchanged source's binding when coalescing. A source
            // that itself moves (e.g. a simultaneous swap) cannot donate its ID.
            const auto* origin = Substitute(input, substitutions) == input ? Rebindings_.Find(input) : nullptr;
            representative = &rebound.Add(input, origin ? *origin : output);
        }
        if (output != *representative) {
            result.Add(output, *representative);
        }
    }
    Rebindings_ = std::move(rebound);
    InputIUs_.Clear();
    Props.OutputIUs.reset();
    return result;
}

TString TOpReplicate::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    const auto& rebindings = GetRebindings();
    TStringBuilder result;
    result << "Replicate #" << Index_;
    if (IsPrimary()) {
        return result << " (identity)";
    }
    result << " {";
    TStringBuf separator;
    for (const auto input : InputIUs_) {
        result << separator << registry.GetDebugName(input) << " -> " << registry.GetDebugName(*rebindings.Find(input));
        separator = ", ";
    }
    return result << "}";
}

/**
 * EmptySource operator methods
 */

TString TOpEmptySource::ToString(TExprContext& ctx, const TInfoUnitRegistry&) {
    Y_UNUSED(ctx); 
    return "EmptySource"; 
}

NJson::TJsonValue TOpEmptySource::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto res = IOperator::ToJson(explainFlags, registry);
    if (Input && TCoParameter::Match(Input.Get())) {
        res["Parameter"] = TCoParameter(Input).Name().StringValue();
    }
    if (Input) {
        res["Columns"] = TStringBuilder() << "{" << FormatIds(Columns, &registry) << "}";
    }
    return res;
}

/**
 * OpRead operator methods
 */

TIntrusivePtr<TOpRead> TOpRead::FromExpr(TExprNode::TPtr node, TInfoUnitRegistry& registry) {
    const auto opSource = TKqpOpRead(node);
    const auto alias = opSource.Alias().StringValue();

    // Keep each Read's definitions consecutive, then bulk-build the bitset.
    TVector<TInfoUnitId> ids;
    ids.reserve(opSource.Columns().Size());
    for (const auto& column : opSource.Columns()) {
        ids.push_back(registry.Add(TInfoUnit(alias, column.StringValue())));
    }

    TUnorderedIUs columns;
    columns.Assign(ids);
    const auto storageType = opSource.SourceType().StringValue() == "Row" ? NYql::EStorageType::RowStorage : NYql::EStorageType::ColumnStorage;
    return MakeIntrusive<TOpRead>(alias, std::move(columns), storageType, opSource.Table().Ptr(),
        nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, node->Pos());
}

TOpRead::TOpRead(const TString& alias, TUnorderedIUs columns, const NYql::EStorageType storageType,
                 const TExprNode::TPtr& tableCallable, const TExprNode::TPtr& olapFilterLambda, const TExprNode::TPtr& limit, std::optional<TRangeInfo> ranges,
                 const std::optional<TExpression>& originalPredicate, const ESortDir sortDir, const TPhysicalOpProps& props, TPositionHandle pos)
    : IOperator(EOperator::Source, pos, props)
    , Alias(alias)
    , StorageType(storageType)
    , TableCallable(tableCallable)
    , OlapFilterLambda(olapFilterLambda)
    , Limit(limit)
    , OriginalPredicate(originalPredicate)
    , SortDir(sortDir)
    , RangeInfo(std::move(ranges))
    , Columns_(std::move(columns)) {
}

NYql::EStorageType TOpRead::GetTableStorageType() const {
    return StorageType;
}

TUnorderedIUs TOpRead::GetRequiredColumns(TExprContext& ctx) const {
    TUnorderedIUs required;
    if (OriginalPredicate) {
        required = OriginalPredicate->GetRawInputIUs();
        required.IntersectWith(Columns_);
    }
    if (!OlapFilterLambda) {
        return required;
    }

    const auto inspection = NOpt::InspectOlapProcessLambda(OlapFilterLambda);
    if (inspection.RequiresAllInputColumns) {
        return Columns_;
    }
    // Match canonical ID atoms, not registry labels or storage column names.
    for (const auto id : Columns_) {
        if (inspection.Columns.contains(ctx.GetIndexAsString(id))) {
            required.Add(id);
        }
    }
    return required;
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

TString StripAliasPrefix(const TString& column) {
    const auto dot = column.rfind('.');
    return (dot != TString::npos) ? column.substr(dot + 1) : column;
}

NJson::TJsonValue TOpRead::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto res = IOperator::ToJson(explainFlags, registry);

    // Tables are usually named in a path-like fashion, like "/<path>/<name>".
    // In such case we extract the name of a table from a path,
    // fallback to the whole name otherwise.
    auto path = TKqpTable(TableCallable).Path().StringValue();
    auto slash = path.rfind('/');
    res["Table"] = (slash == TString::npos) ? path : path.substr(slash + 1);

    if (slash != TString::npos && TStringBuf(path).SubStr(slash + 1) == NTableIndex::ImplTable) {
        const auto indexSlash = path.rfind('/', slash - 1);
        if (indexSlash != TString::npos) {
            const auto tableSlash = path.rfind('/', indexSlash - 1);
            res["Table"] = path.substr(tableSlash == TString::npos ? 0 : tableSlash + 1);
            res["Index"] = path.substr(indexSlash + 1, slash - indexSlash - 1);
        }
    }

    res["Storage"] = StorageType == NYql::EStorageType::RowStorage ? "Row" : "Column";

    if (SortDir != ESortDir::None) {
        res["SortDirection"] = SortDir == ESortDir::Asc ? "asc" : "desc";
    }
    if (const auto limit = GetUint64Literal(Limit)) {
        res["Limit"] = *limit;
    }

    if (OriginalPredicate && !RangeInfo) {
        res["Predicate"] = OriginalPredicate->ToExplainString(registry);
    }

    // Build ReadColumns: for range scans, list ranged key columns first, then remaining columns.
    // This mirrors the old RBO which embeds key range descriptions into ReadColumns.
    NJson::TJsonValue readColumns(NJson::EJsonValueType::JSON_ARRAY);
    THashSet<TString> addedColumns;
    if (RangeInfo) {
        const size_t usedLen = Min(RangeInfo->UsedPrefixLen, RangeInfo->KeyColumns.size());
        TVector<TString> rangedKeys;
        rangedKeys.reserve(usedLen);
        for (size_t i = 0; i < usedLen; ++i) {
            rangedKeys.push_back(StripAliasPrefix(RangeInfo->KeyColumns[i]));
        }

        const auto descriptions = NOpt::BuildReadRangeDescriptions(RangeInfo->ComputeNode, RangeInfo->KeyColumns, usedLen);
        for (const auto& label : descriptions.empty() ? rangedKeys : descriptions) {
            readColumns.AppendValue(label);
        }

        NJson::TJsonValue rangeKeys(NJson::EJsonValueType::JSON_ARRAY);
        for (const auto& key : rangedKeys) {
            addedColumns.insert(key);
            rangeKeys.AppendValue(key);
        }
        res["ReadRangesKeys"] = std::move(rangeKeys);

        if (RangeInfo->ExpectedMaxRanges) {
            res["ReadRangesExpectedSize"] = ::ToString(*RangeInfo->ExpectedMaxRanges);
        }
    }
    // Choose a deterministic presentation order, not a logical column order.
    for (const auto id : Columns_) {
        const auto column = registry.Get(id).GetColumnName();
        if (addedColumns.insert(column).second) {
            readColumns.AppendValue(column);
        }
    }
    res["ReadColumns"] = std::move(readColumns);
    // ReadColumns names storage fields/ranges; Columns names the produced row.
    res["Columns"] = TStringBuilder() << "{" << FormatIds(Columns_, &registry) << "}";

    return res;
}

TString TOpRead::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    auto res = TStringBuilder();
    res << "Read (" << TKqpTable(TableCallable).Path().StringValue() << "," << Alias << ", [";

    TStringBuf separator;
    for (const auto id : Columns_) {
        res << separator << registry.GetDebugName(id);
        separator = ", ";
    }
    res << "])";
    const TString storageType = StorageType == NYql::EStorageType::RowStorage ? "Row" : "Column";
    res << " (StorageType: " << storageType << ")";
    if (OlapFilterLambda) {
        THashMap<TString, TString> names;
        for (const auto id : GetRequiredColumns(ctx)) {
            names.emplace(ctx.GetIndexAsString(id), registry.GetDebugName(id));
        }
        res << " OlapFilter: (" << PrintRBOExpression(NOpt::TOlapFilterInspector::RenameColumns(OlapFilterLambda, names, ctx), ctx) << ")";
    }
    if (const auto ranges = GetRanges()) {
        res << " Ranges: (" << PrintRBOExpression(ranges, ctx) << ")";
    }
    if (SortDir != ESortDir::None) {
        res << " Sort direction: (" << ((SortDir == ESortDir::Asc) ? "ASC" : "DESC");
        res << ")";
    }
    if (OriginalPredicate.has_value()) {
        res << " Original predicate: (" << OriginalPredicate->ToString() << ")";
    }

    return res;
}

TInfoUnitId TMapElement::GetColumnAccess() const {
    Y_ENSURE(IsColumnAccess());
    const auto& ids = Expr.GetRawInputIUs();
    Y_ENSURE(ids.Size() == 1);
    return *ids.begin();
}

/**
 * OpMap operator methods
 */
TOpMap::TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, TMapIUs elements)
    : TOpMap(std::move(input), pos, TPhysicalOpProps{}, std::move(elements))
{}

TOpMap::TOpMap(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props,
    TMapIUs elements)
    : IUnaryOperator(EOperator::Map, pos, props, input)
    , MapElements(std::move(elements))
{}

void TOpMap::SetMapElements(TMapIUs elements) {
    MapElements = std::move(elements);
    Props.OutputIUs.reset();
}

void TOpMap::AddMapElement(TInfoUnitId output, TMapElement element) {
    MapElements.Add(output, std::move(element));
    Props.OutputIUs.reset();
}

void TOpMap::RemoveMapElement(TInfoUnitId output) {
    MapElements.Remove(output);
    Props.OutputIUs.reset();
}

void TOpMap::SetMapElementExpression(TInfoUnitId output, TExpression expression) {
    const auto* element = MapElements.Find(output);
    Y_ENSURE(element, "Unknown Map output " << output);
    // Output IDs are unchanged: a definition edit keeps the Map's output set.
    MapElements.Replace(output, std::move(expression));
}

void TOpMap::ComputeOutputIUs() {
    auto result = GetInput()->GetOutputIUs();
    Y_ENSURE(!result.HasAny(MapElements.Keys()), "Map definitions must have fresh IDs");
    result.UnionWith(MapElements.Keys());
    Props.OutputIUs = std::move(result);
}

TUnorderedIUs TOpMap::GetUsedIUs(TPlanProps& props) {
    TUnorderedIUs result;
    for (const auto& [id, element] : MapElements.Items()) {
        element.GetExpression().BindPlanProps(&props);
        result.UnionWith(element.GetExpression().GetInputIUs(false, true));
    }
    return result;
}

TVector<std::reference_wrapper<const TExpression>> TOpMap::GetExpressions() const {
    TVector<std::reference_wrapper<const TExpression>> result;
    result.reserve(MapElements.Items().size());
    for (const auto& [id, element] : MapElements.Items()) {
        result.push_back(std::cref(element.GetExpression()));
    }
    return result;
}

void TOpMap::ApplyReplaceMap(const TNodeOnNodeOwnedMap& replacements, TRBOContext& ctx) {
    // Replacing values keeps the map structure; keys and iterators stay valid.
    for (const auto& [id, element] : MapElements.Items()) {
        SetMapElementExpression(id, element.GetExpression().ApplyReplaceMap(replacements, ctx));
    }
}

TString TOpMap::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    TStringBuilder text;
    text << "Map [";
    TStringBuf separator;
    for (const auto id : MapElements.Keys()) {
        const auto& element = *MapElements.Find(id);
        text << separator << registry.GetDebugName(id) << " := " << element.GetExpression().ToString();
        separator = ", ";
    }
    return text << "]";
}

NJson::TJsonValue TOpMap::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto result = IOperator::ToJson(explainFlags, registry);
    TStringBuilder name;
    name << "Map [";
    TStringBuf separator;
    for (const auto id : MapElements.Keys()) {
        const auto& element = *MapElements.Find(id);
        name << separator << registry.GetDisplayName(id) << " := " << element.GetExpression().ToExplainString(registry);
        separator = ", ";
    }
    result["Name"] = name << "]";
    return result;
}

/**
 * OpAddDependencies methods
 */
TOpAddDependencies::TOpAddDependencies(TIntrusivePtr<IOperator> input, TPositionHandle pos, TDependencyIUs dependencies)
    : IUnaryOperator(EOperator::AddDependencies, pos, input)
{
    SetDependencies(std::move(dependencies));
}

void TOpAddDependencies::SetDependencies(TDependencyIUs dependencies) {
    Y_ENSURE(!dependencies.Keys().HasAny(dependencies.MappedIUs()), "Captures must have fresh local IDs");
    Dependencies = std::move(dependencies);
    Props.OutputIUs.reset();
}

bool TOpAddDependencies::RebindCaptures(const TSubstitutions& substitutions) {
    bool changed = false;
    auto dependencies = Dependencies;
    for (const auto& [local, capture] : Dependencies.Items()) {
        if (const auto outer = Substitute(capture.Outer, substitutions); outer != capture.Outer) {
            dependencies.Replace(local, TCapturedIU{outer, capture.Type});
            changed = true;
        }
    }
    if (changed) {
        SetDependencies(std::move(dependencies));
    }
    return changed;
}

void TOpAddDependencies::ComputeOutputIUs() {
    auto result = GetInput()->GetOutputIUs();
    Y_ENSURE(!result.HasAny(Dependencies.Keys()), "Captured IDs must be disjoint from the input");
    result.UnionWith(Dependencies.Keys());
    Props.OutputIUs = std::move(result);
}

TString TOpAddDependencies::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    TStringBuilder result;
    result << "Correlated [";
    TStringBuf separator;
    for (const auto local : Dependencies.Keys()) {
        result << separator << registry.GetDebugName(local) << " <- outer " << registry.GetDebugName(Dependencies.Find(local)->Outer);
        separator = ", ";
    }
    return result << "]";
}

/**
 * OpFilter operator methods
 */

TOpFilter::TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExpression& filterExpr)
    : IUnaryOperator(EOperator::Filter, pos, input)
    , FilterExpr(filterExpr) {
}

TOpFilter::TOpFilter(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props, const TExpression& filterExpr, bool partiallyPushedDown)
    : IUnaryOperator(EOperator::Filter, pos, props, input)
    , PartiallyPushedDown(partiallyPushedDown)
    , FilterExpr(filterExpr) {
}

TVector<std::reference_wrapper<const TExpression>> TOpFilter::GetExpressions() const {
    return {std::cref(FilterExpr)};
}

void TOpFilter::SetFilterExpression(TExpression filterExpr) {
    FilterExpr = std::move(filterExpr);
}

void TOpFilter::ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext & ctx) {
    SetFilterExpression(FilterExpr.ApplyReplaceMap(map, ctx));
}

TUnorderedIUs TOpFilter::GetFilterIUs(TPlanProps& props) const {
    FilterExpr.BindPlanProps(&props);
    return FilterExpr.GetInputIUs(true, true);
}

TUnorderedIUs TOpFilter::GetUsedIUs(TPlanProps& props) {
    FilterExpr.BindPlanProps(&props);
    return FilterExpr.GetInputIUs(false, true);
}

TString TOpFilter::ToString(TExprContext& ctx, const TInfoUnitRegistry&) {
    Y_UNUSED(ctx);
    return TStringBuilder() << "Filter :" << FilterExpr.ToString();
}

NJson::TJsonValue TOpFilter::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto res = IOperator::ToJson(explainFlags, registry);
    res["Predicate"] = FilterExpr.ToExplainString(registry);
    return res;
}

/**
 * OpJoin operator methods
 */

TOpJoin::TOpJoin(TIntrusivePtr<IOperator> left, TIntrusivePtr<IOperator> right, TPositionHandle pos,
    TString kind, TJoinIUs keys)
    : TOpJoin(std::move(left), std::move(right), pos, std::move(kind), std::move(keys), {})
{}

TOpJoin::TOpJoin(TIntrusivePtr<IOperator> left, TIntrusivePtr<IOperator> right, TPositionHandle pos,
    TString kind, TJoinIUs keys, const TVector<TExpression>& filters)
    : IBinaryOperator(EOperator::Join, pos, std::move(left), std::move(right))
    , JoinKind(std::move(kind))
    , JoinKeys(std::move(keys))
    , JoinFilters(filters)
{}

void TOpJoin::ComputeOutputIUs() {
    Y_ENSURE(!GetLeftInput()->GetOutputIUs().HasAny(GetRightInput()->GetOutputIUs()),
        "Join input IDs overlap: shared producers require distinct Replicate ports");
    TUnorderedIUs result;
    if (JoinOutputsLeft(JoinKind)) {
        result.UnionWith(GetLeftInput()->GetOutputIUs());
    }
    if (JoinOutputsRight(JoinKind)) {
        result.UnionWith(GetRightInput()->GetOutputIUs());
    }
    Props.OutputIUs = std::move(result);
}

const TUnorderedIUs& TOpJoin::GetUniqueRawInputIUs() const {
    RawInputIUs.Clear();
    for (const auto& expression : JoinFilters) {
        RawInputIUs.UnionWith(expression.GetRawInputIUs());
    }
    return RawInputIUs;
}

TUnorderedIUs TOpJoin::GetUsedIUs(TPlanProps& props) {
    auto result = JoinKeys.Left();
    result.UnionWith(JoinKeys.Right());
    for (const auto& filter : JoinFilters) {
        filter.BindPlanProps(&props);
        result.UnionWith(filter.GetInputIUs(false, true));
    }
    return result;
}

TVector<std::reference_wrapper<const TExpression>> TOpJoin::GetExpressions() const {
    TVector<std::reference_wrapper<const TExpression>> result;
    for (const auto& expr : JoinFilters) {
        result.push_back(std::cref(expr));
    }
    return result;
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

TString GetExplainJoinAlgoName(const TPhysicalOpProps& props) {
    Y_ENSURE(props.JoinAlgo.has_value(), "Join algorithm has not been selected");
    Y_ENSURE(props.UseBlockHashJoin.has_value(), "Physical join implementation has not been selected");

    if (*props.UseBlockHashJoin) {
        return "BlockHash";
    }

    const auto joinAlgo = *props.JoinAlgo;
    if (joinAlgo == NKqp::EJoinAlgoType::GraceJoin || joinAlgo == NKqp::EJoinAlgoType::ReverseBlockJoin) {
        return "Grace";
    }
    return GetJoinAlgoName(joinAlgo);
}

TString TOpJoin::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << "Join, Kind: " << JoinKind;
    if (Props.JoinAlgo.has_value()) {
        res << ", Algo: " << GetJoinAlgoName(*Props.JoinAlgo);
    }
    res << " [";
    TStringBuf separator;
    for (const auto& [left, right, equalNulls] : JoinKeys.Items()) {
        res << separator << registry.GetDebugName(left) << (equalNulls ? " IS NOT DISTINCT FROM " : "=") << registry.GetDebugName(right);
        separator = ", ";
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

static TString FormatJoinKeys(const TJoinIUs& keys, const TInfoUnitRegistry& registry, bool debug = false) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto& [left, right, equalNulls] : keys.Items()) {
        result << separator << (debug ? registry.GetDebugName(left) : registry.GetDisplayName(left)) << (equalNulls ? " IS NOT DISTINCT FROM " : " = ") << (debug ? registry.GetDebugName(right) : registry.GetDisplayName(right));
        separator = ", ";
    }
    return result;
}

NJson::TJsonValue TOpJoin::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto res = IOperator::ToJson(explainFlags, registry);
    const auto joinAlgoName = GetExplainJoinAlgoName(Props);

    if (JoinKind == "Cross") {
        res["Name"] = "CrossJoin";
    } else {
        res["Name"] = TStringBuilder() << JoinKind << "Join (" << joinAlgoName << ")";
    }
    res["JoinKind"] = JoinKind;
    res["JoinAlgo"] = joinAlgoName;
    if (!JoinKeys.Items().empty()) {
        res["Condition"] = FormatJoinKeys(JoinKeys, registry);
    }
    if (!JoinFilters.empty()) {
        NJson::TJsonValue filters(NJson::EJsonValueType::JSON_ARRAY);
        for (const auto& filter : JoinFilters) {
            filters.AppendValue(filter.ToExplainString(registry));
        }
        res["Filters"] = filters;
    }

    return res;
}

/**
 * OpDependentJoin.
 * Note: it does not have runtime support. We have to eliminate it or to rewrite it.
 */

TOpDependentJoin::TOpDependentJoin(TIntrusivePtr<IOperator> domain, TIntrusivePtr<IOperator> input,
    TUnorderedIUs dependencies, TPositionHandle pos, TSubstitutions domainColumns)
    : IBinaryOperator(EOperator::DependentJoin, pos, std::move(domain), std::move(input))
    , Dependencies(std::move(dependencies))
    , DomainColumns(std::move(domainColumns))
{
    Y_ENSURE(!Dependencies.Empty(), "Dependent join must have correlated columns");
}

TUnorderedIUs TOpDependentJoin::GetDomainColumns() const {
    TUnorderedIUs result;
    result.Assign(Dependencies | std::views::transform([&](TInfoUnitId parameter) { return GetDomainColumn(parameter); }));
    return result;
}

void TOpDependentJoin::ComputeOutputIUs() {
    auto result = GetDomain()->GetOutputIUs();
    result.UnionWith(GetInput()->GetOutputIUs());
    Props.OutputIUs = std::move(result);
}

TString TOpDependentJoin::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    return TStringBuilder() << "DependentJoin, Domain: [" << FormatIds(Dependencies, &registry, true) << "]";
}

/**
 * OpUnionAll operator methods
 */

TOpUnionAll::TOpUnionAll(TVector<TIntrusivePtr<IOperator>> inputs, TPositionHandle pos, TUnionAllIUs columns, bool ordered)
    : IVariadicOperator(EOperator::UnionAll, pos, std::move(inputs))
    , Ordered(ordered)
    , Columns(std::move(columns))
{
    Y_ENSURE(GetChildCount() >= 2, "UnionAll must have at least two inputs");
    Y_ENSURE(Columns.Policy().ChildCount == GetChildCount(), "UnionAll mapping arity differs from its inputs");
}

TOpUnionAll::TOpUnionAll(TIntrusivePtr<IOperator> left, TIntrusivePtr<IOperator> right, TPositionHandle pos,
    TUnionAllIUs columns, bool ordered)
    : TOpUnionAll([&] {
        TVector<TIntrusivePtr<IOperator>> inputs;
        inputs.push_back(std::move(left));
        inputs.push_back(std::move(right));
        return inputs;
    }(), pos, std::move(columns), ordered)
{}

TString TOpUnionAll::ToString(TExprContext& ctx, const TInfoUnitRegistry&) {
    Y_UNUSED(ctx);
    return "UnionAll";
}

NJson::TJsonValue TOpUnionAll::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto result = IOperator::ToJson(explainFlags, registry);
    result["Ordered"] = Ordered;
    result["Columns"] = TStringBuilder() << "{" << FormatIds(Columns.Keys(), &registry) << "}";
    return result;
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

// Recompute output IUs for now

const TUnorderedIUs& TOpLimit::GetUniqueRawInputIUs() const {
    RawInputIUs = LimitCond.GetRawInputIUs();
    if (OffsetCond) {
        RawInputIUs.UnionWith(OffsetCond->GetRawInputIUs());
    }
    return RawInputIUs;
}

TUnorderedIUs TOpLimit::GetUsedIUs(TPlanProps& props) {
    LimitCond.BindPlanProps(&props);
    auto result = LimitCond.GetInputIUs(false, true);
    if (OffsetCond) {
        OffsetCond->BindPlanProps(&props);
        result.UnionWith(OffsetCond->GetInputIUs(false, true));
    }
    return result;
}

TString TOpLimit::ToString(TExprContext& ctx, const TInfoUnitRegistry&) {
    Y_UNUSED(ctx);
    TStringBuilder builder;
    builder << "Limit: " << LimitCond.ToString() << " ";
    if (OffsetCond.has_value()) {
        builder << "Offset: " << OffsetCond->ToString() << " ";
    }
    builder << "Phase: " << ToStringPhase(LimitPhase);
    return builder;
}

NJson::TJsonValue TOpLimit::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto res = IOperator::ToJson(explainFlags, registry);
    res["Limit"] = LimitCond.ToExplainString(registry);
    if (OffsetCond) {
        res["Offset"] = OffsetCond->ToExplainString(registry);
    }
    if (LimitPhase != EOpPhase::Undefined) {
        res["Phase"] = ToStringPhase(LimitPhase);
    }
    return res;
}

TVector<std::reference_wrapper<const TExpression>> TOpLimit::GetExpressions() const {
    TVector<std::reference_wrapper<const TExpression>> result{std::cref(LimitCond)};
    if (OffsetCond) {
        result.push_back(std::cref(*OffsetCond));
    }
    return result;
}

/**
 * Sort operator
 * FIXME: This is temporary, we want to get enforcers working
 */
TOpSort::TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, TSortIUs keys, std::optional<TExpression> limit)
    : TOpSort(std::move(input), pos, TPhysicalOpProps{}, std::move(keys), std::move(limit), EOpPhase::Undefined)
{}

TOpSort::TOpSort(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TPhysicalOpProps& props,
    TSortIUs keys, std::optional<TExpression> limit, EOpPhase phase)
    : IUnaryOperator(EOperator::Sort, pos, props, input)
    , LimitCond(std::move(limit))
    , SortElements(std::move(keys))
    , SortPhase(phase)
{}

const TUnorderedIUs& TOpSort::GetUniqueRawInputIUs() const {
    return LimitCond ? LimitCond->GetRawInputIUs() : IOperator::GetUniqueRawInputIUs();
}

TUnorderedIUs TOpSort::GetUsedIUs(TPlanProps& props) {
    auto result = SortElements.Unordered();
    if (LimitCond) {
        LimitCond->BindPlanProps(&props);
        result.UnionWith(LimitCond->GetInputIUs(false, true));
    }
    return result;
}

TVector<std::reference_wrapper<const TExpression>> TOpSort::GetExpressions() const {
    return LimitCond ? TVector<std::reference_wrapper<const TExpression>>{std::cref(*LimitCond)}
                     : TVector<std::reference_wrapper<const TExpression>>{};
}

TString TOpSort::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    TStringBuilder text;
    text << "Sort: [";
    TStringBuf separator;
    for (const auto& [id, order] : SortElements.Items()) {
        text << separator << registry.GetDebugName(id) << (order.Ascending ? " asc" : " desc");
        separator = ", ";
    }
    text << "]";
    if (LimitCond) {
        text << ", Limit: " << LimitCond->ToString();
    }
    return text << " Phase: " << ToStringPhase(SortPhase);
}

TOpTableLookup::TOpTableLookup(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExprNode::TPtr& table,
    TUnorderedIUs columns, TLookupKeys keys)
    : IUnaryOperator(EOperator::TableLookup, pos, input)
    , Table(table)
    , LookupKeys(std::move(keys))
    , Columns(std::move(columns))
{}

TOpTableLookup::TOpTableLookup(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TExprNode::TPtr& table,
    TUnorderedIUs columns, TLookupKeys keys, const TString& kind, const std::optional<TExpression>& filter,
    const std::optional<TLookupKeyPrefix>& prefix, TJoinIUs residualKeys)
    : TOpTableLookup(std::move(input), pos, table, std::move(columns), std::move(keys))
{
    Y_ENSURE(!LookupKeys.Items().empty(), "Lookup join needs at least one key");
    Y_ENSURE(kind == "Inner" || kind == "Left" || kind == "LeftSemi" || kind == "LeftOnly", "Unsupported lookup join kind");
    if (prefix) {
        Y_ENSURE(prefix->Points && prefix->PointsItemType && !prefix->Columns.empty(), "Invalid lookup key prefix");
    }
    JoinKind = kind;
    FetchedRowFilter = filter;
    Prefix = prefix;
    ResidualJoinKeys = std::move(residualKeys);
    Strategy = ELookupStrategy::LookupJoinRows;
}

void TOpTableLookup::ComputeOutputIUs() {
    auto result = Columns;
    if (IsJoin()) {
        result.UnionWith(GetInput()->GetOutputIUs());
    }
    Props.OutputIUs = std::move(result);
}

const TUnorderedIUs& TOpTableLookup::GetUniqueRawInputIUs() const {
    return FetchedRowFilter ? FetchedRowFilter->GetRawInputIUs() : IOperator::GetUniqueRawInputIUs();
}

TUnorderedIUs TOpTableLookup::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);
    auto result = LookupKeys.Unordered();
    if (Prefix) {
        result.UnionWith(Prefix->Equalities.Unordered());
    }
    return result;
}

TVector<std::reference_wrapper<const TExpression>> TOpTableLookup::GetExpressions() const {
    return FetchedRowFilter ? TVector<std::reference_wrapper<const TExpression>>{std::cref(*FetchedRowFilter)}
                            : TVector<std::reference_wrapper<const TExpression>>{};
}

static TString FormatLookupKeys(const TOpTableLookup::TLookupKeys& keys, const TInfoUnitRegistry* registry = nullptr, bool debug = false) {
    TStringBuilder text;
    TStringBuf separator;
    for (const auto& [id, column] : keys.Items()) {
        text << separator << FormatIds(std::views::single(id), registry, debug) << " = " << column;
        separator = ", ";
    }
    return text;
}

TString TOpTableLookup::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << GetExplainName() << ": " << TKqpTable(Table).Path().StringValue();
    if (IsJoin()) {
        res << ", kind: " << JoinKind;
    }
    res << ", keys: [";
    TStringBuf separator;
    for (const auto& [id, column] : LookupKeys.Items()) {
        res << separator << registry.GetDebugName(id);
        if (IsJoin()) {
            res << " = " << column;
        }
        separator = ", ";
    }
    res << "], columns: [" << FormatIds(Columns, &registry, true) << "]";
    if (Prefix) {
        res << ", key prefix: [" << JoinSeq(", ", Prefix->Columns) << "]";
        for (const auto& [id, column] : Prefix->Equalities.Items()) {
            res << ", " << column << " = " << registry.GetDebugName(id);
        }
    }
    if (FetchedRowFilter) {
        res << ", filter: " << FetchedRowFilter->ToString();
    }
    for (const auto& [left, right, equalNulls] : ResidualJoinKeys.Items()) {
        res << ", residual: " << registry.GetDebugName(left) << (equalNulls ? " IS NOT DISTINCT FROM " : " = ") << registry.GetDebugName(right);
    }
    return res;
}

NJson::TJsonValue TOpTableLookup::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto result = IOperator::ToJson(explainFlags, registry);
    result["Table"] = TKqpTable(Table).Path().StringValue();
    if (IsJoin()) {
        result["JoinKind"] = JoinKind;
        TStringBuilder condition;
        condition << FormatLookupKeys(LookupKeys, &registry);
        if (Prefix) {
            if (!Prefix->Equalities.Items().empty()) {
                condition << ", " << FormatLookupKeys(Prefix->Equalities, &registry);
            }
            result["LookupKeyPrefix"] = JoinSeq(", ", Prefix->Columns);
        }
        if (!ResidualJoinKeys.Items().empty()) {
            condition << ", " << FormatJoinKeys(ResidualJoinKeys, registry);
        }
        result["Condition"] = condition;
    }
    if (FetchedRowFilter) {
        result["Predicate"] = FetchedRowFilter->ToExplainString(registry);
    }
    return result;
}

/**
 * OpIndexLookupJoin operator methods
 */

TOpIndexLookupJoin::TOpIndexLookupJoin(TIntrusivePtr<IOperator> input, TPositionHandle pos, const TString& joinKind,
                                       TJoinIUs joinKeys)
    : IUnaryOperator(EOperator::IndexLookupJoin, pos, input)
    , JoinKind(joinKind)
    , JoinKeys(std::move(joinKeys)) {
}

const TOpTableLookup& TOpIndexLookupJoin::GetTableLookup() const {
    const auto& input = (*GetInput());
    Y_ENSURE(input.Kind == EOperator::TableLookup, "Index lookup join must be fed by a table lookup");
    const auto& lookup = CastOperator<TOpTableLookup>(input);
    Y_ENSURE(lookup.IsJoin(), "Index lookup join must be fed by a table lookup in join mode");
    return lookup;
}

TOpTableLookup& TOpIndexLookupJoin::GetTableLookup() {
    return const_cast<TOpTableLookup&>(std::as_const(*this).GetTableLookup());
}

const TUnorderedIUs& TOpIndexLookupJoin::GetOutputIUs() {
    return JoinOutputsRight(JoinKind) ? GetInput()->GetOutputIUs()
                                     : GetTableLookup().GetInput()->GetOutputIUs();
}

TString TOpIndexLookupJoin::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << "IndexLookupJoin, Kind: " << JoinKind << " [" << FormatJoinKeys(JoinKeys, registry, true) << "]";
    return res;
}

NJson::TJsonValue TOpIndexLookupJoin::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto res = IOperator::ToJson(explainFlags, registry);
    res["Name"] = TStringBuilder() << JoinKind << "Join (Lookup)";
    res["JoinKind"] = JoinKind;
    res["JoinAlgo"] = "Lookup";
    if (!JoinKeys.Items().empty()) {
        res["Condition"] = FormatJoinKeys(JoinKeys, registry);
    }
    return res;
}

NJson::TJsonValue TOpSort::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto res = IOperator::ToJson(explainFlags, registry);
    if (IsTopSort()) {
        res["TopSortBy"] = FormatSortElements(SortElements, registry);
        if (LimitCond) {
            res["Limit"] = LimitCond->ToExplainString(registry);
        }
    } else {
        res["SortBy"] = FormatSortElements(SortElements, registry);
    }
    if (SortPhase != EOpPhase::Undefined) {
        res["Phase"] = ToStringPhase(SortPhase);
    }
    return res;
}

/**
 * OpAggregate operator methods
 */
TOpAggregate::TOpAggregate(TIntrusivePtr<IOperator> input, TAggregationIUs aggregations, TOrderedIUs<> keys,
    EOpPhase phase, bool distinctAll, TPositionHandle pos)
    : TOpAggregate(std::move(input), std::move(aggregations), std::move(keys), phase, distinctAll, TPhysicalOpProps{}, pos)
{}

TOpAggregate::TOpAggregate(TIntrusivePtr<IOperator> input, TAggregationIUs aggregations, TOrderedIUs<> keys,
    EOpPhase phase, bool distinctAll, const TPhysicalOpProps& props, TPositionHandle pos)
    : IUnaryOperator(EOperator::Aggregate, pos, props, input)
    , Aggregations(std::move(aggregations))
    , KeyColumns(std::move(keys))
    , AggregationPhase(phase)
    , DistinctAll(distinctAll)
{}

void TOpAggregate::ComputeOutputIUs() {
    auto result = Aggregations.Keys();
    if (!DistinctAll) {
        result.UnionWith(KeyColumns.Unordered());
    }
    Props.OutputIUs = std::move(result);
}

TUnorderedIUs TOpAggregate::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);
    // Grouping/distinct keys pass through; only aggregation inputs are used.
    return DistinctAll ? TUnorderedIUs{} : Aggregations.MappedIUs();
}

TString TOpAggregate::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    TStringBuilder text;
    text << "Aggregate [";
    TStringBuf separator;
    for (const auto id : Aggregations.Keys()) {
        const auto& aggregation = *Aggregations.Find(id);
        text << separator << registry.GetDebugName(id) << ": " << aggregation.AggFunction << "("
             << (aggregation.Distinct ? "distinct " : "") << registry.GetDebugName(aggregation.Input) << ")";
        separator = ", ";
    }
    return text << " [" << FormatIds(KeyColumns.Items(), &registry, true) << "]] "
                << (DistinctAll ? " (Distinct all) " : "") << ToStringPhase(AggregationPhase);
}

NJson::TJsonValue TOpAggregate::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto result = IOperator::ToJson(explainFlags, registry);
    if (!KeyColumns.Items().empty()) {
        result["GroupBy"] = FormatIds(KeyColumns.Items(), &registry);
    }
    if (!Aggregations.Items().empty()) {
        TStringBuilder aggregation;
        aggregation << "{";
        TStringBuf separator;
        for (const auto id : Aggregations.Keys()) {
            const auto& traits = *Aggregations.Find(id);
            aggregation << separator << registry.GetDisplayName(id) << ": " << traits.AggFunction << "(" << registry.GetDisplayName(traits.Input) << ")";
            separator = ", ";
        }
        aggregation << "}";
        result["Aggregation"] = aggregation;
    }
    result["Phase"] = ToStringPhase(AggregationPhase);
    if (DistinctAll) {
        result["Distinct"] = "All";
    }
    return result;
}

/**
 * OpGroupingSets operator. Logical representation of grouping sets.
 */
TOpGroupingSets::TOpGroupingSets(TIntrusivePtr<TOpAggregate> input, TVector<TUnorderedIUs> groupingSets,
    TMappedIUs<TInfoUnitId> columns, TPositionHandle pos, TMappedIUs<TInfoUnitId> groupingIndicators)
    : IUnaryOperator(EOperator::GroupingSets, pos, input)
    , GroupingSets(std::move(groupingSets))
    , Columns(std::move(columns))
    , GroupingIndicators(std::move(groupingIndicators)) {
    Y_ENSURE(!GroupingSets.empty(), "Grouping sets list must not be empty");
    for (const auto& keys : GroupingSets) {
        Y_ENSURE(keys.IsSubsetOf(CastOperator<TOpAggregate>((*GetInput())).GetKeyColumns().Unordered()), "Unknown grouping-set key");
    }
    Y_ENSURE(!Columns.Keys().HasAny(GetInput()->GetOutputIUs()), "Grouping sets must define fresh output bindings");
    for (const auto& [output, source] : Columns.Items()) {
        Y_ENSURE(GetInput()->GetOutputIUs().Contains(source), "Unknown grouping-set output source");
    }
    Y_ENSURE(!GroupingIndicators.Keys().HasAny(GetInput()->GetOutputIUs()) && !GroupingIndicators.Keys().HasAny(Columns.Keys()),
        "Grouping indicators must define fresh output bindings");
    for (const auto& [output, source] : GroupingIndicators.Items()) {
        Y_ENSURE(CastOperator<TOpAggregate>((*GetInput())).GetKeyColumns().Unordered().Contains(source),
            "Unknown grouping-indicator key");
    }
}

void TOpGroupingSets::ComputeOutputIUs() {
    auto result = Columns.Keys();
    result.UnionWith(GroupingIndicators.Keys());
    Props.OutputIUs = std::move(result);
}

TString TOpGroupingSets::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);

    TStringBuilder result;
    result << "GroupingSets [";
    for (size_t setIndex = 0; setIndex < GroupingSets.size(); ++setIndex) {
        if (setIndex != 0) {
            result << ", ";
        }
        result << "(" << FormatIds(GroupingSets[setIndex], &registry, true) << ")";
    }
    result << "] Columns [";
    TStringBuf separator;
    for (const auto output : Columns.Keys()) {
        result << separator << registry.GetDebugName(output) << " <- " << registry.GetDebugName(*Columns.Find(output));
        separator = ", ";
    }
    for (const auto& [output, key] : GroupingIndicators.Items()) {
        result << separator << registry.GetDebugName(output) << " <- GROUPING(" << registry.GetDebugName(key) << ")";
        separator = ", ";
    }
    return result << "]";
}

/***
 * OpWindow operator methods
 */
TString ToStringWindowFuncKind(EWindowFuncKind kind) {
    return kind == EWindowFuncKind::Aggregate ? "Aggregate" : "Native";
}

EWindowFuncKind WindowFuncKindFromString(const TString& kind) {
    if (kind == "Aggregate") {
        return EWindowFuncKind::Aggregate;
    }
    Y_ENSURE(kind == "Native", "Unknown window function kind: " << kind);
    return EWindowFuncKind::Native;
}

TString ToStringWindowFrameType(EWindowFrameType type) {
    switch (type) {
        case EWindowFrameType::Rows:
            return "Rows";
        case EWindowFrameType::Range:
            return "Range";
        case EWindowFrameType::Groups:
            return "Groups";
    }
    Y_ENSURE(false, "Unknown window frame type");
}

EWindowFrameType WindowFrameTypeFromString(const TString& type) {
    if (type == "Rows") {
        return EWindowFrameType::Rows;
    } else if (type == "Range") {
        return EWindowFrameType::Range;
    }
    Y_ENSURE(type == "Groups", "Unknown window frame type: " << type);
    return EWindowFrameType::Groups;
}

TString ToStringWindowFrameBound(EWindowFrameBound bound) {
    switch (bound) {
        case EWindowFrameBound::UnboundedPreceding:
            return "UnboundedPreceding";
        case EWindowFrameBound::Preceding:
            return "Preceding";
        case EWindowFrameBound::CurrentRow:
            return "CurrentRow";
        case EWindowFrameBound::Following:
            return "Following";
        case EWindowFrameBound::UnboundedFollowing:
            return "UnboundedFollowing";
    }
    Y_ENSURE(false, "Unknown window frame bound");
}

EWindowFrameBound WindowFrameBoundFromString(const TString& bound) {
    if (bound == "UnboundedPreceding") {
        return EWindowFrameBound::UnboundedPreceding;
    } else if (bound == "Preceding") {
        return EWindowFrameBound::Preceding;
    } else if (bound == "CurrentRow") {
        return EWindowFrameBound::CurrentRow;
    } else if (bound == "Following") {
        return EWindowFrameBound::Following;
    }
    Y_ENSURE(bound == "UnboundedFollowing", "Unknown window frame bound: " << bound);
    return EWindowFrameBound::UnboundedFollowing;
}

TOpWindow::TOpWindow(TIntrusivePtr<IOperator> input, TPositionHandle pos, TWindowIUs functions,
    TOrderedIUs<> partitionKeys, TSortIUs sortKeys, const TOpWindowFrame& frame)
    : IUnaryOperator(EOperator::Window, pos, input)
    , WindowFuncs(std::move(functions))
    , PartitionKeys(std::move(partitionKeys))
    , SortElements(std::move(sortKeys))
    , Frame(frame)
{}

void TOpWindow::ComputeOutputIUs() {
    auto result = GetInput()->GetOutputIUs();
    result.UnionWith(WindowFuncs.Keys());
    Props.OutputIUs = std::move(result);
}

TUnorderedIUs TOpWindow::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);
    auto result = WindowFuncs.MappedIUs();
    result.UnionWith(PartitionKeys.Unordered());
    result.UnionWith(SortElements.Unordered());
    return result;
}

static TString FormatWindowFunctions(const TWindowIUs& functions, const TInfoUnitRegistry* registry = nullptr, bool debug = false) {
    TStringBuilder text;
    TStringBuf separator;
    for (const auto id : functions.Keys()) {
        const auto& function = *functions.Find(id);
        text << separator << FormatIds(std::views::single(id), registry, debug) << ": " << function.Function
             << "(" << FormatIds(function.Arguments.Items(), registry, debug) << ")";
        separator = ", ";
    }
    return text;
}

TString TOpWindow::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    Y_UNUSED(ctx);
    TStringBuilder text;
    text << "Window [" << FormatWindowFunctions(WindowFuncs, &registry, true) << "]";
    if (!PartitionKeys.Items().empty()) {
        text << " PartitionBy: [" << FormatIds(PartitionKeys.Items(), &registry, true) << "]";
    }
    if (!SortElements.Items().empty()) {
        text << " OrderBy: [";
        TStringBuf separator;
        for (const auto& [id, order] : SortElements.Items()) {
            text << separator << registry.GetDebugName(id) << (order.Ascending ? " asc" : " desc");
            separator = ", ";
        }
        text << "]";
    }
    text << " Frame: " << ToStringWindowFrameType(Frame.Type) << "[" << ToStringWindowFrameBound(Frame.BeginKind);
    if (Frame.BeginKind == EWindowFrameBound::Preceding || Frame.BeginKind == EWindowFrameBound::Following) {
        text << " " << Frame.BeginValue;
    }
    text << ", " << ToStringWindowFrameBound(Frame.EndKind);
    if (Frame.EndKind == EWindowFrameBound::Preceding || Frame.EndKind == EWindowFrameBound::Following) {
        text << " " << Frame.EndValue;
    }
    return text << "]";
}

NJson::TJsonValue TOpWindow::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry) {
    auto result = IOperator::ToJson(explainFlags, registry);
    result["WindowFunctions"] = TStringBuilder() << "{" << FormatWindowFunctions(WindowFuncs, &registry) << "}";
    if (!PartitionKeys.Items().empty()) {
        result["PartitionBy"] = FormatIds(PartitionKeys.Items(), &registry);
    }
    if (!SortElements.Items().empty()) {
        result["OrderBy"] = FormatIds(SortElements.Items() | std::views::keys, &registry);
    }
    result["Frame"] = ToStringWindowFrameType(Frame.Type);
    return result;
}

/***
 * OpCBOTree operator methods
 */
TOpCBOTree::TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TPositionHandle pos) :
    IOperator(EOperator::CBOTree, pos),
    TreeRoot(std::move(treeRoot)),
    TreeNodes({TreeRoot})
{
    RebuildChildren();
}

TOpCBOTree::TOpCBOTree(TIntrusivePtr<IOperator> treeRoot, TVector<TIntrusivePtr<IOperator>> treeNodes, TPositionHandle pos) :
    IOperator(EOperator::CBOTree, pos),
    TreeRoot(std::move(treeRoot)),
    TreeNodes(std::move(treeNodes))
{
    RebuildChildren();
}

void TOpCBOTree::ComputeOutputIUs() {
    // TreeNodes are stored in post-order, so boundary inputs have already been
    // computed and every packed node can be refreshed before its parent.
    for (const auto& node : TreeNodes) {
        node->ComputeOutputIUs();
    }

}

void TOpCBOTree::RebuildChildren() {
    BoundaryInputs_.clear();

    THashSet<IOperator*> treeNodeSet;
    for (const auto& node : TreeNodes) {
        treeNodeSet.insert(node.Get());
    }

    for (const auto& node : TreeNodes) {
        for (size_t index = 0; index < node->GetChildCount(); ++index) {
            if (treeNodeSet.contains(node->GetChild(index).Get())) {
                continue;
            }

            BoundaryInputs_.emplace_back(node.Get(), index);
        }
    }
}

TIntrusivePtr<IOperator>& TOpCBOTree::GetChild(size_t index) {
    const auto& [parent, slot] = BoundaryInputs_.at(index);
    return parent->GetChild(slot);
}

const TIntrusivePtr<IOperator>& TOpCBOTree::GetChild(size_t index) const {
    const auto& [parent, slot] = BoundaryInputs_.at(index);
    return std::as_const(*parent).GetChild(slot);
}

TString TOpCBOTree::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry) {
    TStringBuilder res;
    res << "CBO Tree: [";
    for (size_t i=0; i < TreeNodes.size(); i++) {
        res << TreeNodes[i]->ToString(ctx, registry);
        if (i != TreeNodes.size()-1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

/**
* Table Effect operator methods: these are inserts/updates/deletes
*/
TOpTableEffect::TOpTableEffect(TIntrusivePtr<IOperator> input, TPositionHandle pos, TExprNode::TPtr table,
    EEffectType type, TEffectOptions options, TOrderedIUs<TString> columns, TOrderedIUs<TString> returning)
    : IUnaryOperator(EOperator::TableEffect, pos, input)
    , Table(std::move(table))
    , EffectType(type)
    , Options(std::move(options))
    , Columns_(std::move(columns))
    , ReturningColumns_(std::move(returning))
{}

TUnorderedIUs TOpTableEffect::GetUsedIUs(TPlanProps& props) {
    Y_UNUSED(props);
    return Columns_.Unordered();
}

TString TOpTableEffect::GetExplainName() const {
    switch (EffectType) {
        case EEffectType::InsertRows:
        case EEffectType::InsertRowsIndex:
            return "InsertRows";
        case EEffectType::UpdateRows:
        case EEffectType::UpdateRowsIndex:
            return "UpdateRows";
        case EEffectType::DeleteRows:
        case EEffectType::DeleteRowsIndex:
            return "DeleteRows";
        default:
            Y_ENSURE(false, "Uknown table effect type");
    }
}

TString TOpTableEffect::ToString(TExprContext& ctx, const TInfoUnitRegistry&) {
    Y_UNUSED(ctx);
    return GetExplainName();
}

TExprNode::TPtr TOpTableEffect::BuildSettings(TExprContext& ctx) {
    if (Options.ReturningColumns.has_value() && Options.ReturningColumns->size()) {
        Y_ENSURE(false, "Returning columns not supported in new optimizer");
    }

    TString mode;
    
    switch(EffectType) {
        case EEffectType::InsertRows:
        case EEffectType::InsertRowsIndex:
            mode = "insert";
            break;
        case EEffectType::UpdateRows:
        case EEffectType::UpdateRowsIndex:
            mode = "update";
            break;
        case EEffectType::UpsertRows:
        case EEffectType::UpsertRowsIndex:
            mode = "upsert";
            break;
        case EEffectType::DeleteRows:
        case EEffectType::DeleteRowsIndex:
            mode = "delete";
            break;
        default:
            Y_ENSURE(false, "Unsupported DML in new optimizer");
    }

    TString isBatch = "false";
    if (Options.IsBatch.has_value() && Options.IsBatch.value()){
        isBatch = "true";
    }

    TVector<TExprNode::TPtr> defaultColumns;
    if (Options.DefaultColumns.has_value()) {
        for (auto c : Options.DefaultColumns.value()) {
            defaultColumns.push_back(ctx.NewAtom(Pos, c));
        }
    }

    TVector<TExprNode::TPtr> settings;
    if (Options.Settings.has_value()) {
        settings = Options.Settings.value();
    }

    return Build<TKqpTableSinkSettings>(ctx, Pos)
            .Table(Table)
            .InconsistentWrite().Build("false")
            .Mode().Build(mode)
            .Priority().Build("0")
            .StreamWrite().Build("false")
            .IsBatch().Build(isBatch)
            .IsIndexImplTable().Build("false")
            .DefaultColumns()
                .Add(defaultColumns)
            .Build()
            .ReturningColumns().Build()
            .Settings()
                .Add(settings)
            .Build()
            .Done().Ptr();
}

/**
 * OpRoot operator methods
 */

namespace {

bool IsEmptyPassthrough(const IOperator& op) {
    switch (op.Kind) {
        case EOperator::Map:
            return CastOperator<TOpMap>(op).GetMapElements().Keys().Empty();
        case EOperator::Window:
            return CastOperator<TOpWindow>(op).GetWindowFuncs().Keys().Empty();
        case EOperator::AddDependencies:
            return CastOperator<TOpAddDependencies>(op).GetDependencies().Keys().Empty();
        default:
            return false;
    }
}

// A scalar aggregate yields exactly one row, even for empty input. Without
// results that row carries nothing, so the input subtree is irrelevant.
bool IsResultlessScalarAggregate(const IOperator& op) {
    if (op.Kind != EOperator::Aggregate) {
        return false;
    }
    const auto& aggregate = CastOperator<TOpAggregate>(op);
    return !aggregate.IsDistinctAll()
        && aggregate.GetAggregationPhase() != EOpPhase::Intermediate
        && aggregate.GetKeyColumns().Items().empty()
        && aggregate.GetAggregationTraits().Keys().Empty();
}

void BypassEmptyOperators(TOpRoot& root, TExprContext& ctx) {
    root.ComputeParents();
    TVector<TIntrusivePtr<IOperator>*> pending{&root.MutableChild(0)};
    for (const auto& [id, subplan] : root.PlanProps.Subplans) {
        pending.push_back(&root.PlanProps.Subplans.MutablePlan(id));
    }
    absl::flat_hash_set<TReplicate*> visited;
    while (!pending.empty()) {
        auto& owner = *pending.back();
        pending.pop_back();
        // No borrowed traversal survives deletion. Descendant slots are queued
        // only after their owner is final.
        while (true) {
            if (IsResultlessScalarAggregate(*owner)) {
                owner = MakeIntrusive<TOpEmptySource>(owner->Pos);
            } else if (IsEmptyPassthrough(*owner)) {
                auto input = CastOperator<IUnaryOperator>(*owner).GetInput();
                owner = std::move(input);
            } else if (!TOpReplicate::TryCollapse(owner, ctx, root.PlanProps)) {
                break;
            }
        }
        if (owner->Kind == EOperator::Replicate) {
            auto& hub = CastOperator<TOpReplicate>(*owner).GetReplicate();
            if (visited.insert(&hub).second) {
                pending.push_back(&hub.GetInput());
            }
        } else {
            for (size_t child = 0; child < owner->GetChildCount(); ++child) {
                pending.push_back(&owner->MutableChild(child));
            }
        }
    }
    // Dropped subtrees may have contained Replicate ports.
    root.ComputeParents();
}

void InvalidateProperties(TOpRoot& root) {
    for (const auto& item : IterateSubtreeWithSubplans(&root, root.PlanProps)) {
        auto& op = *item.Current;
        op.Props.OutputIUs.reset();
        op.Props.Metadata.reset();
        op.Props.Statistics.reset();
        op.Props.ClearLogicalAnalysis();
        op.Type = nullptr;
        op.Parents.clear();
    }
    root.RecomputeOutputIUsSubtree();
}

} // anonymous namespace

void FinishLogicalRewrite(TOpRoot& root, TExprContext& ctx) {
    BypassEmptyOperators(root, ctx);
    InvalidateProperties(root);
}

TOpRoot::TOpRoot(TIntrusivePtr<IOperator> input, TPositionHandle pos, TOrderedIUs<TString> columns,
    TVector<TString> queryColumns)
    : IUnaryOperator(EOperator::Root, pos, input)
    , Columns_(std::move(columns))
    , QueryColumns_(std::move(queryColumns))
{}

void TOpRoot::ComputeOutputIUsSubtree() {
    for (const auto& item : *this) {
        if (!item.Current->Props.OutputIUs.has_value()) {
            item.Current->ComputeOutputIUs();
        }
    }
    if (!Props.OutputIUs.has_value()) {
        ComputeOutputIUs();
    }
}

void TOpRoot::RecomputeOutputIUsSubtree() {
    for (const auto& item : *this) {
        item.Current->ComputeOutputIUs();
    }
    ComputeOutputIUs();
}

void TOpRoot::ComputeParents() {
    // Root postorder visits active operators once and clears every child before its parents add edges.
    absl::flat_hash_set<TReplicate*> replicates;
    for (const auto& item : IterateSubtreeWithSubplans(this, PlanProps)) {
        auto& op = item.Current;
        op->Parents.clear();
        if (op->Kind == EOperator::Replicate) {
            auto* port = CastOperator<TOpReplicate>(op);
            auto& replicate = port->GetReplicate();
            if (replicates.insert(&replicate).second) {
                replicate.Outputs_.clear();
            }
            replicate.Outputs_.push_back(port);
        }
        for (ui32 childIndex = 0; childIndex < op->GetChildCount(); ++childIndex) {
            op->GetChild(childIndex)->Parents.emplace_back(op, childIndex);
        }
    }
}

TString TOpRoot::ToString(TExprContext& ctx, const TInfoUnitRegistry&) {
    Y_UNUSED(ctx);
    return "Root";
}

TString TOpRoot::PlanToString(TExprContext& ctx, ui32 printOptions) {
    auto builder = TStringBuilder();
    for (const auto& [binding, subplan] : PlanProps.Subplans) {
        builder << "Subplan binding to " << PlanProps.InfoUnitRegistry.GetDebugName(binding) << ":\n";
        PlanToStringRec(subplan.Plan.Get(), ctx, builder, 0, printOptions);
    }
    PlanToStringRec(GetInput().Get(), ctx, builder, 0, printOptions);
    return builder;
}

void TOpRoot::PlanToStringRec(IOperator* op, TExprContext& ctx, TStringBuilder& builder, int tabs, ui32 printOptions) const {
    TStringBuilder tabString;
    for (int i = 0; i < tabs; i++) {
        tabString << "  ";
    }

    builder << tabString << op->ToString(ctx, PlanProps.InfoUnitRegistry);
    if (op->Props.StageId.has_value()) {
        builder << " StageId: " << *op->Props.StageId;
    }
    builder << "\n";

    if (printOptions & (EPrintPlanOptions::PrintBasicMetadata | EPrintPlanOptions::PrintFullMetadata) && op->Props.Metadata.has_value()) {
        builder << tabString << " ";
        builder << op->Props.Metadata->ToString(printOptions, PlanProps.InfoUnitRegistry);
        if (printOptions & EPrintPlanOptions::PrintFullMetadata) {
            builder << ", Lineage: {";
            for (const auto id : op->GetOutputIUs()) {
                if (const auto* entry = PlanProps.ColumnLineage.Find(id)) {
                    builder << PlanProps.InfoUnitRegistry.GetDebugName(id) << ": <ColName: " << entry->ColumnName
                        << ", Alias: " << entry->SourceAlias
                        << ", Table: " << entry->TableName
                        << ", Relation: " << entry->Relation
                        << ">, ";
                }
            }
            builder << "}";
        }
        builder << "\n";
    }

    if (printOptions & (EPrintPlanOptions::PrintBasicStatistics | EPrintPlanOptions::PrintFullStatistics) && op->Props.Statistics.has_value()) {
        builder << tabString << " ";
        builder << op->Props.Statistics->ToString(printOptions);
        builder << ", Cost: " << (op->Props.Cost.has_value() ? std::to_string(*op->Props.Cost) : "None") << "\n";
    }

    for (auto c : op->GetChildren()) {
        PlanToStringRec(c, ctx, builder, tabs + 1, printOptions);
    }
}

TOpIterator::TOpIterator(IOperator* op, TPlanProps* props, bool followSubplans, ETraversalOrder order)
    : PlanProps(props)
    , RecurseIntoSubplans(followSubplans)
    , Order(order) {
    Y_ENSURE(!followSubplans || props, "Following subplans requires plan properties");
    // One allocation covers 98.8% of measured TPCH/TPCDS traversals.
    Visited.reserve(96);
    PushFrame(op, nullptr, size_t(0), std::nullopt);
    Advance();
}

TOpIterator::TOpIterator(TOpIterator&& other)
    : Stack(std::make_move_iterator(other.Stack.begin()), std::make_move_iterator(other.Stack.end()))
    , Visited(std::move(other.Visited))
    , Current(std::move(other.Current))
    , PlanProps(other.PlanProps)
    , RecurseIntoSubplans(other.RecurseIntoSubplans)
    , Order(other.Order)
    , AtEnd(other.AtEnd) {
    other.Stack.clear();
    other.Visited.clear();
    other.Current = {};
    other.PlanProps = nullptr;
    other.RecurseIntoSubplans = false;
    other.Order = ETraversalOrder::PostOrder;
    other.AtEnd = true;
}

TOpIterator& TOpIterator::operator=(TOpIterator&& other) {
    if (this != &other) {
        Stack.assign(std::make_move_iterator(other.Stack.begin()), std::make_move_iterator(other.Stack.end()));
        Visited = std::move(other.Visited);
        Current = std::move(other.Current);
        PlanProps = other.PlanProps;
        RecurseIntoSubplans = other.RecurseIntoSubplans;
        Order = other.Order;
        AtEnd = other.AtEnd;

        other.Stack.clear();
        other.Visited.clear();
        other.Current = {};
        other.PlanProps = nullptr;
        other.RecurseIntoSubplans = false;
        other.Order = ETraversalOrder::PostOrder;
        other.AtEnd = true;
    }
    return *this;
}

const TOpIterator::TIteratorItem& TOpIterator::operator*() const {
    return Current;
}

TOpIterator& TOpIterator::operator++() {
    Advance();
    return *this;
}

void TOpIterator::operator++(int) {
    ++(*this);
}

bool TOpIterator::PushFrame(IOperator* op, IOperator* parent, size_t childIdx, std::optional<TInfoUnitId> subplanIU) {
    if (!op || !Visited.insert(op).second) {
        return false;
    }

    Stack.emplace_back(op, parent, childIdx, subplanIU);
    return true;
}

void TOpIterator::Advance() {
    while (!Stack.empty()) {
        auto& frame = Stack.back();

        if (Order == ETraversalOrder::PreOrder && !frame.Emitted) {
            frame.Emitted = true;
            Current = TIteratorItem(frame.Current, frame.Parent, frame.ChildIndex, frame.SubplanIU);
            AtEnd = false;
            return;
        }

        if (RecurseIntoSubplans && !PlanProps->Subplans.Empty()) {
            if (!frame.SubplanIUs) {
                const auto calls = frame.Current->GetSubplanIUs(PlanProps->Subplans);
                frame.SubplanIUs.emplace(calls.begin(), calls.end());
            }
            bool pushedSubplan = false;
            while (frame.NextSubplanIU < frame.SubplanIUs->size()) {
                const auto iu = (*frame.SubplanIUs)[frame.NextSubplanIU++];
                const auto* subplan = PlanProps->Subplans.Find(iu);
                if (!subplan) {
                    continue;
                }
                if (PushFrame(subplan->Plan.Get(), nullptr, size_t(0), iu)) {
                    pushedSubplan = true;
                    break;
                }
            }
            if (pushedSubplan) {
                continue;
            }
        }

        const auto& children = frame.Current->GetChildren();
        if (frame.NextChildIdx < children.size()) {
            const auto childIdx = frame.NextChildIdx++;
            PushFrame(children[childIdx], frame.Current, childIdx, frame.SubplanIU);
            continue;
        }

        if (Order == ETraversalOrder::PostOrder) {
            Current = TIteratorItem(frame.Current, frame.Parent, frame.ChildIndex, frame.SubplanIU);
            Stack.pop_back();
            AtEnd = false;
            return;
        }

        Stack.pop_back();
    }

    Current = TIteratorItem();
    AtEnd = true;
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
