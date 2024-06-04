#include "source.h"
#include "context.h"

#include <ydb/library/yql/ast/yql_ast_escaping.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/sql_types/simple_types.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/charset/ci_string.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/escape.h>
#include <util/string/subst.h>

using namespace NYql;

namespace NSQLTranslationV1 {


TTableRef::TTableRef(const TString& refName, const TString& service, const TDeferredAtom& cluster, TNodePtr keys)
    : RefName(refName)
    , Service(to_lower(service))
    , Cluster(cluster)
    , Keys(keys)
{
}

TString TTableRef::ShortName() const {
    Y_DEBUG_ABORT_UNLESS(Keys);
    if (Keys->GetTableKeys()->GetTableName()) {
        return *Keys->GetTableKeys()->GetTableName();
    }
    return TString();
}

ISource::ISource(TPosition pos)
    : INode(pos)
{
}

ISource::~ISource()
{
}

TSourcePtr ISource::CloneSource() const {
    Y_DEBUG_ABORT_UNLESS(dynamic_cast<ISource*>(Clone().Get()), "Cloned node is no source");
    TSourcePtr result = static_cast<ISource*>(Clone().Get());
    for (auto curFilter: Filters) {
        result->Filters.emplace_back(curFilter->Clone());
    }
    for (int i = 0; i < static_cast<int>(EExprSeat::Max); ++i) {
        result->NamedExprs[i] = CloneContainer(NamedExprs[i]);
    }
    result->FlattenColumns = FlattenColumns;
    result->FlattenMode = FlattenMode;
    return result;
}

bool ISource::IsFake() const {
    return false;
}

void ISource::AllColumns() {
    return;
}

const TColumns* ISource::GetColumns() const {
    return nullptr;
}

void ISource::GetInputTables(TTableList& tableList) const {
    for (auto srcPtr: UsedSources) {
        srcPtr->GetInputTables(tableList);
    }
    return;
}

TMaybe<bool> ISource::AddColumn(TContext& ctx, TColumnNode& column) {
    if (column.IsReliable()) {
        ctx.Error(Pos) << "Source does not allow column references";
        ctx.Error(column.GetPos()) << "Column reference " <<
            (column.GetColumnName() ? "'" + *column.GetColumnName() + "'" : "(expr)");
    }
    return {};
}

void ISource::FinishColumns() {
}


bool ISource::AddFilter(TContext& ctx, TNodePtr filter) {
    Y_UNUSED(ctx);
    Filters.push_back(filter);
    return true;
}

bool ISource::AddGroupKey(TContext& ctx, const TString& column) {
    if (!GroupKeys.insert(column).second) {
        ctx.Error() << "Duplicate grouping column: " << column;
        return false;
    }
    OrderedGroupKeys.push_back(column);
    return true;
}

void ISource::SetCompactGroupBy(bool compactGroupBy) {
    CompactGroupBy = compactGroupBy;
}

void ISource::SetGroupBySuffix(const TString& suffix) {
    GroupBySuffix = suffix;
}

bool ISource::AddExpressions(TContext& ctx, const TVector<TNodePtr>& expressions, EExprSeat exprSeat) {
    YQL_ENSURE(exprSeat < EExprSeat::Max);
    THashSet<TString> names;
    THashSet<TString> aliasSet;
    // TODO: merge FlattenBy with FlattenByExpr
    const bool isFlatten = (exprSeat == EExprSeat::FlattenBy || exprSeat == EExprSeat::FlattenByExpr);
    THashSet<TString>& aliases = isFlatten ? FlattenByAliases : aliasSet;
    for (const auto& expr: expressions) {
        const auto& alias = expr->GetLabel();
        const auto& columnNamePtr = expr->GetColumnName();
        if (alias) {
            ExprAliases.insert(alias);
            if (!aliases.emplace(alias).second) {
                ctx.Error(expr->GetPos()) << "Duplicate alias found: " << alias << " in " << exprSeat << " section";
                return false;
            }
            if (names.contains(alias)) {
                ctx.Error(expr->GetPos()) << "Collision between alias and column name: " << alias << " in " << exprSeat << " section";
                return false;
            }
        }
        if (columnNamePtr) {
            const auto& sourceName = *expr->GetSourceName();
            auto columnName = *columnNamePtr;
            if (sourceName) {
                columnName = DotJoin(sourceName, columnName);
            }
            if (!names.emplace(columnName).second) {
                ctx.Error(expr->GetPos()) << "Duplicate column name found: " << columnName << " in " << exprSeat << " section";
                return false;
            }
            if (!alias && aliases.contains(columnName)) {
                ctx.Error(expr->GetPos()) << "Collision between alias and column name: " << columnName << " in " << exprSeat << " section";
                return false;
            }
            if (alias && exprSeat == EExprSeat::GroupBy) {
                auto columnAlias = GroupByColumnAliases.emplace(columnName, alias);
                auto oldAlias = columnAlias.first->second;
                if (columnAlias.second && oldAlias != alias) {
                    ctx.Error(expr->GetPos()) << "Alias for column not same, column: " << columnName <<
                        ", exist alias: " << oldAlias << ", another alias: " << alias;
                    return false;
                }
            }
        }

        if (exprSeat == EExprSeat::GroupBy) {
            if (auto sessionWindow = dynamic_cast<TSessionWindow*>(expr.Get())) {
                if (SessionWindow) {
                    ctx.Error(expr->GetPos()) << "Duplicate session window specification:";
                    ctx.Error(SessionWindow->GetPos()) << "Previous session window is declared here";
                    return false;
                }
                SessionWindow = expr;
            }
            if (auto hoppingWindow = dynamic_cast<THoppingWindow*>(expr.Get())) {
                if (HoppingWindow) {
                    ctx.Error(expr->GetPos()) << "Duplicate hopping window specification:";
                    ctx.Error(HoppingWindow->GetPos()) << "Previous hopping window is declared here";
                    return false;
                }
                HoppingWindow = expr;
            }
        }
        Expressions(exprSeat).emplace_back(expr);
    }
    return true;
}

void ISource::SetFlattenByMode(const TString& mode) {
    FlattenMode = mode;
}

void ISource::MarkFlattenColumns() {
    FlattenColumns = true;
}

bool ISource::IsFlattenColumns() const {
    return FlattenColumns;
}

TString ISource::MakeLocalName(const TString& name) {
    auto iter = GenIndexes.find(name);
    if (iter == GenIndexes.end()) {
        iter = GenIndexes.emplace(name, 0).first;
    }
    TStringBuilder str;
    str << name << iter->second;
    ++iter->second;
    return std::move(str);
}

bool ISource::AddAggregation(TContext& ctx, TAggregationPtr aggr) {
    Y_UNUSED(ctx);
    YQL_ENSURE(aggr);
    Aggregations.push_back(aggr);
    return true;
}

bool ISource::HasAggregations() const {
    return !Aggregations.empty() || !GroupKeys.empty();
}

void ISource::AddWindowSpecs(TWinSpecs winSpecs) {
    WinSpecs = winSpecs;
}

bool ISource::AddFuncOverWindow(TContext& ctx, TNodePtr expr) {
    Y_UNUSED(ctx);
    Y_UNUSED(expr);
    return false;
}

void ISource::AddTmpWindowColumn(const TString& column) {
    TmpWindowColumns.push_back(column);
}

const TVector<TString>& ISource::GetTmpWindowColumns() const {
    return TmpWindowColumns;
}

void ISource::SetLegacyHoppingWindowSpec(TLegacyHoppingWindowSpecPtr spec) {
    LegacyHoppingWindowSpec = spec;
}

TLegacyHoppingWindowSpecPtr ISource::GetLegacyHoppingWindowSpec() const {
    return LegacyHoppingWindowSpec;
}

TNodePtr ISource::GetSessionWindowSpec() const {
    return SessionWindow;
}

TNodePtr ISource::GetHoppingWindowSpec() const {
    return HoppingWindow;
}

TWindowSpecificationPtr ISource::FindWindowSpecification(TContext& ctx, const TString& windowName) const {
    auto winIter = WinSpecs.find(windowName);
    if (winIter == WinSpecs.end()) {
        ctx.Error(Pos) << "Unable to find window specification for window '" << windowName << "'";
        return {};
    }
    YQL_ENSURE(winIter->second);
    return winIter->second;
}

inline TVector<TNodePtr>& ISource::Expressions(EExprSeat exprSeat) {
    return NamedExprs[static_cast<size_t>(exprSeat)];
}

const TVector<TNodePtr>& ISource::Expressions(EExprSeat exprSeat) const {
    return NamedExprs[static_cast<size_t>(exprSeat)];
}

inline TNodePtr ISource::AliasOrColumn(const TNodePtr& node, bool withSource) {
    auto result = node->GetLabel();
    if (!result) {
        const auto columnNamePtr = node->GetColumnName();
        YQL_ENSURE(columnNamePtr);
        result = *columnNamePtr;
        if (withSource) {
            const auto sourceNamePtr = node->GetSourceName();
            if (sourceNamePtr) {
                result = DotJoin(*sourceNamePtr, result);
            }
        }
    }
    return BuildQuotedAtom(node->GetPos(), result);
}

bool ISource::AddAggregationOverWindow(TContext& ctx, const TString& windowName, TAggregationPtr func) {
    YQL_ENSURE(func->IsOverWindow());
    if (func->IsDistinct()) {
        ctx.Error(func->GetPos()) << "Aggregation with distinct is not allowed over window: " << windowName;
        return false;
    }
    if (!FindWindowSpecification(ctx, windowName)) {
        return false;
    }
    AggregationOverWindow[windowName].emplace_back(std::move(func));
    return true;
}

bool ISource::AddFuncOverWindow(TContext& ctx, const TString& windowName, TNodePtr func) {
    if (!FindWindowSpecification(ctx, windowName)) {
        return false;
    }
    FuncOverWindow[windowName].emplace_back(std::move(func));
    return true;
}

void ISource::SetMatchRecognize(TMatchRecognizeBuilderPtr matchRecognize) {
    MatchRecognizeBuilder = matchRecognize;
}

bool ISource::IsCompositeSource() const {
    return false;
}

bool ISource::IsGroupByColumn(const TString& column) const {
    return GroupKeys.contains(column);
}

bool ISource::IsFlattenByColumns() const {
    return !Expressions(EExprSeat::FlattenBy).empty();
}

bool ISource::IsFlattenByExprs() const {
    return !Expressions(EExprSeat::FlattenByExpr).empty();
}

bool ISource::IsAlias(EExprSeat exprSeat, const TString& column) const {
    for (const auto& exprNode: Expressions(exprSeat)) {
        const auto& labelName = exprNode->GetLabel();
        if (labelName && labelName == column) {
            return true;
        }
    }
    return false;
}

bool ISource::IsExprAlias(const TString& column) const {
    std::array<EExprSeat, 5> exprSeats = {{EExprSeat::FlattenBy, EExprSeat::FlattenByExpr, EExprSeat::GroupBy,
                                           EExprSeat::WindowPartitionBy, EExprSeat::DistinctAggr}};
    for (auto seat: exprSeats) {
        if (IsAlias(seat, column)) {
            return true;
        }
    }
    return false;
}

bool ISource::IsExprSeat(EExprSeat exprSeat, EExprType type) const {
    auto expressions = Expressions(exprSeat);
    if (!expressions) {
        return false;
    }
    for (const auto& exprNode: expressions) {
        if (exprNode->GetLabel()) {
            return type == EExprType::WithExpression;
        }
    }
    return type == EExprType::ColumnOnly;
}

TString ISource::GetGroupByColumnAlias(const TString& column) const {
    auto iter = GroupByColumnAliases.find(column);
    if (iter == GroupByColumnAliases.end()) {
        return {};
    }
    return iter->second;
}

const TString* ISource::GetWindowName() const {
    return {};
}

bool ISource::IsCalcOverWindow() const {
    return !AggregationOverWindow.empty() || !FuncOverWindow.empty() ||
        AnyOf(WinSpecs, [](const auto& item) { return item.second->Session; });
}

bool ISource::IsOverWindowSource() const {
    return !WinSpecs.empty();
}

bool ISource::IsStream() const {
    return false;
}

EOrderKind ISource::GetOrderKind() const {
    return EOrderKind::None;
}

TWriteSettings ISource::GetWriteSettings() const {
    return {};
}

TNodePtr ISource::PrepareSamplingRate(TPosition pos, ESampleClause clause, TNodePtr samplingRate) {
    if (ESampleClause::Sample == clause) {
        samplingRate = Y("*", samplingRate, Y("Double", Q("100")));
    }
    auto ensureLow  = Y("Ensure", "samplingRate", Y(">=", "samplingRate", Y("Double", Q("0"))), Y("String", BuildQuotedAtom(pos, "Expected sampling rate to be nonnegative")));
    auto ensureHigh = Y("Ensure", "samplingRate", Y("<=", "samplingRate", Y("Double", Q("100"))), Y("String", BuildQuotedAtom(pos, "Sampling rate is over 100%")));

    auto block(Y(Y("let", "samplingRate", samplingRate)));
    block = L(block, Y("let", "samplingRate", ensureLow));
    block = L(block, Y("let", "samplingRate", ensureHigh));
    samplingRate = Y("block", Q(L(block, Y("return", "samplingRate"))));
    return samplingRate;
}


bool ISource::SetSamplingOptions(TContext& ctx,
                                 TPosition pos,
                                 ESampleClause sampleClause,
                                 ESampleMode mode,
                                 TNodePtr samplingRate,
                                 TNodePtr samplingSeed) {
    Y_UNUSED(pos);
    Y_UNUSED(sampleClause);
    Y_UNUSED(mode);
    Y_UNUSED(samplingRate);
    Y_UNUSED(samplingSeed);
    ctx.Error() << "Sampling is only supported for table sources";
    return false;
}

bool ISource::SetTableHints(TContext& ctx, TPosition pos, const TTableHints& hints, const TTableHints& contextHints) {
    Y_UNUSED(pos);
    Y_UNUSED(contextHints);
    if (hints) {
        ctx.Error() << "Explicit hints are only supported for table sources";
        return false;
    }
    return true;
}

bool ISource::AddGrouping(TContext& ctx, const TVector<TString>& columns, TString& grouingColumn) {
    Y_UNUSED(columns);
    Y_UNUSED(grouingColumn);
    ctx.Error() << "Source not support grouping hint";
    return false;
}

size_t ISource::GetGroupingColumnsCount() const {
    return 0;
}

TNodePtr ISource::BuildFilter(TContext& ctx, const TString& label) {
    return Filters.empty() ? nullptr : Y(ctx.UseUnordered(*this) ? "OrderedFilter" : "Filter", label, BuildFilterLambda());
}

TNodePtr ISource::BuildFilterLambda() {
    if (Filters.empty()) {
        return BuildLambda(Pos, Y("row"), Y("Bool", Q("true")));
    }
    YQL_ENSURE(Filters[0]->HasState(ENodeState::Initialized));
    TNodePtr filter(Filters[0]);
    for (ui32 i = 1; i < Filters.size(); ++i) {
        YQL_ENSURE(Filters[i]->HasState(ENodeState::Initialized));
        filter = Y("And", filter, Filters[i]);
    }
    filter = Y("Coalesce", filter, Y("Bool", Q("false")));
    return BuildLambda(Pos, Y("row"), filter);
}

TNodePtr ISource::BuildFlattenByColumns(const TString& label) {
    auto columnsList = Y("FlattenByColumns", Q(FlattenMode), label);
    for (const auto& column: Expressions(EExprSeat::FlattenBy)) {
        const auto columnNamePtr = column->GetColumnName();
        YQL_ENSURE(columnNamePtr);
        if (column->GetLabel().empty()) {
            columnsList = L(columnsList, Q(*columnNamePtr));
        } else {
            columnsList = L(columnsList, Q(Y(Q(*columnNamePtr), Q(column->GetLabel()))));
        }
    }
    return Y(Y("let", "res", columnsList));
}

TNodePtr ISource::BuildFlattenColumns(const TString& label) {
    return Y(Y("let", "res", Y("Just", Y("FlattenStructs", label))));
}

namespace {

TNodePtr BuildLambdaBodyForExprAliases(TPosition pos, const TVector<TNodePtr>& exprs) {
    auto structObj = BuildAtom(pos, "row", TNodeFlags::Default);
    for (const auto& exprNode: exprs) {
        const auto name = exprNode->GetLabel();
        YQL_ENSURE(name);
        structObj = structObj->Y("ForceRemoveMember", structObj, structObj->Q(name));
        if (dynamic_cast<const TSessionWindow*>(exprNode.Get())) {
            continue;
        }
        if (dynamic_cast<const THoppingWindow*>(exprNode.Get())) {
            continue;
        }
        structObj = structObj->Y("AddMember", structObj, structObj->Q(name), exprNode);
    }
    return structObj->Y("AsList", structObj);
}

}

TNodePtr ISource::BuildPreaggregatedMap(TContext& ctx) {
    Y_UNUSED(ctx);
    const auto& groupByExprs = Expressions(EExprSeat::GroupBy);
    const auto& distinctAggrExprs = Expressions(EExprSeat::DistinctAggr);
    YQL_ENSURE(groupByExprs || distinctAggrExprs);

    TNodePtr res;
    if (groupByExprs) {
        auto body = BuildLambdaBodyForExprAliases(Pos, groupByExprs);
        res = Y("FlatMap", "core", BuildLambda(Pos, Y("row"), body));
    }

    if (distinctAggrExprs) {
        auto body = BuildLambdaBodyForExprAliases(Pos, distinctAggrExprs);
        auto lambda = BuildLambda(Pos, Y("row"), body);
        res = res ? Y("FlatMap", res,  lambda) : Y("FlatMap", "core", lambda);
    }
    return res;
}

TNodePtr ISource::BuildPreFlattenMap(TContext& ctx) {
    Y_UNUSED(ctx);
    YQL_ENSURE(IsFlattenByExprs());
    return BuildLambdaBodyForExprAliases(Pos, Expressions(EExprSeat::FlattenByExpr));
}

TNodePtr ISource::BuildPrewindowMap(TContext& ctx) {
    auto feed = BuildAtom(Pos, "row", TNodeFlags::Default);
    for (const auto& exprNode: Expressions(EExprSeat::WindowPartitionBy)) {
        const auto name = exprNode->GetLabel();
        if (name && !dynamic_cast<const TSessionWindow*>(exprNode.Get())) {
            feed = Y("AddMember", feed, Q(name), exprNode);
        }
    }
    return Y(ctx.UseUnordered(*this) ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos, Y("row"), Y("AsList", feed)));
}

bool ISource::BuildSamplingLambda(TNodePtr& node) {
    if (!SamplingRate) {
        return true;
    }
    auto res = Y("Coalesce", Y("SafeCast", SamplingRate, Y("DataType", Q("Double"))), Y("Double", Q("0")));
    res = Y("/", res, Y("Double", Q("100")));
    res = Y(Y("let", "res", Y("OptionalIf", Y("<", Y("Random", Y("DependsOn", "row")), res), "row")));
    node = BuildLambda(GetPos(), Y("row"), res, "res");
    return !!node;
}

bool ISource::SetSamplingRate(TContext& ctx, ESampleClause clause, TNodePtr samplingRate) {
    if (samplingRate) {
        if (!samplingRate->Init(ctx, this)) {
            return false;
        }
        SamplingRate = PrepareSamplingRate(Pos, clause, samplingRate);
    }
    return true;
}

std::pair<TNodePtr, bool> ISource::BuildAggregation(const TString& label, TContext& ctx) {
    if (GroupKeys.empty() && Aggregations.empty() && !IsCompositeSource() && !LegacyHoppingWindowSpec) {
        return { nullptr, true };
    }

    auto keysTuple = Y();
    YQL_ENSURE(GroupKeys.size() == OrderedGroupKeys.size());
    for (const auto& key: OrderedGroupKeys) {
        YQL_ENSURE(GroupKeys.contains(key));
        keysTuple = L(keysTuple, BuildQuotedAtom(Pos, key));
    }

    std::map<std::pair<bool, TString>, std::vector<IAggregation*>> genericAggrs;
    for (const auto& aggr: Aggregations) {
        if (const auto key = aggr->GetGenericKey()) {
            genericAggrs[{aggr->IsDistinct(), *key}].emplace_back(aggr.Get());
        }
    }

    for (const auto& aggr : genericAggrs) {
        for (size_t i = 1U; i < aggr.second.size(); ++i) {
            aggr.second.front()->Join(aggr.second[i]);
        }
    }

    const auto listType = Y("TypeOf", label);
    auto aggrArgs = Y();
    const bool overState = GroupBySuffix == "CombineState" || GroupBySuffix == "MergeState" ||
        GroupBySuffix == "MergeFinalize" || GroupBySuffix == "MergeManyFinalize";
    const bool allowAggApply = !LegacyHoppingWindowSpec && !SessionWindow && !HoppingWindow;
    for (const auto& aggr: Aggregations) {
        auto res = aggr->AggregationTraits(listType, overState, GroupBySuffix == "MergeManyFinalize", allowAggApply, ctx);
        if (!res.second) {
           return { nullptr, false };
        }

        if (res.first) {
            aggrArgs = L(aggrArgs, res.first);
        }
    }

    auto options = Y();
    if (CompactGroupBy || GroupBySuffix == "Finalize") {
        options = L(options, Q(Y(Q("compact"))));
    }

    if (LegacyHoppingWindowSpec) {
        auto hoppingTraits = Y(
            "HoppingTraits",
            Y("ListItemType", listType),
            BuildLambda(Pos, Y("row"), LegacyHoppingWindowSpec->TimeExtractor),
            LegacyHoppingWindowSpec->Hop,
            LegacyHoppingWindowSpec->Interval,
            LegacyHoppingWindowSpec->Delay,
            LegacyHoppingWindowSpec->DataWatermarks ? Q("true") : Q("false"),
            Q("v1"));

        options = L(options, Q(Y(Q("hopping"), hoppingTraits)));
    }

    if (SessionWindow) {
        YQL_ENSURE(SessionWindow->GetLabel());
        auto sessionWindow = dynamic_cast<TSessionWindow*>(SessionWindow.Get());
        YQL_ENSURE(sessionWindow);
        options = L(options, Q(Y(Q("session"),
            Q(Y(BuildQuotedAtom(Pos, SessionWindow->GetLabel()), sessionWindow->BuildTraits(label))))));
    }

    if (HoppingWindow) {
        YQL_ENSURE(HoppingWindow->GetLabel());
        auto hoppingWindow = dynamic_cast<THoppingWindow*>(HoppingWindow.Get());
        YQL_ENSURE(hoppingWindow);
        options = L(options, Q(Y(Q("hopping"),
            Q(Y(BuildQuotedAtom(Pos, HoppingWindow->GetLabel()), hoppingWindow->BuildTraits(label))))));
    }

    return { Y("AssumeColumnOrderPartial", Y("Aggregate" + GroupBySuffix, label, Q(keysTuple), Q(aggrArgs), Q(options)), Q(keysTuple)), true };
}

TMaybe<TString> ISource::FindColumnMistype(const TString& name) const {
    auto result = FindMistypeIn(GroupKeys, name);
    return result ? result : FindMistypeIn(ExprAliases, name);
}

void ISource::AddDependentSource(ISource* usedSource) {
    UsedSources.push_back(usedSource);
}

class TYqlFrameBound final: public TCallNode {
public:
    TYqlFrameBound(TPosition pos, TNodePtr bound)
        : TCallNode(pos, "EvaluateExpr", 1, 1, { bound })
        , FakeSource(BuildFakeSource(pos))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args[0]->Init(ctx, FakeSource.Get())) {
            return false;
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TYqlFrameBound(Pos, Args[0]->Clone());
    }
private:
    TSourcePtr FakeSource;
};

TNodePtr BuildFrameNode(const TFrameBound& frame, EFrameType frameType) {
    TString settingStr;
    switch (frame.Settings) {
        case FramePreceding: settingStr = "preceding"; break;
        case FrameCurrentRow: settingStr = "currentRow"; break;
        case FrameFollowing: settingStr = "following"; break;
        default: YQL_ENSURE(false, "Unexpected frame setting");
    }

    TNodePtr node = frame.Bound;
    TPosition pos = frame.Pos;
    if (frameType != EFrameType::FrameByRows) {
        TVector<TNodePtr> settings;
        settings.push_back(BuildQuotedAtom(pos, settingStr, TNodeFlags::Default));
        if (frame.Settings != FrameCurrentRow) {
            if (!node) {
                node = BuildQuotedAtom(pos, "unbounded", TNodeFlags::Default);
            } else if (!node->IsLiteral()) {
                node = new TYqlFrameBound(pos, node);
            }
            settings.push_back(std::move(node));
        }
        return BuildTuple(pos, std::move(settings));
    }

    // TODO: switch FrameByRows to common format above
    YQL_ENSURE(frame.Settings != FrameCurrentRow, "Should be already replaced by 0 preceding/following");
    if (!node) {
        node = BuildLiteralVoid(pos);
    } else if (node->IsLiteral()) {
        YQL_ENSURE(node->GetLiteralType() == "Int32");
        i32 value = FromString<i32>(node->GetLiteralValue());
        YQL_ENSURE(value >= 0);
        if (frame.Settings == FramePreceding) {
            value = -value;
        }
        node = new TCallNodeImpl(pos, "Int32", { BuildQuotedAtom(pos, ToString(value), TNodeFlags::Default) });
    } else {
        if (frame.Settings == FramePreceding) {
            node = new TCallNodeImpl(pos, "Minus", { node->Clone() });
        }
        node = new TYqlFrameBound(pos, node);
    }
    return node;
}

TNodePtr ISource::BuildWindowFrame(const TFrameSpecification& spec, bool isCompact) {
    YQL_ENSURE(spec.FrameExclusion == FrameExclNone);
    YQL_ENSURE(spec.FrameBegin);
    YQL_ENSURE(spec.FrameEnd);

    auto frameBeginNode = BuildFrameNode(*spec.FrameBegin, spec.FrameType);
    auto frameEndNode = BuildFrameNode(*spec.FrameEnd, spec.FrameType);

    auto begin = Q(Y(Q("begin"), frameBeginNode));
    auto end = Q(Y(Q("end"), frameEndNode));

    return isCompact ? Q(Y(begin, end, Q(Y(Q("compact"))))) : Q(Y(begin, end));
}

class TSessionWindowTraits final: public TCallNode {
public:
    TSessionWindowTraits(TPosition pos, const TVector<TNodePtr>& args)
        : TCallNode(pos, "SessionWindowTraits", args)
        , FakeSource(BuildFakeSource(pos))
    {
        YQL_ENSURE(args.size() == 4);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!ValidateArguments(ctx)) {
            return false;
        }

        if (!Args.back()->Init(ctx, FakeSource.Get())) {
            return false;
        }

        return TCallNode::DoInit(ctx, src);
    }

    TNodePtr DoClone() const final {
        return new TSessionWindowTraits(Pos, CloneContainer(Args));
    }
private:
    TSourcePtr FakeSource;
};

TNodePtr ISource::BuildCalcOverWindow(TContext& ctx, const TString& label) {
    YQL_ENSURE(IsCalcOverWindow());

    TSet<TString> usedWindows;
    for (auto& it : AggregationOverWindow) {
        usedWindows.insert(it.first);
    }
    for (auto& it : FuncOverWindow) {
        usedWindows.insert(it.first);
    }
    for (auto& it : WinSpecs) {
        if (it.second->Session) {
            usedWindows.insert(it.first);
        }
    }

    YQL_ENSURE(!usedWindows.empty());

    const bool onePartition = usedWindows.size() == 1;
    const auto useLabel = onePartition ? label : "partitioning";
    const auto listType = Y("TypeOf", useLabel);
    auto framesProcess = Y();
    auto resultNode = onePartition ? Y() : Y(Y("let", "partitioning", label));

    for (const auto& name : usedWindows) {
        auto spec = FindWindowSpecification(ctx, name);
        YQL_ENSURE(spec);

        auto aggsIter = AggregationOverWindow.find(name);
        auto funcsIter = FuncOverWindow.find(name);

        const auto& aggs = (aggsIter == AggregationOverWindow.end()) ? TVector<TAggregationPtr>() : aggsIter->second;
        const auto& funcs = (funcsIter == FuncOverWindow.end()) ? TVector<TNodePtr>() : funcsIter->second;

        auto frames = Y();
        TString frameType;
        switch (spec->Frame->FrameType) {
            case EFrameType::FrameByRows: frameType = "WinOnRows"; break;
            case EFrameType::FrameByRange: frameType = "WinOnRange"; break;
            case EFrameType::FrameByGroups: frameType = "WinOnGroups"; break;
        }
        YQL_ENSURE(frameType);
        auto callOnFrame = Y(frameType, BuildWindowFrame(*spec->Frame, spec->IsCompact));
        for (auto& agg : aggs) {
            auto winTraits = agg->WindowTraits(listType, ctx);
            callOnFrame = L(callOnFrame, winTraits);
        }
        for (auto& func : funcs) {
            auto winSpec = func->WindowSpecFunc(listType);
            callOnFrame = L(callOnFrame, winSpec);
        }
        frames = L(frames, callOnFrame);

        auto keysTuple = Y();
        for (const auto& key: spec->Partitions) {
            if (!dynamic_cast<TSessionWindow*>(key.Get())) {
                keysTuple = L(keysTuple, AliasOrColumn(key, GetJoin()));
            }
        }

        auto sortSpec = spec->OrderBy.empty() ? Y("Void") : BuildSortSpec(spec->OrderBy, useLabel, true, false);
        if (spec->Session) {
            TString label = spec->Session->GetLabel();
            YQL_ENSURE(label);
            auto sessionWindow = dynamic_cast<TSessionWindow*>(spec->Session.Get());
            YQL_ENSURE(sessionWindow);
            auto labelNode = BuildQuotedAtom(sessionWindow->GetPos(), label);

            auto sessionTraits = sessionWindow->BuildTraits(useLabel);
            framesProcess = Y("CalcOverSessionWindow", useLabel, Q(keysTuple), sortSpec, Q(frames), sessionTraits, Q(Y(labelNode)));
        } else {
            YQL_ENSURE(aggs || funcs);
            framesProcess = Y("CalcOverWindow", useLabel, Q(keysTuple), sortSpec, Q(frames));
        }

        if (!onePartition) {
            resultNode = L(resultNode, Y("let", "partitioning", framesProcess));
        }
    }
    if (onePartition) {
        return framesProcess;
    } else {
        return Y("block", Q(L(resultNode, Y("return", "partitioning"))));
    }
}

TNodePtr ISource::BuildSort(TContext& ctx, const TString& label) {
    Y_UNUSED(ctx);
    Y_UNUSED(label);
    return nullptr;
}

TNodePtr ISource::BuildCleanupColumns(TContext& ctx, const TString& label) {
    Y_UNUSED(ctx);
    Y_UNUSED(label);
    return nullptr;
}

TNodePtr ISource::BuildGroupingColumns(const TString& label) {
    Y_UNUSED(label);
    return nullptr;
}

IJoin* ISource::GetJoin() {
    return nullptr;
}

ISource* ISource::GetCompositeSource() {
    return nullptr;
}

bool ISource::IsSelect() const {
    return true;
}

bool ISource::IsTableSource() const {
    return false;
}

bool ISource::ShouldUseSourceAsColumn(const TString& source) const {
    Y_UNUSED(source);
    return false;
}

bool ISource::IsJoinKeysInitializing() const {
    return false;
}

bool ISource::DoInit(TContext& ctx, ISource* src) {
    for (auto& column: Expressions(EExprSeat::FlattenBy)) {
        if (!column->Init(ctx, this)) {
            return false;
        }
    }

    if (IsFlattenColumns() && src) {
        src->AllColumns();
    }

    return true;
}

bool ISource::InitFilters(TContext& ctx) {
    for (auto& filter: Filters) {
        if (!filter->Init(ctx, this)) {
            return false;
        }
        if (filter->IsAggregated() && !filter->IsConstant() && !filter->HasState(ENodeState::AggregationKey)) {
            ctx.Error(filter->GetPos()) << "Can not use aggregated values in filtering";
            return false;
        }
    }
    return true;
}

TAstNode* ISource::Translate(TContext& ctx) const {
    Y_DEBUG_ABORT_UNLESS(false);
    Y_UNUSED(ctx);
    return nullptr;
}

void ISource::FillSortParts(const TVector<TSortSpecificationPtr>& orderBy, TNodePtr& sortDirection, TNodePtr& sortKeySelector) {
    TNodePtr expr;
    if (orderBy.empty()) {
        YQL_ENSURE(!sortKeySelector);
        sortDirection = sortKeySelector = Y("Void");
        return;
    } else if (orderBy.size() == 1) {
        auto& sortSpec = orderBy.front();
        expr = Y("PersistableRepr", sortSpec->OrderExpr);
        sortDirection = Y("Bool", Q(sortSpec->Ascending ? "true" : "false"));
    } else {
        auto exprList = Y();
        sortDirection = Y();
        for (const auto& sortSpec: orderBy) {
            const auto asc = sortSpec->Ascending;
            sortDirection = L(sortDirection, Y("Bool", Q(asc ? "true" : "false")));
            exprList = L(exprList, Y("PersistableRepr", sortSpec->OrderExpr));
        }
        sortDirection = Q(sortDirection);
        expr = Q(exprList);
    }
    sortKeySelector = BuildLambda(Pos, Y("row"), expr);
}

TNodePtr ISource::BuildSortSpec(const TVector<TSortSpecificationPtr>& orderBy, const TString& label, bool traits, bool assume) {
    YQL_ENSURE(!orderBy.empty());
    TNodePtr dirsNode;
    TNodePtr keySelectorNode;
    FillSortParts(orderBy, dirsNode, keySelectorNode);
    if (traits) {
        return Y("SortTraits", Y("TypeOf", label), dirsNode, keySelectorNode);
    } else if (assume) {
        return Y("AssumeSorted", label, dirsNode, keySelectorNode);
    } else {
        return Y("Sort", label, dirsNode, keySelectorNode);
    }
}

bool ISource::HasMatchRecognize() const {
    return static_cast<bool>(MatchRecognizeBuilder);
}

TNodePtr ISource::BuildMatchRecognize(TContext& ctx, TString&& inputTable){
    YQL_ENSURE(HasMatchRecognize());
    return MatchRecognizeBuilder->Build(ctx, std::move(inputTable), this);
};

IJoin::IJoin(TPosition pos)
    : ISource(pos)
{
}

IJoin::~IJoin()
{
}

IJoin* IJoin::GetJoin() {
    return this;
}

} // namespace NSQLTranslationV1
