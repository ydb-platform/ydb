#include "kqp_type_annotation.h"
#include "kqp_new_parser.h"
#include "kqp_ast_visit.h"

#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_optimize.h>

#include <util/generic/scope.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NKqp::NNewYql {
namespace {

using namespace NYql;
using TNode = TExprNode::TPtr;
using TType = const TTypeAnnotationNode*;

struct TAnnotationError {};
// The enclosing SQL clause imposes requirements beyond the scalar return type:
// predicates must be Bool, sort keys comparable, and counts coercible to Uint64.
enum class EUse { Value, Predicate, SortKey, Limit, Offset };

class TAnnotator {
public:
    TAnnotator(const TScript& script, const TResolvedNames& names, TExprContext& ctx,
        TTypeAnnotationContext& types, const TAstTypeAnnotationSettings& settings)
        : Script(script), Names(names), Ctx(ctx), Types(types), Settings(settings)
    {
        Result.Columns.resize(names.Columns.size());
        for (TColumnId id = 0; id < names.Columns.size(); ++id) {
            if (names.Columns[id].Source) { SourceColumns[names.Columns[id].Source].push_back(id); }
        }
    }

    std::unique_ptr<TAnnotatedAst> Run() {
        for (const auto& statement : Script.Statements) {
            if (const auto* query = std::get_if<TQueryStatement>(&statement.Value)) {
                Query(*query->Query);
            } else if (const auto* binding = std::get_if<TNamedBinding>(&statement.Value)) {
                // Scalar definitions (especially polymorphic lambdas) acquire
                // types at their uses. Query producers must establish column
                // types before any named-source consumer follows their IDs.
                if (const auto* query = Names.NamedQueries.FindPtr(&binding->Name)) { Query(**query); }
            } else {
                const auto& pragma = std::get<TPragmaStatement>(statement.Value);
                // Enabling comma joins affects syntax acceptance, not scalar
                // types; their Cross nodes use the ordinary join typing below.
                if (!pragma.Namespace && AsciiEqualsIgnoreCase(pragma.Name.Spelling, "AnsiImplicitCrossJoin")
                    && pragma.ValueForm == EPragmaValueForm::None) {
                    continue;
                }
                if (pragma.Namespace || (!AsciiEqualsIgnoreCase(pragma.Name.Spelling, "TablePathPrefix")
                    && !AsciiEqualsIgnoreCase(pragma.Name.Spelling, "YqlSelect")
                    && !AsciiEqualsIgnoreCase(pragma.Name.Spelling, "YqlSelectAllowUnnamedGroupByExpr"))) {
                    Unsupported(statement.Range, "this pragma's type semantics");
                }
            }
        }
        // Include aliases not referenced by a subsequent query (e.g. CTE
        // column lists); their input types have already been established.
        for (TColumnId id = 0; id < Names.Columns.size(); ++id) { Column(id); }
        return std::make_unique<TAnnotatedAst>(std::move(Result));
    }

private:
    TPositionHandle Pos(const TSourceRange& range) {
        return Ctx.AppendPosition(TPosition(range.Column + 1, range.Line, Script.File));
    }

    [[noreturn]] void Unsupported(const TSourceRange& range, TStringBuf feature) const {
        ythrow TNewYqlParserFallback() << "New YQL type annotation does not support " << feature
            << " at " << Script.File << ":" << range.Line << ":" << range.Column + 1;
    }

    [[noreturn]] void Error(const TSourceRange& range, TStringBuf message) {
        Ctx.AddError(TIssue(Ctx.GetPosition(Pos(range)), TStringBuilder() << "New YQL type annotation: " << message));
        throw TAnnotationError{};
    }

    TNode Call(TPositionHandle pos, TStringBuf name, TExprNode::TListType args) {
        return Ctx.NewCallable(pos, name, std::move(args));
    }

    TType Optional(TType type) {
        // YQL outer joins do not nest an existing optional. Pg values are
        // intrinsically nullable and do not use Optional wrappers.
        return type->GetKind() == ETypeAnnotationKind::Optional || type->GetKind() == ETypeAnnotationKind::Pg
            || type->GetKind() == ETypeAnnotationKind::Null ? type : Ctx.MakeType<TOptionalExprType>(type);
    }

    // Memoize stored-column and alias types by resolved identity. Projection
    // types are supplied by Query in dependency order; recursively following
    // Input handles renames without looking up ambiguous SQL names again.
    TType Column(TColumnId id) {
        Y_ENSURE(id < Names.Columns.size());
        if (Result.Columns[id]) { return Result.Columns[id]; }
        const auto& column = Names.Columns[id];
        TType type = nullptr;
        if (column.Input) {
            type = Column(*column.Input);
        } else if (column.Table && column.MetadataColumnId) {
            const auto it = std::find_if(column.Table->Columns.begin(), column.Table->Columns.end(),
                [&](const auto& metadata) { return metadata.Id == *column.MetadataColumnId; });
            Y_ENSURE(it != column.Table->Columns.end());
            const auto pos = Pos(column.Source->Range);
            auto parsedType = Call(pos, "ParseType", {Ctx.NewAtom(pos, it->Type)});
            Annotate(parsedType, column.Source->Range);
            type = parsedType->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!it->NotNull) { type = Optional(type); }
        } else {
            // Projection expression types must be established by Query before
            // a derived table or ORDER BY consumes their column IDs.
            Y_ENSURE(false, "Projection type is not available for column " << id);
        }
        Result.Columns[id] = type;
        return type;
    }

    // Each scalar lambda is a fresh annotation job. Isolate its repeat counter
    // from the surrounding host compilation, but preserve the caller's completed
    // transform stages. The transformer may replace root as it inserts coercions.
    void Annotate(TNode& root, const TSourceRange& range) {
        const auto repeats = Ctx.RepeatTransformCounter;
        const auto steps = Ctx.Step;
        Y_DEFER { Ctx.RepeatTransformCounter = repeats; Ctx.Step = steps; };
        Ctx.RepeatTransformCounter = 0;
        TIssueScopeGuard issue(Ctx.IssueManager, [&] {
            return MakeIntrusive<TIssue>(Ctx.GetPosition(Pos(range)), "New YQL type annotation");
        });
        // The host may have expanded its legacy graph already. This is a fresh
        // scalar graph, so run the existing lambda-application expansion locally
        // and restore the caller's stage state on both success and error.
        std::vector<TType> argumentTypes;
        if (root->IsLambda()) {
            for (const auto& argument : root->Head().Children()) { argumentTypes.push_back(argument->GetTypeAnn()); }
        }
        Ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
        if (ExpandApplyNoRepeat(root, root, Ctx, Types) != IGraphTransformer::TStatus::Ok) {
            throw TAnnotationError{};
        }
        // Expansion rebuilds enclosing lambdas and can clear their argument
        // annotations. Restore the relational scope on the rewritten root.
        if (root->IsLambda()) { UpdateLambdaAllArgumentsTypes(root, argumentTypes, Ctx); }
        auto transformer = CreateTypeAnnotationTransformer(CreateExtCallableTypeAnnotationTransformer(Types, true), Types);
        if (InstantTransform(*transformer, root, Ctx) != IGraphTransformer::TStatus::Ok) {
            throw TAnnotationError{};
        }
    }

    // Wrap the lowered body in a closed, typed lambda before calling the YQL
    // annotator. Supplying all visible columns gives lowering a stable contract
    // even when a particular expression uses only a subset of the scope.
    TType Expression(const TExpression& expression, TColumnTypes scope, EUse use = EUse::Value) {
        PrepareSubqueries(expression, scope);
        VisitScalar(expression, [&](const auto& node) {
            if (const auto* id = Names.ColumnReferences.FindPtr(&node); id && Outer.contains(*id)) {
                Result.Captures[CurrentQuery][*id] = Outer.at(*id);
            }
            return !Result.Values.contains(&node);
        });
        const auto pos = Pos(expression.Range);
        TExpressionBindings bindings;
        bindings.Names = &Names;
        TAnnotatedExpression annotated;
        // Stable argument order also makes the resulting lambdas convenient
        // for relational lowering. Each lambda owns its argument nodes.
        for (const auto& [id, type] : scope) { Y_UNUSED(type); annotated.Inputs.push_back(id); }
        std::sort(annotated.Inputs.begin(), annotated.Inputs.end());
        TExprNode::TListType arguments;
        for (const auto id : annotated.Inputs) {
            auto argument = Ctx.NewArgument(pos, TStringBuilder() << "column" << id);
            argument->SetTypeAnn(scope.at(id));
            bindings.Columns[id] = argument;
            arguments.push_back(std::move(argument));
        }
        for (const auto& [node, id] : Result.Values) {
            if (const auto* argument = bindings.Columns.FindPtr(id)) {
                auto value = *argument;
                if (const auto* in = std::get_if<TIn>(&node->Value); in && in->Negated) { value = Call(pos, "Not", {value}); }
                if (const auto* call = std::get_if<TCall>(&node->Value); call && AggregateName(*node) == "variance_1_1") {
                    value = Call(pos, "SqlCall", {Ctx.NewAtom(pos, "Math.Sqrt"), Ctx.NewList(pos, {Call(pos, "PositionalArgs", {value})})});
                }
                bindings.Expressions[node] = std::move(value);
            }
        }
        for (const auto& [name, type] : Settings.Parameters) {
            bindings.Parameters[name] = Call(pos, "Parameter", {Ctx.NewAtom(pos, name), ExpandType(pos, *type, Ctx)});
        }
        TNode body;
        if (const auto* star = Names.Stars.FindPtr(&expression)) {
            TExprNode::TListType items;
            for (const auto id : *star) { items.push_back(bindings.Columns.at(id)); }
            body = Ctx.NewList(pos, std::move(items));
        } else {
            body = LowerExpression(expression, Script, bindings, Ctx, Settings.Expressions);
            if (!body) { throw TAnnotationError{}; }
        }
        if (use == EUse::Predicate) {
            body = Call(pos, "Coalesce", {body, Call(pos, "Bool", {Ctx.NewAtom(pos, "false")})});
        } else if (use == EUse::Limit || use == EUse::Offset) {
            // Match the SQL translator's defaults, then let Take's existing
            // type rule validate and coerce the count to Uint64.
            body = Call(pos, "Coalesce", {body, Call(pos, "Uint64", {
                Ctx.NewAtom(pos, use == EUse::Limit ? "18446744073709551615" : "0")})});
            body = Call(pos, "Take", {Call(pos, "AsList", {Call(pos, "Bool", {Ctx.NewAtom(pos, "true")})}), body});
        }
        annotated.Lambda = Ctx.NewLambda(pos, Ctx.NewArguments(pos, std::move(arguments)), std::move(body));
        std::vector<TType> argumentTypes;
        for (const auto id : annotated.Inputs) { argumentTypes.push_back(scope.at(id)); }
        // The YQL annotator requires the Arguments node to be initialized even
        // for a zero-argument lambda (constants and LIMIT/OFFSET expressions).
        UpdateLambdaAllArgumentsTypes(annotated.Lambda, argumentTypes, Ctx);
        Annotate(annotated.Lambda, expression.Range);
        if (use == EUse::Limit || use == EUse::Offset) {
            // Take was only a vehicle for its count-validation rule. Retain the
            // rewritten count (including coercions/defaults), then annotate the
            // replacement lambda so its result type describes the count itself.
            auto count = annotated.Lambda->Tail().TailPtr();
            annotated.Lambda = Ctx.NewLambda(pos, annotated.Lambda->HeadPtr(), std::move(count));
            Annotate(annotated.Lambda, expression.Range);
        }
        const auto& value = annotated.Lambda->Tail();
        if (!value.GetTypeAnn()) { Error(expression.Range, "Unable to infer scalar expression type"); }
        if (!EnsureComputable(value, Ctx)) { throw TAnnotationError{}; }
        if (use == EUse::Predicate && !EnsureSpecificDataType(value, EDataSlot::Bool, Ctx)) {
            Error(expression.Range, "Predicate must have Bool or Optional<Bool> type");
        }
        if (use == EUse::SortKey && !EnsureComparableType(pos, *value.GetTypeAnn(), Ctx)) {
            throw TAnnotationError{};
        }
        const auto type = value.GetTypeAnn();
        Result.Expressions[&expression] = std::move(annotated);
        return type;
    }

    // Synthetic identities have the same binding contract as resolved columns,
    // but belong to this typed AST rather than to name resolution.
    TColumnId Value(TType type) {
        const auto id = Result.Columns.size();
        Result.Columns.push_back(type);
        return id;
    }

    TString AggregateName(const TExpression& expression) const {
        const auto* call = std::get_if<TCall>(&expression.Value);
        if (!call || call->Over) { return {}; }
        const auto* name = std::get_if<TName>(&call->Callable->Value);
        if (!name || name->Module) { return {}; }
        const auto function = to_lower(name->Name.Spelling);
        if (function == "sum" || function == "min" || function == "max" || function == "avg"
            || function == "count" || function == "some") { return function; }
        if (function == "stddev_samp" || function == "stddev_sample") { return "variance_1_1"; }
        return {};
    }

    // Compare group expressions after ordinary scalar lowering. Binding every
    // column to a stable atom makes aliases/qualification irrelevant, without
    // treating merely similar text (or differently bound names) as equal.
    bool SameGroup(const TExpression& a, const TExpression& b) {
        if (&a == &b) { return true; }
        if (const auto* left = Names.ColumnReferences.FindPtr(&a)) {
            const auto* right = Names.ColumnReferences.FindPtr(&b);
            const auto sourceIdentity = [&](TColumnId id) {
                // Follow projection aliases, but stop at a source occurrence:
                // two references to one CTE still denote different joined rows.
                while (!Names.Columns[id].Source && Names.Columns[id].Input) { id = *Names.Columns[id].Input; }
                return id;
            };
            return right && sourceIdentity(*left) == sourceIdentity(*right);
        }
        if (a.Value.index() != b.Value.index()) { return false; }
        bool relational = false;
        VisitScalar(a, [&](const auto& node) {
            if (!AggregateName(node).empty() || std::holds_alternative<TSubquery>(node.Value)
                || std::holds_alternative<TExists>(node.Value)) { relational = true; return false; }
            if (const auto* call = std::get_if<TCall>(&node.Value); call && call->Over) { relational = true; return false; }
            return true;
        });
        if (relational) { return false; }
        TExpressionBindings bindings;
        bindings.Names = &Names;
        for (TColumnId id = 0; id < Names.Columns.size(); ++id) { bindings.Columns[id] = Ctx.NewAtom(Pos(a.Range), ToString(id)); }
        auto left = LowerExpression(a, Script, bindings, Ctx, Settings.Expressions);
        auto right = LowerExpression(b, Script, bindings, Ctx, Settings.Expressions);
        if (!left || !right) { throw TAnnotationError{}; }
        const auto* x = left.Get();
        const auto* y = right.Get();
        return CompareExprTrees(x, y);
    }

    void PrepareSubqueries(const TExpression& expression, TColumnTypes& scope) {
        VisitScalar(expression, [&](const TExpression& node) {
            if (const auto* id = Result.Values.FindPtr(&node); id && scope.contains(*id)) { return false; }
            const TQuery* query = nullptr;
            EAstSubquery kind = EAstSubquery::Scalar;
            const TExpression* comparison = nullptr;
            if (const auto* subquery = std::get_if<TSubquery>(&node.Value)) { query = subquery->Query.get(); }
            if (const auto* exists = std::get_if<TExists>(&node.Value)) { query = exists->Query.get(); kind = EAstSubquery::Exists; }
            if (const auto* in = std::get_if<TIn>(&node.Value)) {
                if (const auto* subquery = std::get_if<TSubquery>(&in->Collection->Value)) {
                    query = subquery->Query.get(); kind = EAstSubquery::In; comparison = in->Value.get();
                }
            }
            if (!query) { return true; }
            if (comparison) { Expression(*comparison, scope); }
            Query(*query, scope);
            const auto& outputs = Result.QueryOutputs.at(query);
            if (kind != EAstSubquery::Exists && outputs.size() != 1) { Error(node.Range, "A scalar/IN subquery must return one column"); }
            auto type = kind == EAstSubquery::Scalar ? Optional(outputs.front())
                : Ctx.MakeType<TDataExprType>(EDataSlot::Bool);
            const auto id = Value(type);
            Result.Values[&node] = id;
            Result.Subqueries.emplace(id, TAnnotatedSubquery{query, kind, comparison});
            scope[id] = type;
            // A grandparent dependency must be available to this query before
            // its nested subplan can capture it in turn.
            for (const auto& [captured, capturedType] : Result.Captures[query]) {
                if (Outer.contains(captured)) { Result.Captures[CurrentQuery][captured] = capturedType; }
            }
            return false;
        });
    }

    void Grouping(const TQuery& query, const TSelect& select, TColumnTypes& scope) {
        TAnnotatedGrouping grouping;
        const auto input = scope;
        // Enumerate the grouping tree. Concatenation is a cartesian product;
        // ROLLUP emits prefixes and CUBE emits the power set, including ().
        using TSets = TVector<TVector<TColumnId>>;
        const auto product = [](const TSets& left, const TSets& right) {
            TSets result;
            for (const auto& a : left) { for (const auto& b : right) {
                auto keys = a;
                for (const auto key : b) { if (std::find(keys.begin(), keys.end(), key) == keys.end()) { keys.push_back(key); } }
                result.push_back(std::move(keys));
            } }
            return result;
        };
        std::function<TSets(const TGroupingElement&)> expand = [&](const TGroupingElement& element) -> TSets {
            if (element.Expression.Expression) {
                const auto& expression = *element.Expression.Expression;
                for (const auto& [id, key] : grouping.Keys) { if (SameGroup(expression, *key)) { return {{id}}; } }
                const auto type = Expression(expression, scope);
                const auto* alias = Names.GroupAliases.FindPtr(&expression);
                const auto* column = Names.ColumnReferences.FindPtr(&expression);
                const auto id = alias ? *alias : column ? *column : Value(type);
                Result.Columns[id] = type;
                grouping.Keys.emplace_back(id, &expression);
                return {{id}};
            }
            TSets result = {{}};
            if (element.Kind == EGroupingKind::Sets) { result.clear(); }
            for (const auto& child : element.Elements) {
                const auto next = expand(child);
                if (element.Kind == EGroupingKind::Sets) { result.insert(result.end(), next.begin(), next.end()); }
                else if (element.Kind == EGroupingKind::Cube) {
                    auto added = product(result, next); result.insert(result.end(), added.begin(), added.end());
                } else { result = product(result, next); }
            }
            if (element.Kind == EGroupingKind::Rollup) {
                const auto full = result.front(); result.clear();
                // Match YqlGroupingSet's prefix order, starting with the grand total.
                for (size_t n = 0; n <= full.size(); ++n) { result.emplace_back(full.begin(), full.begin() + n); }
            }
            return result;
        };
        TSets sets = {{}};
        for (const auto& group : select.GroupBy) { sets = product(sets, expand(group)); }
        const bool scalar = std::any_of(sets.begin(), sets.end(), [](const auto& set) { return set.empty(); });
        TColumnTypes grouped = Outer;
        for (const auto& [id, key] : grouping.Keys) {
            Y_UNUSED(key);
            auto type = Result.Columns[id];
            if (std::any_of(sets.begin(), sets.end(), [&](const auto& set) { return std::find(set.begin(), set.end(), id) == set.end(); })) {
                type = Optional(type);
            }
            grouped[id] = type;
        }
        // Each aggregate is collected in its own query scope. Its argument is
        // typed against input rows; enclosing arithmetic is typed against the
        // aggregate result, so nested aggregate arguments cannot slip through.
        const auto collect = [&](const TExpression& root) {
            VisitScalar(root, [&](const TExpression& node) {
                const auto function = AggregateName(node);
                if (function.empty()) { return true; }
                if (Result.Values.contains(&node)) { return false; }
                const auto& call = std::get<TCall>(node.Value);
                if (call.Filter || call.NullTreatment != ENullTreatment::Default) { Unsupported(node.Range, "aggregate FILTER/null treatment"); }
                if ((call.StarArgument && function != "count") || (!call.StarArgument && call.Arguments.size() != 1)
                    || (call.StarArgument && call.Quantifier == ESetQuantifier::Distinct)) {
                    Error(node.Range, "Invalid aggregate arguments");
                }
                const auto* argument = call.StarArgument ? nullptr : call.Arguments.front().Expression.get();
                // SELECT, HAVING and ORDER BY can reference the same aggregate.
                // Reuse its result so DISTINCT expansion does not add another
                // branch for an already computed value.
                for (const auto& aggregate : grouping.Aggregates) {
                    if (aggregate.Function == function && aggregate.Distinct == (call.Quantifier == ESetQuantifier::Distinct)
                        && ((!argument && !aggregate.Argument)
                            || (argument && aggregate.Argument && SameGroup(*argument, *aggregate.Argument)))) {
                        Result.Values[&node] = aggregate.Output;
                        return false;
                    }
                }
                TType type = argument ? Expression(*argument, input) : Ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
                if (function == "count") { type = Ctx.MakeType<TDataExprType>(EDataSlot::Uint64); }
                else if (function == "sum") {
                    if (!GetSumResultType(Pos(node.Range), *type, type, Ctx)) { throw TAnnotationError{}; }
                } else if (function == "avg" || function == "variance_1_1") {
                    if (!GetAvgResultType(Pos(node.Range), *type, type, Ctx)) { throw TAnnotationError{}; }
                } else if (!EnsureComparableType(Pos(node.Range), *type, Ctx)) { throw TAnnotationError{}; }
                if (scalar && function != "count") { type = Optional(type); }
                const auto id = Value(type);
                grouping.Aggregates.push_back({id, argument, function, call.Quantifier == ESetQuantifier::Distinct});
                Result.Values[&node] = id;
                grouped[id] = type;
                return false;
            });
        };
        for (const auto& projection : select.Columns) { collect(*projection.Expression); }
        if (select.Having) { collect(*select.Having); }
        for (const auto& sort : select.OrderBy) { collect(*sort.Expression); }
        if (grouping.Keys.empty() && grouping.Aggregates.empty() && select.GroupBy.empty()) { return; }
        const auto bindKeys = [&](const TExpression& root) {
            VisitScalar(root, [&](const TExpression& node) {
                if (Result.Values.contains(&node)) { return false; }
                if (const auto* call = std::get_if<TCall>(&node.Value)) {
                    const auto* name = std::get_if<TName>(&call->Callable->Value);
                    if (name && !name->Module && AsciiEqualsIgnoreCase(name->Name.Spelling, "Grouping")) {
                        if (call->Arguments.size() != 1) { Unsupported(node.Range, "multi-key GROUPING masks"); }
                        for (const auto& [keyId, key] : grouping.Keys) {
                            if (SameGroup(*call->Arguments.front().Expression, *key)) {
                                const auto type = Ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
                                const auto id = Value(type);
                                grouping.Indicators[id] = keyId;
                                Result.Values[&node] = id; grouped[id] = type;
                                return false;
                            }
                        }
                        Error(node.Range, "GROUPING argument is not a grouping key");
                    }
                }
                for (const auto& [id, key] : grouping.Keys) {
                    if (SameGroup(node, *key)) { Result.Values[&node] = id; return false; }
                }
                return true;
            });
        };
        for (const auto& projection : select.Columns) { bindKeys(*projection.Expression); }
        if (select.Having) { bindKeys(*select.Having); }
        for (const auto& sort : select.OrderBy) { bindKeys(*sort.Expression); }
        if (sets.size() > 1 || (sets.size() == 1 && sets.front().size() != grouping.Keys.size())) { grouping.Sets = std::move(sets); }
        Result.Groupings[&query] = std::move(grouping);
        scope = std::move(grouped);
    }

    // Window arguments consume grouped rows when grouping is present. Collect
    // the inner aggregates first, then type partitions, ordering and arguments
    // against that scope before publishing the window result identity.
    void Windows(const TQuery& query, const TSelect& select, TColumnTypes& scope) {
        const auto collect = [&](const TExpression& root) {
            VisitScalar(root, [&](const TExpression& node) {
                const auto* call = std::get_if<TCall>(&node.Value);
                if (!call || !call->Over) { return true; }
                if (Result.Values.contains(&node)) { return false; }
                const auto* name = std::get_if<TName>(&call->Callable->Value);
                if (!name || name->Module || call->Filter || call->Quantifier == ESetQuantifier::Distinct
                    || call->NullTreatment != ENullTreatment::Default) { Unsupported(node.Range, "this window function modifier"); }
                auto function = to_lower(name->Name.Spelling);
                if (function == "dense_rank") { function = "denserank"; }
                if (function == "row_number") { function = "rownumber"; }
                const bool native = function == "rank" || function == "denserank" || function == "rownumber";
                if (!native && function != "count" && function != "sum" && function != "avg" && function != "min" && function != "max") {
                    Unsupported(node.Range, "this window function");
                }
                if ((native && !call->Arguments.empty()) || (!native && !call->StarArgument && call->Arguments.size() != 1)
                    || (call->StarArgument && function != "count")) { Error(node.Range, "Invalid window arguments"); }
                const auto* window = std::get_if<TWindowSpecification>(&call->Over->Value);
                if (!window) { window = &Names.WindowReferences.at(&*call->Over)->Specification; }
                if (window->BaseWindow) { Unsupported(window->Range, "window inheritance"); }
                for (const auto& key : window->PartitionBy) { Expression(*key.Expression, scope); }
                for (const auto& key : window->OrderBy) { Expression(*key.Expression, scope, EUse::SortKey); }
                bool nullable = false;
                if (window->Frame) {
                    const auto& frame = *window->Frame;
                    if (frame.Exclusion != EWindowExclusion::Default && frame.Exclusion != EWindowExclusion::NoOthers) {
                        Unsupported(frame.Range, "window frame exclusions");
                    }
                    if (frame.Begin.Offset) { Expression(*frame.Begin.Offset, {}, EUse::Offset); }
                    if (frame.End && frame.End->Offset) { Expression(*frame.End->Offset, {}, EUse::Offset); }
                    nullable = frame.Begin.Kind == EWindowBoundKind::Following || frame.Begin.Kind == EWindowBoundKind::UnboundedFollowing
                        || (frame.End && (frame.End->Kind == EWindowBoundKind::Preceding || frame.End->Kind == EWindowBoundKind::UnboundedPreceding));
                }
                const auto* argument = call->Arguments.empty() ? nullptr : call->Arguments.front().Expression.get();
                TType type = argument ? Expression(*argument, scope) : Ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
                if (native || function == "count") { type = Ctx.MakeType<TDataExprType>(EDataSlot::Uint64); }
                else if (function == "sum") { if (!GetSumResultType(Pos(node.Range), *type, type, Ctx)) { throw TAnnotationError{}; } }
                else if (function == "avg") { if (!GetAvgResultType(Pos(node.Range), *type, type, Ctx)) { throw TAnnotationError{}; } }
                if (!native && function != "count" && nullable) { type = Optional(type); }
                const auto id = Value(type);
                Result.Values[&node] = id;
                Result.Windows[&query].push_back({id, argument, window, function, native});
                scope[id] = type;
                return false;
            });
        };
        for (const auto& projection : select.Columns) { collect(*projection.Expression); }
        for (const auto& sort : select.OrderBy) { collect(*sort.Expression); }
    }

    // A source has its own output schema. ON is typed against the original
    // input schemas before outer-join null extension; enclosing clauses consume
    // the nullable output schema stored for the join, not the child read schema.
    TColumnTypes Source(const TSource& source) {
        if (source.Sampling) { Unsupported(source.Range, "sampling"); }
        TColumnTypes scope;
        if (const auto* join = std::get_if<TJoin>(&source.Value)) {
            auto left = Source(*join->Left);
            auto right = Source(*join->Right);
            scope = left;
            scope.insert(right.begin(), right.end());
            if (join->On) { Expression(*join->On, scope, EUse::Predicate); }
            const auto nullable = [&](TColumnTypes& columns) {
                for (auto& [id, type] : columns) { Y_UNUSED(id); type = Optional(type); }
            };
            switch (join->Kind) {
                case EJoinKind::Left: nullable(right); break;
                case EJoinKind::Right: nullable(left); break;
                case EJoinKind::Full:
                case EJoinKind::Exclusion: nullable(left); nullable(right); break;
                case EJoinKind::LeftSemi:
                case EJoinKind::LeftOnly: right.clear(); break;
                case EJoinKind::RightSemi:
                case EJoinKind::RightOnly: left.clear(); break;
                case EJoinKind::Inner:
                case EJoinKind::Cross: break;
            }
            scope = std::move(left);
            scope.insert(right.begin(), right.end());
        } else {
            if (const auto* derived = std::get_if<TDerivedTable>(&source.Value)) { Query(*derived->Query); }
            if (const auto* query = Names.NamedSources.FindPtr(&source)) { Query(**query); }
            if (const auto* columns = SourceColumns.FindPtr(&source)) {
                for (const auto id : *columns) { scope[id] = Column(id); }
            }
        }
        for (const auto& [id, type] : scope) { Result.Columns[id] = type; }
        Result.Sources[&source] = scope;
        return scope;
    }

    // Visit CTE/derived producers before their consumers so every referenced
    // output has an established type. Cache by query identity for shared uses.
    // Grouping/windows require scope rules that this scalar subset does not supply.
    void Query(const TQuery& query, const TColumnTypes& outer = {}) {
        if (Result.QueryOutputs.contains(&query)) { return; }
        const auto* previous = CurrentQuery;
        auto previousOuter = std::move(Outer);
        CurrentQuery = &query;
        Outer = outer;
        Y_DEFER { CurrentQuery = previous; Outer = std::move(previousOuter); };
        for (const auto& cte : query.With) { Query(*cte.Query); }
        const auto* select = std::get_if<TSelect>(&query.Body);
        if (!select) {
            const auto& set = std::get<TSetOperation>(query.Body);
            Query(*set.Left, outer); Query(*set.Right, outer);
            const auto& left = Result.QueryOutputs.at(set.Left.get());
            const auto& right = Result.QueryOutputs.at(set.Right.get());
            TVector<TType> outputs;
            for (size_t i = 0; i < left.size(); ++i) {
                // AsList's existing coercion rule computes the common column
                // type; branch maps will apply these same target types in RBO.
                auto a = Ctx.NewArgument(Pos(query.Range), "left"); a->SetTypeAnn(left[i]);
                auto b = Ctx.NewArgument(Pos(query.Range), "right"); b->SetTypeAnn(right[i]);
                auto lambda = Ctx.NewLambda(Pos(query.Range), Ctx.NewArguments(Pos(query.Range), {a, b}), Call(Pos(query.Range), "AsList", {a, b}));
                UpdateLambdaAllArgumentsTypes(lambda, {left[i], right[i]}, Ctx);
                Annotate(lambda, query.Range);
                outputs.push_back(lambda->Tail().GetTypeAnn()->Cast<TListExprType>()->GetItemType());
                Result.Columns[Names.QueryOutputs.at(&query)[i]] = outputs.back();
                const auto conversion = [&](size_t branch) {
                    TAnnotatedExpression expression;
                    expression.Inputs = {Names.QueryOutputs.at(set.Left.get())[i], Names.QueryOutputs.at(set.Right.get())[i]};
                    expression.Lambda = Ctx.NewLambda(Pos(query.Range), lambda->HeadPtr(), lambda->Tail().ChildPtr(branch));
                    Annotate(expression.Lambda, query.Range);
                    return expression;
                };
                Result.SetConversions[&query].emplace_back(conversion(0), conversion(1));
            }
            if (query.Limit) { Expression(*query.Limit, {}, EUse::Limit); }
            if (query.Offset) { Expression(*query.Offset, {}, EUse::Offset); }
            Result.QueryOutputs[&query] = std::move(outputs);
            return;
        }
        auto scope = select->From ? Source(*select->From) : TColumnTypes{};
        scope.insert(outer.begin(), outer.end());
        if (select->Where) { Expression(*select->Where, scope, EUse::Predicate); }
        Grouping(query, *select, scope);
        Windows(query, *select, scope);
        const auto& outputIds = Names.QueryOutputs.at(&query);
        TVector<TType> outputs;
        for (const auto& projection : select->Columns) {
            const auto& expression = *projection.Expression;
            const auto type = Expression(expression, scope);
            if (Names.Stars.contains(&expression)) {
                for (const auto item : type->Cast<TTupleExprType>()->GetItems()) { outputs.push_back(item); }
            } else {
                outputs.push_back(type);
            }
        }
        Y_ENSURE(outputIds.size() == outputs.size());
        // Publish projection types before ORDER BY, retaining input types for
        // hidden sort expressions. Name resolution has already decided which
        // IDs each reference denotes, including output-alias precedence.
        for (size_t i = 0; i < outputs.size(); ++i) {
            Result.Columns[outputIds[i]] = outputs[i];
            scope[outputIds[i]] = outputs[i];
        }
        if (select->Having) { Expression(*select->Having, scope, EUse::Predicate); }
        for (const auto& sort : select->OrderBy) { Expression(*sort.Expression, scope, EUse::SortKey); }
        if (query.Limit) { Expression(*query.Limit, {}, EUse::Limit); }
        if (query.Offset) { Expression(*query.Offset, {}, EUse::Offset); }
        Result.QueryOutputs[&query] = std::move(outputs);
    }

    const TScript& Script;
    const TResolvedNames& Names;
    TExprContext& Ctx;
    TTypeAnnotationContext& Types;
    const TAstTypeAnnotationSettings& Settings;
    TAnnotatedAst Result;
    const TQuery* CurrentQuery = nullptr;
    TColumnTypes Outer;
    THashMap<const TSource*, TVector<TColumnId>> SourceColumns;
};

} // namespace

std::unique_ptr<TAnnotatedAst> AnnotateAst(const TScript& script, const TResolvedNames& names,
    TExprContext& ctx, TTypeAnnotationContext& types, const TAstTypeAnnotationSettings& settings)
{
    try {
        return TAnnotator(script, names, ctx, types, settings).Run();
    } catch (const TAnnotationError&) {
        return {};
    }
}

} // namespace NKikimr::NKqp::NNewYql
