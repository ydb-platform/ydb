#include "kqp_rbo_lowering.h"

#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <util/stream/str.h>
#include <util/string/cast.h>

#include <algorithm>

namespace NKikimr::NKqp::NNewYql {
namespace {

using namespace NYql;
using TRboExpression = NKqp::TExpression;
using TBindings = THashMap<TColumnId, TInfoUnitId>;

// Bindings translate frontend column identities into the IUs produced by this
// operator. They can include hidden inputs needed by ORDER BY until the final
// query boundary narrows the result to projected columns.
struct TRelation {
    TIntrusivePtr<IOperator> Op;
    TBindings Bindings;
};

// One producer can have several CTE consumers. A replicate hub shares the
// producer while assigning independent IU identities to additional ports.
struct TSharedQuery {
    TIntrusivePtr<TReplicate> Hub;
    TBindings Outputs;
};

class TLowerer {
public:
    TLowerer(const TScript& script, const TResolvedNames& names, const TAnnotatedAst& annotations,
        TExprContext& ctx, const TRboTableLookup& tables)
        : Script(script), Names(names), Annotations(annotations), Ctx(ctx), Tables(tables)
    {
        for (TColumnId id = 0; id < names.Columns.size(); ++id) {
            if (const auto* source = names.Columns[id].Source) { SourceColumns[source].push_back(id); }
        }
        for (const auto& [query, outputs] : names.QueryOutputs) {
            for (const auto id : outputs) { OutputQueries[id] = query; }
        }
    }

    // Only this outer boundary creates the result contract and owns PlanProps.
    // Subqueries return relations so their IUs live in the same registry as the
    // parent plan, even when their producer is shared.
    TIntrusivePtr<TOpRoot> Run(const TQuery& query, bool orderedColumns) {
        auto relation = Query(query);
        auto outputIds = Names.QueryOutputs.at(&query);
        const auto& select = std::get<TSelect>(query.Body);
        // Match RewriteSelect's presentation order for a plain SELECT *.
        if (!orderedColumns && select.Columns.size() == 1
            && Names.Stars.contains(select.Columns.front().Expression.get())) {
            std::stable_sort(outputIds.begin(), outputIds.end(), [&](auto a, auto b) {
                return Names.Columns[a].Name < Names.Columns[b].Name;
            });
        }
        TOrderedIUs<TString> columns;
        for (const auto id : outputIds) { columns.Append(relation.Bindings.at(id), Names.Columns[id].Name); }
        auto root = MakeIntrusive<TOpRoot>(std::move(relation.Op), Pos(query.Range), std::move(columns));
        // Expressions hold a pointer to their plan properties. Moving ownership
        // into the returned root requires rebinding every expression before the
        // short-lived lowerer and its original Props member are destroyed.
        root->PlanProps = std::move(Props);
        for (const auto& item : *root) { item.Current->BindExpressionPlanProps(&root->PlanProps); }
        root->RecomputeOutputIUsSubtree();
        root->ComputeParents();
        return root;
    }

private:
    TPositionHandle Pos(const TSourceRange& range) {
        return Ctx.AppendPosition(TPosition(range.Column + 1, range.Line, Script.File));
    }

    [[noreturn]] void Unsupported(const TSourceRange& range, TStringBuf feature) const {
        ythrow TNewYqlParserFallback() << "New YQL RBO lowering does not support " << feature
            << " at " << Script.File << ":" << range.Line << ":" << range.Column + 1;
    }

    TString Identifier(const TIdentifier& id) const {
        const auto& s = id.Spelling;
        if (Script.AnsiLexer && s.StartsWith('"')) {
            TString result;
            for (size_t i = 1; i + 1 < s.size(); ++i) {
                result += s[i];
                if (s[i] == '"' && i + 2 < s.size() && s[i + 1] == '"') { ++i; }
            }
            return result;
        }
        if (s.StartsWith('`') || s.StartsWith('"') || s.StartsWith('\'')) {
            TString result;
            TStringOutput output(result);
            size_t read = 0;
            const TStringBuf atom(s.data() + 1, s.size() - 1);
            Y_ENSURE(UnescapeArbitraryAtom(atom, s[0], &output, &read) == EUnescapeResult::OK && read == atom.size());
            return result;
        }
        return s;
    }

    // Convert the annotation lambda's column arguments to Member(row, IU)
    // accesses in one RBO row lambda. Substitution uses node identity and retains
    // all rewrites/coercions produced by the existing type annotator.
    TRboExpression Expression(const TExpression& expression, const TBindings& bindings) {
        const auto& annotated = Annotations.Expressions.at(&expression);
        const auto pos = annotated.Lambda->Pos();
        auto row = Ctx.NewArgument(pos, "row");
        TNodeOnNodeOwnedMap replacements;
        for (size_t i = 0; i < annotated.Inputs.size(); ++i) {
            // Annotation includes all columns in scope, including unused ones.
            // DISTINCT may have removed some; reject only actual references.
            const auto* id = bindings.FindPtr(annotated.Inputs[i]);
            if (id) {
                replacements[annotated.Lambda->Head().Child(i)] = Ctx.NewCallable(pos, "Member",
                    {row, Ctx.NewAtom(pos, *id)});
            }
        }
        auto body = Ctx.ReplaceNodes(annotated.Lambda->TailPtr(), replacements);
        // Nested lambdas may own legitimate arguments of their own. Exclude those
        // from the dangling-argument check; any remaining foreign argument means
        // a real dependency is unavailable in the current relational scope.
        TNodeSet locals;
        VisitExpr(body, [&](const TExprNode::TPtr& node) {
            if (node->IsLambda()) {
                for (const auto& arg : node->Head().Children()) { locals.insert(arg.Get()); }
            }
            return true;
        });
        VisitExpr(body, [&](const TExprNode::TPtr& node) {
            if (node->IsArgument() && node != row && !locals.contains(node.Get())) {
                Unsupported(expression.Range, "expression outside its relational scope");
            }
            return true;
        });
        return TRboExpression(Ctx.NewLambda(pos, Ctx.NewArguments(pos, {row}), std::move(body)), &Ctx, &Props);
    }

    TRboExpression Predicate(const TExpression& expression, const TBindings& bindings) {
        auto predicate = Expression(expression, bindings);
        const auto body = predicate.GetExpressionBody();
        // RBO predicates accept Optional<Bool>; physical filters supply the
        // NULL-to-false conversion. Keep comparisons visible to logical rules.
        if (body->IsCallable("Coalesce") && body->ChildrenSize() == 2
            && body->Tail().IsCallable("Bool") && body->Tail().Head().Content() == "false") {
            return TRboExpression(body->HeadPtr(), &Ctx, &Props);
        }
        return predicate;
    }

    // Recognize only nonnegative integral constants through the wrappers added
    // by count annotation. Dynamic expressions are left to runtime evaluation;
    // this is not a general constant folder.
    std::optional<ui64> LiteralCount(const TExprNode& node) const {
        if (node.IsCallable("Coalesce") || node.IsCallable("Just")
            || (node.IsCallable("Convert") && (node.Tail().IsAtom("Uint64")
                || (node.Tail().IsCallable("DataType") && node.Tail().Head().IsAtom("Uint64"))))) {
            return LiteralCount(node.Head());
        }
        if (node.IsCallable({"Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32", "Int64", "Uint64"})) {
            ui64 value;
            if (TryFromString(node.Head().Content(), value)) { return value; }
        }
        return std::nullopt;
    }

    TRboExpression Count(const TExpression& expression) {
        auto count = Expression(expression, {});
        // Like RewriteSelect's ProcessCount, expose integer literals before
        // RBO's limit/top-sort pushdown. Leave dynamic and nullable counts
        // intact, including their SQL LIMIT/OFFSET defaults.
        if (const auto literal = LiteralCount(*count.GetExpressionBody())) {
            return TRboExpression(Ctx.NewCallable(Pos(expression.Range), "Uint64",
                {Ctx.NewAtom(Pos(expression.Range), ToString(*literal))}), &Ctx, &Props);
        }
        return count;
    }

    // Aliases can add several layers of logical IDs without creating new values.
    // Follow their provenance until reaching an IU supplied by the current source.
    TInfoUnitId Input(TColumnId id, const TBindings& bindings) const {
        if (const auto* found = bindings.FindPtr(id)) { return *found; }
        Y_ENSURE(Names.Columns[id].Input, "Missing resolved input column " << id);
        return Input(*Names.Columns[id].Input, bindings);
    }

    // A CTE alias may point through renamed columns before reaching a query
    // output. Recover the producer from that provenance, not from table spelling.
    const TQuery& Producer(TColumnId id) const {
        if (const auto* query = OutputQueries.FindPtr(id)) { return **query; }
        Y_ENSURE(Names.Columns[id].Input, "Missing derived query output " << id);
        return Producer(*Names.Columns[id].Input);
    }

    // Cache the query producer once, then obtain a fresh consumer port per use.
    // The non-primary port's rebindings must also update the visible column map;
    // otherwise a self join would reference the first consumer's IUs on both sides.
    TRelation Reference(const TQuery& query) {
        auto it = Shared.find(&query);
        if (it == Shared.end()) {
            auto relation = Query(query);
            TBindings outputs;
            for (const auto id : Names.QueryOutputs.at(&query)) { outputs[id] = relation.Bindings.at(id); }
            it = Shared.emplace(&query, TSharedQuery{
                TReplicate::Create(std::move(relation.Op), Pos(query.Range), Props.InfoUnitRegistry), std::move(outputs)}).first;
        }
        auto port = it->second.Hub->AddOutput();
        auto bindings = it->second.Outputs;
        if (!port->IsPrimary()) {
            const auto& rebindings = port->GetRebindings();
            for (auto& [column, id] : bindings) { Y_UNUSED(column); id = *rebindings.Find(id); }
        }
        return {std::move(port), std::move(bindings)};
    }

    TRelation Source(const TSource& source) {
        if (source.Sampling || !source.Hints.empty()) { Unsupported(source.Range, "sampling and table hints"); }
        const auto pos = Pos(source.Range);
        if (const auto* join = std::get_if<TJoin>(&source.Value)) {
            if (join->Natural || !join->Using.empty() || join->LeftAny || join->RightAny) {
                Unsupported(source.Range, "NATURAL, USING and ANY joins");
            }
            auto left = Source(*join->Left);
            auto right = Source(*join->Right);
            auto bindings = left.Bindings;
            bindings.insert(right.Bindings.begin(), right.Bindings.end());
            TJoinIUs keys;
            TVector<TRboExpression> filters;
            if (join->On) {
                auto predicate = Predicate(*join->On, bindings);
                // Extract only equalities crossing the two inputs and orient keys
                // left-to-right. Same-side equalities and all other conjuncts remain
                // join filters, including residual predicates of outer joins.
                for (auto& term : predicate.SplitConjunct()) {
                    if (term.MaybeEquiJoinCondition()) {
                        TEquiJoinCondition condition(term);
                        auto a = condition.GetLeftIU(), b = condition.GetRightIU();
                        if (right.Op->GetOutputIUs().Contains(a) && left.Op->GetOutputIUs().Contains(b)) { std::swap(a, b); }
                        if (left.Op->GetOutputIUs().Contains(a) && right.Op->GetOutputIUs().Contains(b)) {
                            keys.Add(a, b);
                            continue;
                        }
                    }
                    filters.push_back(std::move(term));
                }
            }
            TString kind;
            switch (join->Kind) {
                case EJoinKind::Inner: kind = "Inner"; break;
                case EJoinKind::Cross: kind = "Cross"; break;
                case EJoinKind::Left: kind = "Left"; break;
                case EJoinKind::Right: kind = "Right"; break;
                case EJoinKind::Full: kind = "Full"; break;
                case EJoinKind::Exclusion: kind = "Exclusion"; break;
                case EJoinKind::LeftSemi: kind = "LeftSemi"; break;
                case EJoinKind::RightSemi: kind = "RightSemi"; break;
                case EJoinKind::LeftOnly: kind = "LeftOnly"; break;
                case EJoinKind::RightOnly: kind = "RightOnly"; break;
            }
            if (!JoinOutputsRight(kind)) { bindings = std::move(left.Bindings); }
            if (!JoinOutputsLeft(kind)) { bindings = std::move(right.Bindings); }
            return {MakeIntrusive<TOpJoin>(std::move(left.Op), std::move(right.Op), pos, kind,
                std::move(keys), filters), std::move(bindings)};
        }
        const auto& columns = SourceColumns.at(&source);
        TRelation result;
        // Allocate IUs for stored columns, then resolve alias IDs through Input.
        // Keep the complete read schema here; column pruning, ranges and physical
        // read properties belong to later RBO rules.
        if (const auto* metadata = Names.Tables.FindPtr(&source)) {
            const auto table = Tables(**metadata);
            if (!table.Table) { Unsupported(source.Range, "unavailable table metadata"); }
            TString alias = source.Alias ? Identifier(*source.Alias) : Identifier(std::get<TTable>(source.Value).Name);
            TUnorderedIUs ius;
            for (const auto id : columns) {
                const auto& column = Names.Columns[id];
                if (column.MetadataColumnId) {
                    const auto iu = Props.InfoUnitRegistry.Add(TInfoUnit(alias, column.Name));
                    ius.Add(iu);
                    result.Bindings[id] = iu;
                }
            }
            result.Op = MakeIntrusive<TOpRead>(alias, std::move(ius), table.StorageType,
                table.Table, nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
        } else {
            const auto* derived = std::get_if<TDerivedTable>(&source.Value);
            result = Reference(derived ? *derived->Query : Producer(columns.front()));
        }
        TBindings visible;
        for (const auto id : columns) { visible[id] = Input(id, result.Bindings); }
        result.Bindings = std::move(visible);
        return result;
    }

    // Build logical operators in SQL evaluation order: input, WHERE, projection,
    // DISTINCT, sorting, then count/offset. A source-free SELECT uses EmptySource
    // as the singleton input on which constant projections are evaluated.
    TRelation Query(const TQuery& query) {
        const auto* select = std::get_if<TSelect>(&query.Body);
        if (!select || !select->GroupBy.empty() || select->Having || !select->Windows.empty()) {
            Unsupported(query.Range, "set operations, grouping and windows");
        }
        if (select->AssumeOrderBy) { Unsupported(query.Range, "ASSUME ORDER BY"); }
        const auto pos = Pos(query.Range);
        TRelation relation = select->From ? Source(*select->From) : TRelation{MakeIntrusive<TOpEmptySource>(pos), {}};
        if (select->Where) {
            relation.Op = MakeIntrusive<TOpFilter>(std::move(relation.Op), pos, Predicate(*select->Where, relation.Bindings));
        }
        TMapIUs definitions;
        TBindings outputs;
        const auto& outputIds = Names.QueryOutputs.at(&query);
        size_t index = 0;
        const auto add = [&](TRboExpression expression) {
            const auto column = outputIds.at(index++);
            const auto id = Props.InfoUnitRegistry.Add(TInfoUnit("", Names.Columns[column].Name));
            definitions.Add(id, std::move(expression));
            outputs[column] = id;
        };
        for (const auto& projection : select->Columns) {
            if (const auto* star = Names.Stars.FindPtr(projection.Expression.get())) {
                for (const auto id : *star) { add(MakeColumnAccess(relation.Bindings.at(id), pos, &Ctx, &Props)); }
            } else {
                add(Expression(*projection.Expression, relation.Bindings));
            }
        }
        Y_ENSURE(index == outputIds.size());
        relation.Op = MakeIntrusive<TOpMap>(std::move(relation.Op), pos, std::move(definitions));
        // Map keeps input IUs alive as well as defining outputs. ORDER BY may use
        // an unprojected input column; discard those bindings only at a boundary
        // that removes them semantically, such as DISTINCT or the final result.
        relation.Bindings.insert(outputs.begin(), outputs.end());
        // DISTINCT is a key-only aggregate over projected values. Its output
        // scope excludes hidden inputs, so sorting by a lost dependency must fail
        // rather than generate a reference to an IU the aggregate no longer emits.
        if (select->Quantifier == ESetQuantifier::Distinct) {
            TOrderedIUs<> keys;
            for (const auto id : outputIds) { keys.Append(outputs.at(id)); }
            relation.Op = MakeIntrusive<TOpAggregate>(std::move(relation.Op), TAggregationIUs{}, std::move(keys),
                EOpPhase::Undefined, false, pos);
            relation.Bindings = outputs;
        }
        if (!select->OrderBy.empty()) {
            // Simple column keys reuse existing IUs; computed keys need hidden Map
            // definitions. These generated columns participate in sorting but never
            // appear in the query's externally visible output binding map.
            TMapIUs sortDefinitions;
            TSortIUs keys;
            for (const auto& sort : select->OrderBy) {
                auto expression = Expression(*sort.Expression, relation.Bindings);
                TInfoUnitId id;
                if (expression.IsColumnAccess()) { id = *expression.GetRawInputIUs().begin(); }
                else {
                    id = Props.InfoUnitRegistry.AddGenerated("sort_key");
                    sortDefinitions.Add(id, std::move(expression));
                }
                // SQL v1 YqlSort uses nulls-first for both directions.
                keys.Append(id, TSortOrder{sort.Direction != ESortDirection::Descending, true});
            }
            if (!sortDefinitions.Keys().Empty()) {
                relation.Op = MakeIntrusive<TOpMap>(std::move(relation.Op), pos, std::move(sortDefinitions));
            }
            relation.Op = MakeIntrusive<TOpSort>(std::move(relation.Op), pos, std::move(keys));
        }
        if (query.Limit || query.Offset) {
            auto count = query.Limit ? Count(*query.Limit) : MakeConstant("Uint64", "18446744073709551615", pos, &Ctx);
            std::optional<TRboExpression> offset;
            if (query.Offset) { offset = Count(*query.Offset); }
            relation.Op = MakeIntrusive<TOpLimit>(std::move(relation.Op), pos, TPhysicalOpProps{},
                count, std::move(offset), EOpPhase::Undefined);
        }
        relation.Bindings = std::move(outputs);
        return relation;
    }

    const TScript& Script;
    const TResolvedNames& Names;
    const TAnnotatedAst& Annotations;
    TExprContext& Ctx;
    const TRboTableLookup& Tables;
    TPlanProps Props;
    THashMap<const TSource*, TVector<TColumnId>> SourceColumns;
    THashMap<TColumnId, const TQuery*> OutputQueries;
    THashMap<const TQuery*, TSharedQuery> Shared;
};

} // namespace

TIntrusivePtr<TOpRoot> LowerToRbo(const TQuery& query, const TScript& script,
    const TResolvedNames& names, const TAnnotatedAst& annotations, TExprContext& ctx,
    const TRboTableLookup& tables, bool orderedColumns)
{
    return TLowerer(script, names, annotations, ctx, tables).Run(query, orderedColumns);
}

} // namespace NKikimr::NKqp::NNewYql
