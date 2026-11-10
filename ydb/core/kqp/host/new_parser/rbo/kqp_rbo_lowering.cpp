#include "kqp_rbo_lowering.h"

#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
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
        const auto* select = std::get_if<TSelect>(&query.Body);
        // Match RewriteSelect's presentation order for a plain SELECT *.
        if (!orderedColumns && select && select->Columns.size() == 1
            && Names.Stars.contains(select->Columns.front().Expression.get())) {
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
        return Finish(std::move(root));
    }

    TIntrusivePtr<TOpRoot> RunWrite(const TWriteStatement& write, bool orderedColumns) {
        const auto pos = Pos(write.Target->Range);
        const auto& metadata = *Names.Tables.at(write.Target.get());
        const auto table = Tables(metadata);
        if (table.StorageType != NYql::EStorageType::RowStorage) {
            Unsupported(write.Target->Range, "native column-table writes");
        }
        TRelation relation;
        if (const auto* rows = Annotations.WriteRows.FindPtr(&write)) {
            TUnorderedIUs ius;
            for (const auto id : SourceColumns.at(write.Target.get())) {
                const auto& name = Names.Columns[id].Name;
                if (!(*rows)->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->FindItemType(name)) { continue; }
                const auto iu = Props.InfoUnitRegistry.Add(TInfoUnit("", name));
                ius.Add(iu);
                relation.Bindings[id] = iu;
            }
            relation.Op = MakeIntrusive<TOpEmptySource>(pos, *rows, std::move(ius));
        } else {
            relation = write.Input ? Query(*write.Input) : Source(*write.Target);
        }
        if (write.Where) {
            relation.Op = MakeIntrusive<TOpFilter>(relation.Op, pos, Predicate(*write.Where, relation.Bindings));
        }
        TMapIUs values;
        TEffectOptions options;
        for (const auto& [name, expression] : Annotations.Writes.at(&write)) {
            const auto id = Props.InfoUnitRegistry.AddGenerated("write");
            values.Add(id, AnnotatedExpression(expression, write.Target->Range, relation.Bindings));
            options.Columns.Append(id, name);
        }
        auto input = MakeIntrusive<TOpMap>(relation.Op, pos, std::move(values));
        EEffectType kind;
        switch (write.Kind) {
            case EWriteKind::Insert:
                kind = metadata.HasWriteIndexes ? EEffectType::InsertRowsIndex : EEffectType::InsertRows;
                options.OnConflict = write.OnConflict;
                break;
            case EWriteKind::Upsert:
            case EWriteKind::Replace:
                kind = metadata.HasWriteIndexes ? EEffectType::UpsertRowsIndex : EEffectType::UpsertRows;
                if (write.Kind == EWriteKind::Replace) {
                    options.Settings = TVector<TExprNode::TPtr>{Ctx.NewList(pos, {
                        Ctx.NewAtom(pos, "Mode"), Ctx.NewAtom(pos, "replace")})};
                }
                break;
            case EWriteKind::Update:
                kind = metadata.HasWriteIndexes ? EEffectType::UpdateRowsIndex : EEffectType::UpdateRows;
                break;
            case EWriteKind::Delete:
                kind = metadata.HasWriteIndexes ? EEffectType::DeleteRowsIndex : EEffectType::DeleteRows;
                break;
        }
        auto returning = Names.ReturningColumns.at(&write);
        if (write.ReturningAll && !orderedColumns) { std::sort(returning.begin(), returning.end()); }
        for (const auto& name : returning) {
            options.ReturningColumns.Append(Props.InfoUnitRegistry.Add(TInfoUnit("", name)), name);
        }
        auto columns = options.ReturningColumns;
        auto effect = MakeIntrusive<TOpTableEffect>(input, pos, table.Table, kind, std::move(options));
        auto root = MakeIntrusive<TOpRoot>(effect, pos, std::move(columns));
        Props.WithEffects = true;
        Props.WithReturning = !returning.empty();
        return Finish(std::move(root));
    }

private:
    TIntrusivePtr<TOpRoot> Finish(TIntrusivePtr<TOpRoot> root) {
        root->PlanProps = std::move(Props);
        for (const auto& item : IterateSubtreeWithSubplans(root.Get(), root->PlanProps)) { item.Current->BindExpressionPlanProps(&root->PlanProps); }
        root->RecomputeOutputIUsSubtree();
        root->ComputeParents();
        return root;
    }

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
        return AnnotatedExpression(Annotations.Expressions.at(&expression), expression.Range, bindings);
    }

    TRboExpression AnnotatedExpression(const TAnnotatedExpression& annotated, const TSourceRange& range, const TBindings& bindings) {
        const auto pos = annotated.Lambda->Pos();
        // Only newly imported subplan results belong to this expression. Reuse
        // the relational scope without copying all of its column bindings.
        TBindings subqueryBindings;
        // Subplan bindings are scalar inputs but have their own relational
        // producer. Captures are allocated by Query with local IU identities.
        // Lambda expansion and type rewrites can eliminate an input after its
        // initial binding. Import only subplans still referenced by the body.
        TNodeSet used;
        VisitExpr(annotated.Lambda->TailPtr(), [&](const TExprNode::TPtr& node) {
            if (node->IsArgument()) { used.insert(node.Get()); }
            return true;
        });
        for (size_t i = 0; i < annotated.Inputs.size(); ++i) {
            if (!used.contains(annotated.Lambda->Head().Child(i))) { continue; }
            const auto column = annotated.Inputs[i];
            const auto* subquery = Annotations.Subqueries.FindPtr(column);
            if (!subquery || bindings.contains(column)) { continue; }
            auto producer = Query(*subquery->Query, bindings);
            const auto id = Props.InfoUnitRegistry.AddGenerated("subquery");
            TOrderedIUs<> tuple;
            std::optional<TInfoUnitId> result;
            auto kind = ESubplanType::EXPR;
            if (subquery->Kind == EAstSubquery::Exists) { kind = ESubplanType::EXISTS; }
            else { result = producer.Bindings.at(Names.QueryOutputs.at(subquery->Query).front()); }
            if (subquery->Kind == EAstSubquery::In) {
                kind = ESubplanType::IN_SUBPLAN;
                const auto comparison = Expression(*subquery->Comparison, bindings);
                if (!comparison.IsColumnAccess()) { Unsupported(range, "computed IN subquery input"); }
                tuple.Append(*comparison.GetRawInputIUs().begin());
            }
            Props.Subplans.Add(id, producer.Op, kind, std::move(tuple), result);
            subqueryBindings[column] = id;
        }
        auto row = Ctx.NewArgument(pos, "row");
        TNodeOnNodeOwnedMap replacements;
        for (size_t i = 0; i < annotated.Inputs.size(); ++i) {
            // Do not allocate row accesses for inputs eliminated by annotation.
            if (!used.contains(annotated.Lambda->Head().Child(i))) { continue; }
            const auto* id = subqueryBindings.FindPtr(annotated.Inputs[i]);
            if (!id) { id = bindings.FindPtr(annotated.Inputs[i]); }
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
                Unsupported(range, "expression outside its relational scope");
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
            TMapIUs leftKeys, rightKeys;
            TVector<TRboExpression> filters;
            if (join->On) {
                auto predicate = Predicate(*join->On, bindings);
                // Evaluate side-local computed keys before the join, including
                // outer joins: computing them after null-extension changes ON
                // semantics and leaves RBO without equality keys.
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
                    if (term.MaybeExprEquiJoinCondition()) {
                        const auto body = term.GetExpressionBody();
                        const size_t first = body->IsCallable("PgResolvedOp") ? 2 : 0;
                        TRboExpression a(body->ChildPtr(first), &Ctx, &Props), b(body->ChildPtr(first + 1), &Ctx, &Props);
                        const auto belongs = [](const TRboExpression& expression, const TRelation& input) {
                            return !expression.GetInputIUs().Empty() && expression.GetInputIUs().IsSubsetOf(input.Op->GetOutputIUs());
                        };
                        if (belongs(a, right) && belongs(b, left)) { std::swap(a, b); }
                        if (belongs(a, left) && belongs(b, right)) {
                            const auto key = [&](TRboExpression expression, TMapIUs& definitions) {
                                if (expression.IsColumnAccess()) { return *expression.GetRawInputIUs().begin(); }
                                const auto id = Props.InfoUnitRegistry.AddGenerated("join_key");
                                definitions.Add(id, std::move(expression));
                                return id;
                            };
                            keys.Add(key(std::move(a), leftKeys), key(std::move(b), rightKeys));
                            continue;
                        }
                    }
                    filters.push_back(std::move(term));
                }
            }
            if (!leftKeys.Keys().Empty()) { left.Op = MakeIntrusive<TOpMap>(left.Op, pos, std::move(leftKeys)); }
            if (!rightKeys.Keys().Empty()) { right.Op = MakeIntrusive<TOpMap>(right.Op, pos, std::move(rightKeys)); }
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
        const auto& columns = SourceColumns[&source];
        TRelation result;
        // Allocate IUs for stored columns, then resolve alias IDs through Input.
        // Keep the complete read schema here; column pruning, ranges and physical
        // read properties belong to later RBO rules.
        if (const auto* metadata = Names.Tables.FindPtr(&source)) {
            const auto table = Tables(**metadata);
            if (!table.Table) { Unsupported(source.Range, "unavailable table metadata"); }
            TString alias = source.Alias ? Identifier(*source.Alias) : Identifier(std::get<TTable>(source.Value).Name);
            TUnorderedIUs ius;
            // The legacy read schema is a struct, whose fields are name-sorted.
            // Keep the same IU allocation order: join key pairs are ordered by
            // IU identity, which also determines physical hash key sequences.
            auto storedColumns = columns;
            std::stable_sort(storedColumns.begin(), storedColumns.end(), [&](auto a, auto b) {
                return Names.Columns[a].Name < Names.Columns[b].Name;
            });
            for (const auto id : storedColumns) {
                const auto& column = Names.Columns[id];
                if (column.MetadataColumnId) {
                    const auto iu = Props.InfoUnitRegistry.Add(TInfoUnit(alias, column.Name));
                    ius.Add(iu);
                    result.Bindings[id] = iu;
                }
            }
            result.Op = MakeIntrusive<TOpRead>(alias, std::move(ius), table.StorageType,
                table.Table, nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
        } else if (const auto* expression = std::get_if<TExpressionSource>(&source.Value)) {
            const auto& annotated = Annotations.Expressions.at(expression->Expression.get());
            Y_ENSURE(annotated.Inputs.empty());
            TUnorderedIUs ius;
            for (const auto id : columns) {
                if (Names.Columns[id].Input) { continue; }
                const auto iu = Props.InfoUnitRegistry.Add(TInfoUnit("", Names.Columns[id].Name));
                ius.Add(iu);
                result.Bindings[id] = iu;
            }
            result.Op = MakeIntrusive<TOpEmptySource>(pos, annotated.Lambda->TailPtr(), std::move(ius));
        } else {
            const auto* derived = std::get_if<TDerivedTable>(&source.Value);
            const auto* named = Names.NamedSources.FindPtr(&source);
            // Named preamble queries share the same producer/port machinery as
            // WITH CTEs. The source-to-query link was fixed at definition/use
            // resolution, so later redefinitions cannot retarget this read.
            result = Reference(named ? **named : derived ? *derived->Query : Producer(columns.front()));
        }
        TBindings visible;
        for (const auto id : columns) { visible[id] = Input(id, result.Bindings); }
        result.Bindings = std::move(visible);
        return result;
    }

    // Aggregate arguments and computed keys are mapped before aggregation.
    // Output bindings contain only keys/results, enforcing SQL grouping scope.
    void Grouping(const TQuery& query, TRelation& relation) {
        const auto* grouping = Annotations.Groupings.FindPtr(&query);
        if (!grouping) { return; }
        const auto pos = Pos(query.Range);
        TMapIUs definitions;
        TOrderedIUs<> keys;
        TAggregationIUs aggregates;
        TBindings outputs;
        const auto map = [&](TRboExpression expression, TStringBuf label) {
            if (expression.IsColumnAccess()) { return *expression.GetRawInputIUs().begin(); }
            const auto id = Props.InfoUnitRegistry.AddGenerated(label);
            definitions.Add(id, std::move(expression));
            return id;
        };
        for (const auto& [column, expression] : grouping->Keys) {
            const auto id = map(Expression(*expression, relation.Bindings), "group_key");
            keys.Append(id); outputs[column] = id;
        }
        for (const auto& aggregate : grouping->Aggregates) {
            auto argument = aggregate.Argument ? Expression(*aggregate.Argument, relation.Bindings)
                : MakeConstant("Uint64", "1", pos, &Ctx);
            if (aggregate.Key) {
                auto key = Expression(*aggregate.Key, relation.Bindings);
                auto row = Ctx.NewArgument(pos, "row");
                auto payload = Ctx.ReplaceNode(argument.GetExpressionBody(), argument.GetLambda()->Head().Head(), row);
                auto keyValue = Ctx.ReplaceNode(key.GetExpressionBody(), key.GetLambda()->Head().Head(), row);
                TExprNode::TPtr body;
                const auto keyType = Annotations.Expressions.at(aggregate.Key).GetType();
                if (keyType->GetKind() == ETypeAnnotationKind::Null) {
                    auto tuple = Ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
                        keyType, Annotations.Expressions.at(aggregate.Argument).GetType()});
                    body = Ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *Ctx.MakeType<TOptionalExprType>(tuple), Ctx)});
                } else if (keyType->IsOptionalOrNull()) {
                    auto present = Ctx.NewArgument(pos, "key");
                    body = Ctx.NewCallable(pos, "Map", {keyValue,
                        Ctx.NewLambda(pos, Ctx.NewArguments(pos, {present}), Ctx.NewList(pos, {present, payload}))});
                } else {
                    body = Ctx.NewList(pos, {keyValue, payload});
                }
                argument = TRboExpression(Ctx.NewLambda(pos, Ctx.NewArguments(pos, {row}), std::move(body)), &Ctx, &Props);
            }
            const auto input = map(std::move(argument), "aggregate_argument");
            const auto output = Props.InfoUnitRegistry.AddGenerated("aggregate");
            aggregates.Add(output, TOpAggregationTraits{input, aggregate.Function, aggregate.Distinct});
            outputs[aggregate.Output] = output;
        }
        if (!definitions.Keys().Empty()) { relation.Op = MakeIntrusive<TOpMap>(relation.Op, pos, std::move(definitions)); }
        auto aggregate = MakeIntrusive<TOpAggregate>(relation.Op, std::move(aggregates), std::move(keys), EOpPhase::Undefined, false, pos);
        relation.Op = aggregate;
        if (!grouping->Sets.empty()) {
            TVector<TUnorderedIUs> sets;
            for (const auto& set : grouping->Sets) {
                TUnorderedIUs ius;
                for (const auto column : set) { ius.Add(outputs.at(column)); }
                sets.push_back(std::move(ius));
            }
            TMappedIUs<TInfoUnitId> indicators;
            for (const auto& [column, key] : grouping->Indicators) {
                const auto id = Props.InfoUnitRegistry.AddGenerated("grouping_indicator");
                indicators.Add(id, outputs.at(key));
                relation.Bindings[column] = id;
            }
            TMappedIUs<TInfoUnitId> columns;
            for (auto& [column, id] : outputs) {
                Y_UNUSED(column);
                const auto output = Props.InfoUnitRegistry.AddCopy(id);
                columns.Add(output, id); id = output;
            }
            relation.Op = MakeIntrusive<TOpGroupingSets>(aggregate, std::move(sets), std::move(columns), pos, std::move(indicators));
            for (const auto& [column, key] : grouping->Indicators) { Y_UNUSED(key); outputs[column] = relation.Bindings.at(column); }
        } else if (!grouping->Indicators.empty()) {
            TMapIUs constants;
            for (const auto& [column, key] : grouping->Indicators) {
                Y_UNUSED(key);
                const auto id = Props.InfoUnitRegistry.AddGenerated("grouping_indicator");
                constants.Add(id, MakeConstant("Uint64", "0", pos, &Ctx)); outputs[column] = id;
            }
            relation.Op = MakeIntrusive<TOpMap>(relation.Op, pos, std::move(constants));
        }
        TMapIUs payloads;
        for (const auto& aggregate : grouping->Aggregates) {
            if (!aggregate.Key) { continue; }
            auto row = Ctx.NewArgument(pos, "row");
            auto state = Ctx.NewCallable(pos, "Member", {row, Ctx.NewAtom(pos, outputs.at(aggregate.Output))});
            const auto payload = [&](TExprNode::TPtr tuple) {
                return Ctx.NewCallable(pos, "Nth", {std::move(tuple), Ctx.NewAtom(pos, "1")});
            };
            TExprNode::TPtr body;
            if (Annotations.Columns.at(aggregate.Output)->GetKind() == ETypeAnnotationKind::Null) {
                body = Ctx.NewCallable(pos, "Null", {});
            } else if (aggregate.OptionalState) {
                auto present = Ctx.NewArgument(pos, "state");
                auto value = payload(present);
                if (!Annotations.Expressions.at(aggregate.Argument).GetType()->IsOptionalOrNull()) {
                    value = Ctx.NewCallable(pos, "Just", {std::move(value)});
                }
                body = Ctx.NewCallable(pos, "IfPresent", {state,
                    Ctx.NewLambda(pos, Ctx.NewArguments(pos, {present}), std::move(value)),
                    Ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *Annotations.Columns.at(aggregate.Output), Ctx)})});
            } else { body = payload(state); }
            const auto output = Props.InfoUnitRegistry.AddGenerated("aggregate_payload");
            payloads.Add(output, TRboExpression(Ctx.NewLambda(pos, Ctx.NewArguments(pos, {row}), std::move(body)), &Ctx, &Props));
            outputs[aggregate.Output] = output;
        }
        if (!payloads.Keys().Empty()) { relation.Op = MakeIntrusive<TOpMap>(relation.Op, pos, std::move(payloads)); }
        relation.Bindings = std::move(outputs);
    }

    void Windows(const TQuery& query, TRelation& relation) {
        const auto* windows = Annotations.Windows.FindPtr(&query);
        if (!windows) { return; }
        for (const auto& window : *windows) {
            const auto& specification = *window.Specification;
            const auto pos = Pos(specification.Range);
            TMapIUs definitions;
            const auto map = [&](TRboExpression expression) {
                if (expression.IsColumnAccess()) { return *expression.GetRawInputIUs().begin(); }
                const auto id = Props.InfoUnitRegistry.AddGenerated("window_argument");
                definitions.Add(id, std::move(expression)); return id;
            };
            TOrderedIUs<> partitions;
            TSortIUs ordering;
            for (const auto& key : specification.PartitionBy) { partitions.Append(map(Expression(*key.Expression, relation.Bindings))); }
            for (const auto& key : specification.OrderBy) {
                ordering.Append(map(Expression(*key.Expression, relation.Bindings)), TSortOrder{key.Direction != ESortDirection::Descending, true});
            }
            TOpWindowFunc function;
            function.Function = window.Function;
            function.Kind = window.Native ? EWindowFuncKind::Native : EWindowFuncKind::Aggregate;
            if (!window.Native) {
                function.Arguments.Append(map(window.Argument ? Expression(*window.Argument, relation.Bindings)
                    : MakeConstant("Uint64", "1", pos, &Ctx)));
            }
            TOpWindowFrame frame;
            frame.Type = EWindowFrameType::Range;
            frame.EndKind = specification.OrderBy.empty() ? EWindowFrameBound::UnboundedFollowing : EWindowFrameBound::CurrentRow;
            if (specification.Frame) {
                const auto& syntax = *specification.Frame;
                frame.Type = syntax.Units == EWindowFrameUnits::Rows ? EWindowFrameType::Rows
                    : syntax.Units == EWindowFrameUnits::Range ? EWindowFrameType::Range : EWindowFrameType::Groups;
                const auto bound = [&](const TWindowBound& value, EWindowFrameBound& kind, ui64& offset) {
                    switch (value.Kind) {
                        case EWindowBoundKind::CurrentRow: kind = EWindowFrameBound::CurrentRow; break;
                        case EWindowBoundKind::Preceding: kind = EWindowFrameBound::Preceding; break;
                        case EWindowBoundKind::Following: kind = EWindowFrameBound::Following; break;
                        case EWindowBoundKind::UnboundedPreceding: kind = EWindowFrameBound::UnboundedPreceding; break;
                        case EWindowBoundKind::UnboundedFollowing: kind = EWindowFrameBound::UnboundedFollowing; break;
                    }
                    if (value.Offset) {
                        const auto literal = LiteralCount(*Count(*value.Offset).GetExpressionBody());
                        if (!literal) { Unsupported(value.Range, "nonconstant window bounds"); }
                        offset = *literal;
                    }
                };
                bound(syntax.Begin, frame.BeginKind, frame.BeginValue);
                if (syntax.End) { bound(*syntax.End, frame.EndKind, frame.EndValue); }
                else { frame.EndKind = EWindowFrameBound::CurrentRow; }
            }
            if (!definitions.Keys().Empty()) { relation.Op = MakeIntrusive<TOpMap>(relation.Op, pos, std::move(definitions)); }
            const auto id = Props.InfoUnitRegistry.AddGenerated("window_result");
            TWindowIUs functions; functions.Add(id, std::move(function));
            relation.Op = MakeIntrusive<TOpWindow>(relation.Op, pos, std::move(functions), std::move(partitions), std::move(ordering), frame);
            relation.Bindings[window.Output] = id;
        }
    }

    void Limit(const TQuery& query, TRelation& relation) {
        if (!query.Limit && !query.Offset) { return; }
        const auto pos = Pos(query.Range);
        auto count = query.Limit ? Count(*query.Limit) : MakeConstant("Uint64", "18446744073709551615", pos, &Ctx);
        std::optional<TRboExpression> offset;
        if (query.Offset) { offset = Count(*query.Offset); }
        relation.Op = MakeIntrusive<TOpLimit>(relation.Op, pos, TPhysicalOpProps{}, count, std::move(offset), EOpPhase::Undefined);
    }

    void Distinct(const TVector<TColumnId>& outputIds, TPositionHandle pos, TRelation& result) {
        TOrderedIUs<> keys;
        TAggregationIUs aggregates;
        for (const auto column : outputIds) {
            const auto input = result.Bindings.at(column);
            keys.Append(input);
            const auto output = Props.InfoUnitRegistry.Add(TInfoUnit("", Names.Columns[column].Name));
            aggregates.Add(output, TOpAggregationTraits{input, "distinct"});
            result.Bindings[column] = output;
        }
        result.Op = MakeIntrusive<TOpAggregate>(result.Op, std::move(aggregates), std::move(keys), EOpPhase::Undefined, true, pos);
    }

    void Sort(const TVector<TSortSpecification>& orderBy, TPositionHandle pos, TRelation& relation) {
        if (!orderBy.empty()) {
            // Simple column keys reuse existing IUs; computed keys need hidden Map
            // definitions. These generated columns participate in sorting but never
            // appear in the query's externally visible output binding map.
            TMapIUs sortDefinitions;
            TSortIUs keys;
            for (const auto& sort : orderBy) {
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
    }

    TRelation SetOperation(const TQuery& query, const TSetOperation& set, const TBindings& outer) {
        if (set.Op != ESetOperation::Union && set.Quantifier == ESetQuantifier::All) {
            Unsupported(query.Range, "INTERSECT/EXCEPT ALL multiplicities");
        }
        auto left = Query(*set.Left, outer), right = Query(*set.Right, outer);
        const auto& outputIds = Names.QueryOutputs.at(&query);
        const auto& leftIds = Names.QueryOutputs.at(set.Left.get());
        const auto& rightIds = Names.QueryOutputs.at(set.Right.get());
        const auto pos = Pos(query.Range);
        // Both operands must expose the common type computed by the existing
        // YQL annotator, including optionality and numeric widening.
        const auto widen = [&](TRelation& relation, const TVector<TColumnId>& ids, bool rightBranch) {
            TMapIUs definitions;
            for (size_t i = 0; i < ids.size(); ++i) {
                const auto* type = Annotations.Columns[outputIds[i]];
                if (IsSameAnnotation(*type, *Annotations.Columns[ids[i]])) { continue; }
                const auto id = Props.InfoUnitRegistry.AddGenerated("set_column");
                const auto& conversions = Annotations.SetConversions.at(&query)[i];
                definitions.Add(id, AnnotatedExpression(rightBranch ? conversions.second : conversions.first, query.Range, relation.Bindings));
                relation.Bindings[ids[i]] = id;
            }
            if (!definitions.Keys().Empty()) { relation.Op = MakeIntrusive<TOpMap>(relation.Op, pos, std::move(definitions)); }
        };
        widen(left, leftIds, false); widen(right, rightIds, true);
        TRelation result;
        if (set.Op == ESetOperation::Union) {
            TUnionAllIUs columns(TUnionInputPolicy{2});
            for (size_t i = 0; i < outputIds.size(); ++i) {
                const auto id = Props.InfoUnitRegistry.Add(TInfoUnit("", Names.Columns[outputIds[i]].Name));
                TUnionInputRow row; row.Inputs = {left.Bindings.at(leftIds[i]), right.Bindings.at(rightIds[i])};
                columns.Add(id, std::move(row)); result.Bindings[outputIds[i]] = id;
            }
            result.Op = MakeIntrusive<TOpUnionAll>(left.Op, right.Op, pos, std::move(columns));
        } else {
            TMapIUs leftPickles, rightPickles;
            TJoinIUs keys;
            for (size_t i = 0; i < outputIds.size(); ++i) {
                auto a = left.Bindings.at(leftIds[i]), b = right.Bindings.at(rightIds[i]);
                result.Bindings[outputIds[i]] = a;
                if (Annotations.Columns[outputIds[i]]->IsOptionalOrNull()) {
                    const auto pickle = [&](TInfoUnitId input, TMapIUs& definitions) {
                        const auto id = Props.InfoUnitRegistry.AddGenerated("null_safe_key");
                        definitions.Add(id, MakeUnaryCallable("StablePickle", MakeColumnAccess(input, pos, &Ctx, &Props)));
                        return id;
                    };
                    a = pickle(a, leftPickles); b = pickle(b, rightPickles);
                }
                keys.Add(a, b);
            }
            if (!leftPickles.Keys().Empty()) {
                left.Op = MakeIntrusive<TOpMap>(left.Op, pos, std::move(leftPickles));
                right.Op = MakeIntrusive<TOpMap>(right.Op, pos, std::move(rightPickles));
            }
            result.Op = MakeIntrusive<TOpJoin>(left.Op, right.Op, pos,
                set.Op == ESetOperation::Intersect ? "LeftSemi" : "LeftOnly", std::move(keys));
        }
        if (set.Quantifier != ESetQuantifier::All) {
            // Match the legacy set-operation aggregate's physical key order.
            // SQL positions remain in outputIds and Bindings; struct type
            // annotation does not preserve projection order.
            auto distinctColumns = outputIds;
            std::stable_sort(distinctColumns.begin(), distinctColumns.end(), [&](auto a, auto b) {
                return Names.Columns[a].Name < Names.Columns[b].Name;
            });
            Distinct(distinctColumns, pos, result);
        }
        Sort(query.OrderBy, pos, result);
        Limit(query, result);
        return result;
    }

    // Build logical operators in SQL evaluation order: input, WHERE, projection,
    // DISTINCT, sorting, then count/offset. A source-free SELECT uses EmptySource
    // as the singleton input on which constant projections are evaluated.
    TRelation Query(const TQuery& query, const TBindings& outer = {}) {
        const auto* select = std::get_if<TSelect>(&query.Body);
        if (!select) { return SetOperation(query, std::get<TSetOperation>(query.Body), outer); }
        if (select->AssumeOrderBy) { Unsupported(query.Range, "ASSUME ORDER BY"); }
        const auto pos = Pos(query.Range);
        TRelation relation = select->From ? Source(*select->From) : TRelation{MakeIntrusive<TOpEmptySource>(pos), {}};
        if (const auto* captures = Annotations.Captures.FindPtr(&query); captures && !captures->empty()) {
            TDependencyIUs dependencies;
            for (const auto& [column, type] : *captures) {
                const auto input = outer.at(column);
                const auto local = Props.InfoUnitRegistry.AddCopy(input);
                dependencies.Add(local, TCapturedIU{input, type});
                relation.Bindings[column] = local;
            }
            relation.Op = MakeIntrusive<TOpAddDependencies>(relation.Op, pos, std::move(dependencies));
        }
        if (select->Where) {
            relation.Op = MakeIntrusive<TOpFilter>(std::move(relation.Op), pos, Predicate(*select->Where, relation.Bindings));
        }
        Grouping(query, relation);
        Windows(query, relation);
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
        if (select->Having) {
            relation.Op = MakeIntrusive<TOpFilter>(relation.Op, pos, Predicate(*select->Having, relation.Bindings));
        }
        // DISTINCT exports fresh values just like the legacy importer, keeping
        // its physical distribution and aggregation traits consistent.
        if (select->Quantifier == ESetQuantifier::Distinct) {
            relation.Bindings = std::move(outputs);
            Distinct(outputIds, pos, relation);
            outputs = relation.Bindings;
        }
        Sort(select->OrderBy, pos, relation);
        Limit(query, relation);
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

TIntrusivePtr<TOpRoot> LowerWriteToRbo(const TWriteStatement& write, const TScript& script,
    const TResolvedNames& names, const TAnnotatedAst& annotations, TExprContext& ctx,
    const TRboTableLookup& tables, bool orderedColumns)
{
    return TLowerer(script, names, annotations, ctx, tables).RunWrite(write, orderedColumns);
}

} // namespace NKikimr::NKqp::NNewYql
