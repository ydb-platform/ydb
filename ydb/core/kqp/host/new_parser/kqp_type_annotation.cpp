#include "kqp_type_annotation.h"
#include "kqp_new_parser.h"

#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

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
            } else {
                const auto& pragma = std::get<TPragmaStatement>(statement.Value);
                if (pragma.Namespace || (!AsciiEqualsIgnoreCase(pragma.Name.Spelling, "TablePathPrefix")
                    && !AsciiEqualsIgnoreCase(pragma.Name.Spelling, "YqlSelect"))) {
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
        Y_DEFER { Ctx.RepeatTransformCounter = repeats; };
        Ctx.RepeatTransformCounter = 0;
        TIssueScopeGuard issue(Ctx.IssueManager, [&] {
            return MakeIntrusive<TIssue>(Ctx.GetPosition(Pos(range)), "New YQL type annotation");
        });
        auto transformer = CreateTypeAnnotationTransformer(CreateExtCallableTypeAnnotationTransformer(Types, true), Types);
        if (InstantTransform(*transformer, root, Ctx) != IGraphTransformer::TStatus::Ok) {
            throw TAnnotationError{};
        }
    }

    // Wrap the lowered body in a closed, typed lambda before calling the YQL
    // annotator. Supplying all visible columns gives lowering a stable contract
    // even when a particular expression uses only a subset of the scope.
    TType Expression(const TExpression& expression, const TColumnTypes& scope, EUse use = EUse::Value) {
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
        Result.Expressions.emplace(&expression, std::move(annotated));
        return type;
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
    void Query(const TQuery& query) {
        if (Result.QueryOutputs.contains(&query)) { return; }
        for (const auto& cte : query.With) { Query(*cte.Query); }
        const auto* select = std::get_if<TSelect>(&query.Body);
        if (!select) { Unsupported(query.Range, "set operations"); }
        if (!select->GroupBy.empty() || select->Having || !select->Windows.empty()) {
            Unsupported(query.Range, "grouping and window scopes");
        }
        auto scope = select->From ? Source(*select->From) : TColumnTypes{};
        if (select->Where) { Expression(*select->Where, scope, EUse::Predicate); }
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
