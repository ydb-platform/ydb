#include "kqp_expression.h"
#include "kqp_rbo_utils.h"

#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_stat.h>
#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

#include <util/stream/str.h>
#include <util/string/cast.h>

#include <optional>

namespace NKikimr::NKqp {

namespace {

using namespace NYql::NNodes;
using namespace NKikimr;

bool IsRowMember(const TExprNode& node, const TExprNode& row) {
    return node.IsCallable("Member") && node.HeadPtr().Get() == &row;
}

// Replace free row arguments by identity. Members of nested structs and locally
// bound lambda arguments are not column references and must retain their meaning.
TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext& ctx) {
    TNodeSet bound;
    TNodeOnNodeOwnedMap replacements;
    VisitExpr(input, [&](const TExprNode::TPtr& node) {
        if (node->IsLambda()) {
            for (const auto& local : node->Head().Children()) {
                bound.insert(local.Get());
            }
        } else if (node->IsArgument()) {
            replacements.emplace(node.Get(), arg);
        }
        return true;
    });
    for (const auto* local : bound) {
        replacements.erase(local);
    }
    return ctx.ReplaceNodes(std::move(input), replacements);
}

// A new row argument drops the old schema and invalidates dependent annotations.
// Apply replacements simultaneously before rebinding, including new sublink IUs.
TExprNode::TPtr RewriteRow(TExprNode::TPtr lambda, const TNodeOnNodeOwnedMap& replacements, TExprContext& ctx) {
    const auto& row = lambda->Head().Head();
    auto newRow = ctx.NewArgument(row.Pos(), row.Content());
    auto body = ctx.ReplaceNodes(lambda->ChildPtr(1), replacements);
    body = ctx.ReplaceNodes(std::move(body), {{&row, newRow}});
    return ctx.NewLambda(lambda->Pos(), ctx.NewArguments(lambda->Head().Pos(), {newRow}), std::move(body));
}

bool TestAndExtractEqualityPredicate(TExprNode::TPtr pred, TExprNode::TPtr& leftArg, TExprNode::TPtr& rightArg) {
    if (pred->IsCallable("PgResolvedOp") && pred->ChildPtr(0)->Content() == "=") {
        leftArg = pred->ChildPtr(2);
        rightArg = pred->ChildPtr(3);
        return true;
    } else if (pred->IsCallable("==")) {
        leftArg = pred->ChildPtr(0);
        rightArg = pred->ChildPtr(1);
        return true;
    }
    return false;
}

bool SameExpr(const TExprNode::TPtr& left, const TExprNode::TPtr& right) {
    if (left.Get() == right.Get()) {
        return true;
    }

    const TExprNode* leftPtr = left.Get();
    const TExprNode* rightPtr = right.Get();
    return CompareExprTrees(leftPtr, rightPtr);
}

bool ContainsExpr(const TExprNode::TListType& nodes, const TExprNode::TPtr& needle) {
    return AnyOf(nodes, [&](const TExprNode::TPtr& node) {
        return SameExpr(node, needle);
    });
}

TExprNode::TPtr MakeLogical(TPositionHandle pos, TStringBuf op, TExprNode::TListType terms, TExprContext& ctx) {
    if (terms.empty()) {
        return op == "And" ? MakeBool<true>(pos, ctx) : MakeBool<false>(pos, ctx);
    }

    if (terms.size() == 1) {
        return terms.front();
    }

    return ctx.NewCallable(pos, op, std::move(terms));
}

TExprNode::TPtr MakeConjunct(TPositionHandle pos, TExprNode::TListType terms, TExprContext& ctx) {
    return MakeLogical(pos, "And", std::move(terms), ctx);
}

TExprNode::TPtr MakeDisjunct(TPositionHandle pos, TExprNode::TListType terms, TExprContext& ctx) {
    return MakeLogical(pos, "Or", std::move(terms), ctx);
}

struct TExtractedCommonExpressions {
    TExprNode::TListType Common;
    TVector<TExprNode::TListType> Residuals;
};

std::optional<TExtractedCommonExpressions> ExtractCommonExpressions(const TVector<TExprNode::TListType>& branches) {
    TExtractedCommonExpressions result;
    if (branches.empty()) {
        return std::nullopt;
    }

    auto& common = result.Common;
    for (const auto& candidate : branches.front()) {
        if (candidate->HasSideEffects() || ContainsExpr(common, candidate)) {
            continue;
        }

        if (AllOf(branches, [&](const auto& branch) { return ContainsExpr(branch, candidate); })) {
            common.push_back(candidate);
        }
    }

    if (common.empty()) {
        return std::nullopt;
    }

    result.Residuals.reserve(branches.size());
    for (const auto& branch : branches) {
        auto& residual = result.Residuals.emplace_back();
        for (const auto& term : branch) {
            if (!ContainsExpr(common, term)) {
                residual.push_back(term);
            }
        }
    }

    return result;
}

std::optional<TExprNode::TPtr> FactorCommonExpressions(TExprNode::TPtr node, TExprContext& ctx) {
    TExprNode::TListType disjuncts;
    GetOrTerms(node, disjuncts);
    if (disjuncts.size() < 2) {
        return std::nullopt;
    }

    TVector<TExprNode::TListType> branches;
    branches.reserve(disjuncts.size());
    for (const auto& disjunct : disjuncts) {
        GetAndTerms(disjunct, branches.emplace_back());
    }

    auto extracted = ExtractCommonExpressions(branches);
    if (!extracted) {
        return std::nullopt;
    }

    auto common = std::move(extracted->Common);
    auto residuals = std::move(extracted->Residuals);
    TExprNode::TListType residualDisjuncts;
    residualDisjuncts.reserve(residuals.size());

    for (auto& residual : residuals) {
        if (residual.empty()) {
            return MakeConjunct(node->Pos(), std::move(common), ctx);
        }

        residualDisjuncts.push_back(MakeConjunct(node->Pos(), std::move(residual), ctx));
    }

    common.push_back(MakeDisjunct(node->Pos(), std::move(residualDisjuncts), ctx));

    return MakeConjunct(node->Pos(), std::move(common), ctx);
}

TString QuoteString(TStringBuf value) {
    TStringStream out;
    out << '"';
    EscapeArbitraryAtom(value, '"', &out);
    out << '"';
    return out.Str();
}

std::optional<TString> FormatSimpleExpression(TExprNode::TPtr node, const TExprNode* row, const TInfoUnitRegistry& registry, ui32 depth = 0);

bool IsBinaryCallable(TStringBuf callable) {
    return callable == "+" || callable == "-" || callable == "*" || callable == "/" || callable == "%" ||
        callable == "==" || callable == "!=" || callable == "<" || callable == "<=" || callable == ">" || callable == ">=" ||
        callable == "DecimalAdd" || callable == "DecimalSub" || callable == "DecimalMul" || callable == "DecimalDiv";
}

TString GetBinaryOperator(TStringBuf callable) {
    if (callable == "DecimalAdd") {
        return "+";
    }
    if (callable == "DecimalSub") {
        return "-";
    }
    if (callable == "DecimalMul") {
        return "*";
    }
    if (callable == "DecimalDiv") {
        return "/";
    }
    return TString(callable);
}

TString GetLogicOperator(TStringBuf callable) {
    if (callable == "And") {
        return "AND";
    }
    if (callable == "Or") {
        return "OR";
    }
    if (callable == "Xor") {
        return "XOR";
    }
    return TString(callable);
}

bool NeedParens(TExprNode::TPtr node) {
    return node->IsCallable() && (IsBinaryCallable(node->Content()) || node->IsCallable({"And", "Or", "Xor"}));
}

std::optional<TString> FormatAtomLiteral(TExprNode::TPtr node) {
    if (!node->IsCallable() || node->ChildrenSize() == 0 || !node->Child(0)->IsAtom()) {
        return {};
    }

    const auto callable = node->Content();
    const auto value = node->Child(0)->Content();
    if (callable == "String" || callable == "Utf8") {
        return QuoteString(value);
    }
    if (callable == "Bool") {
        return ToString(value);
    }
    if (callable == "Date" || callable == "Datetime" || callable == "Timestamp") {
        return TStringBuilder() << callable << "(" << value << ")";
    }
    if (callable == "Decimal") {
        return ToString(value);
    }
    if (callable == "Int8" || callable == "Int16" || callable == "Int32" || callable == "Int64" ||
        callable == "Uint8" || callable == "Uint16" || callable == "Uint32" || callable == "Uint64" ||
        callable == "Float" || callable == "Double")
    {
        return ToString(value);
    }

    return {};
}

std::optional<TString> FormatSimpleBinary(TExprNode::TPtr node, const TExprNode* row, const TInfoUnitRegistry& registry, ui32 depth) {
    if (node->ChildrenSize() != 2) {
        return {};
    }

    auto left = FormatSimpleExpression(node->ChildPtr(0), row, registry, depth + 1);
    auto right = FormatSimpleExpression(node->ChildPtr(1), row, registry, depth + 1);
    if (!left || !right) {
        return {};
    }

    if (NeedParens(node->ChildPtr(0))) {
        left = TStringBuilder() << "(" << *left << ")";
    }
    if (NeedParens(node->ChildPtr(1))) {
        right = TStringBuilder() << "(" << *right << ")";
    }

    return TStringBuilder() << *left << " " << GetBinaryOperator(node->Content()) << " " << *right;
}

std::optional<TString> FormatSimpleLogic(TExprNode::TPtr node, const TExprNode* row, const TInfoUnitRegistry& registry, ui32 depth) {
    TVector<TString> parts;
    for (const auto& child : node->Children()) {
        auto part = FormatSimpleExpression(child, row, registry, depth + 1);
        if (!part) {
            return {};
        }
        parts.push_back(*part);
    }

    TStringBuilder result;
    const auto logicOp = GetLogicOperator(node->Content());
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i != 0) {
            result << " " << logicOp << " ";
        }
        result << parts[i];
    }
    return result;
}

std::optional<TString> FormatSimpleExpression(TExprNode::TPtr node, const TExprNode* row, const TInfoUnitRegistry& registry, ui32 depth) {
    if (!node || depth > 12) {
        return {};
    }
    if (node->IsLambda()) {
        return node->ChildrenSize() >= 2 ? FormatSimpleExpression(node->ChildPtr(1), row, registry, depth + 1) : std::optional<TString>();
    }
    if (!node->IsCallable()) {
        return {};
    }
    if (auto literal = FormatAtomLiteral(node)) {
        return literal;
    }
    if (node->IsCallable("Member")) {
        if (node->ChildrenSize() == 2 && node->Child(1)->IsAtom()) {
            if (node->Child(0) == row) {
                return registry.GetDisplayName(GetMemberId(*node));
            }
            return TString(node->Child(1)->Content());
        }
        return {};
    }
    if (node->IsCallable({"SafeCast", "Just", "Unwrap", "Convert"})) {
        return node->ChildrenSize() >= 1 ? FormatSimpleExpression(node->ChildPtr(0), row, registry, depth + 1) : std::optional<TString>();
    }
    if (node->IsCallable("Coalesce")) {
        if (node->ChildrenSize() != 2) {
            return {};
        }
        auto value = FormatSimpleExpression(node->ChildPtr(0), row, registry, depth + 1);
        auto fallback = FormatSimpleExpression(node->ChildPtr(1), row, registry, depth + 1);
        if (!value || !fallback) {
            return {};
        }
        return TStringBuilder() << "Coalesce(" << *value << ", " << *fallback << ")";
    }
    if (IsBinaryCallable(node->Content())) {
        return FormatSimpleBinary(node, row, registry, depth);
    }
    if (node->IsCallable({"And", "Or", "Xor"})) {
        return FormatSimpleLogic(node, row, registry, depth);
    }
    if (node->IsCallable("Not")) {
        if (node->ChildrenSize() != 1) {
            return {};
        }
        auto value = FormatSimpleExpression(node->ChildPtr(0), row, registry, depth + 1);
        if (!value) {
            return {};
        }
        if (NeedParens(node->ChildPtr(0))) {
            return TStringBuilder() << "NOT (" << *value << ")";
        }
        return TStringBuilder() << "NOT " << *value;
    }

    return {};
}

TString FormatExpressionDependencies(const TExpression& expr, const TInfoUnitRegistry& registry) {
    const auto& deps = expr.GetInputIUs(true, true);
    if (deps.Empty()) {
        return "<expr>";
    }
    TStringBuilder text;
    text << "<expr depends on: ";
    TStringBuf separator;
    for (const auto id : deps) {
        text << separator << registry.GetDisplayName(id);
        separator = ",";
    }
    return text << ">";
}

} // anonymous namespace

TExpression TExpression::FromExpr(TExprNode::TPtr lambda, const TBindings& bindings,
    TExprContext& ctx, TPlanProps& props, TNodeOnNodeOwnedMap replacements)
{
    Y_ENSURE(lambda && lambda->IsLambda() && lambda->Head().ChildrenSize() == 1, "Expected a single-row lambda");
    const auto& row = lambda->Head().Head();
    const auto bind = [&](TStringBuf spelling, TPositionHandle pos) {
        const auto it = bindings.find(TString(spelling));
        Y_ENSURE(it != bindings.end(), "Unknown input binding " << spelling);
        Y_ENSURE(it->second != TUnorderedIUs::InvalidBit, "Invalid input binding ID");
        return ctx.NewCallable(pos, "Member", {lambda->Head().HeadPtr(), ctx.NewAtom(pos, it->second)});
    };
    VisitExpr(lambda->ChildPtr(1), [&](const TExprNode::TPtr& node) {
        if (replacements.contains(node.Get())) {
            return false;
        }
        if (IsRowMember(*node, row)) {
            replacements.emplace(node.Get(), bind(node->Tail().Content(), node->Pos()));
            return false;
        }
        if (node.Get() == &row) {
            // A row used as a value retains its source struct shape. These
            // field labels are value data, not internal binding-lookup keys.
            Y_ENSURE(row.GetTypeAnn(), "Whole-row binding requires the source row type");
            TExprNode::TListType items;
            for (const auto* item : row.GetTypeAnn()->Cast<TStructExprType>()->GetItems()) {
                items.push_back(ctx.NewList(row.Pos(), {ctx.NewAtom(row.Pos(), item->GetName()), bind(item->GetName(), row.Pos())}));
            }
            replacements.emplace(&row, ctx.NewCallable(row.Pos(), "AsStruct", std::move(items)));
            return false;
        }
        return true;
    });
    return TExpression(RewriteRow(std::move(lambda), replacements, ctx), &ctx, &props);
}

TExpression::TExpression(TExprNode::TPtr node, TExprContext* ctx, TPlanProps* props) : Ctx(ctx), PlanProps(props) {
    Y_ENSURE(ctx, "Creating an expression with null context");

    if (node->IsLambda()) {
        Y_ENSURE(node->ChildrenSize() == 2 && node->Head().ChildrenSize() == 1, "Expected a single-row lambda");
        Node = node;
    } else {
        auto arg = Build<TCoArgument>(*ctx, node->Pos()).Name("lambda_arg").Done().Ptr();
        Node = Build<TCoLambda>(*ctx, node->Pos())
            .Args({arg})
            .Body(ReplaceArg(node, arg, *ctx))
            .Done().Ptr();
    }
}

TVector<TExpression> TExpression::SplitConjunct() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    Y_ENSURE(Ctx, "Expression context is null");

    TExprNode::TListType terms;
    GetAndTerms(GetExpressionBody(), terms);

    TVector<TExpression> conjuncts;
    conjuncts.reserve(terms.size());
    for (const auto& term : terms) {
        conjuncts.emplace_back(term, Ctx, PlanProps);
    }

    return conjuncts;
}

TVector<TExpression> TExpression::SplitDisjunct() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    Y_ENSURE(Ctx, "Expression context is null");

    TExprNode::TListType terms;
    GetOrTerms(GetExpressionBody(), terms);

    TVector<TExpression> disjuncts;
    disjuncts.reserve(terms.size());
    for (const auto& term : terms) {
        disjuncts.emplace_back(term, Ctx, PlanProps);
    }

    return disjuncts;
}

bool TExpression::IsColumnAccess() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    return IsRowMember(*Node->Child(1), Node->Head().Head());
}

 bool TExpression::IsSingleCallable(const THashSet<TString>& allowedCallables) const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
     auto body = Node->ChildPtr(1);
    if (body->IsCallable(allowedCallables) && body->ChildrenSize() == 1 && IsRowMember(body->Head(), Node->Head().Head())) {
        return true;
    } else {
        return false;
    }
 }

bool TExpression::MaybeEquiJoinCondition() const {
    return MaybeEquiJoinConditionInternal(false);
}

bool TExpression::MaybeExprEquiJoinCondition() const {
    return MaybeEquiJoinConditionInternal(true);
}

bool TExpression::MaybeEquiJoinConditionInternal(bool includeExpressions) const {
    auto body = GetExpressionBody();
    TExprNode::TPtr left;
    TExprNode::TPtr right;
    if (!TestAndExtractEqualityPredicate(body, left, right)) {
        return false;
    }
    if (!includeExpressions) {
        const auto& row = Node->Head().Head();
        return IsRowMember(*left, row) && IsRowMember(*right, row);
    }
    return !TExpression(left, Ctx, PlanProps).GetInputIUs(true, false).Empty()
        && !TExpression(right, Ctx, PlanProps).GetInputIUs(true, false).Empty();
}

bool TExpression::MaybeConstantCondition() const {
    auto body = Node->ChildPtr(1);
    if (TCoCompare::Match(body.Get())) {
        auto left = body->Child(0);
        auto right = body->Child(1);

        if (!IsConstantExpr(left) && !IsConstantExpr(right)) {
            return false;
        }

        auto ius = GetInputIUs(true, false);
        if (ius.Size() != 1) {
            return false;
        }

        return true;
    }

    return false;
}

TExprNode::TPtr TExpression::GetLambda() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    return Node;
}

TExprNode::TPtr TExpression::GetExpressionBody() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not a lambda");
    return Node->ChildPtr(1);
}

const TUnorderedIUs& TExpression::GetInputIUs(bool includeSubplanVars, bool includeCorrelatedDeps) const {
    const auto& rawInputIUs = GetRawInputIUs();
    if (rawInputIUs.Empty()) {
        return rawInputIUs;
    }

    Y_ENSURE(PlanProps, "Plan properties null for an expression with members");
    if (PlanProps->Subplans.Empty()) {
        return rawInputIUs;
    }

    const auto calls = PlanProps->Subplans.CallsIn(rawInputIUs);
    if (calls.Empty()) {
        return rawInputIUs;
    }

    // Only direct references are cached. Subplan additions/removals and changes
    // to their dependencies must be visible without changing the expression AST.
    ResolvedInputIUs = rawInputIUs;
    if (!includeSubplanVars) {
        ResolvedInputIUs.Subtract(calls);
    }
    if (includeCorrelatedDeps) {
        for (const auto call : calls) {
            const auto& subplan = PlanProps->Subplans.At(call);
            ResolvedInputIUs.UnionWith(subplan.Tuple.Unordered());
            ResolvedInputIUs.UnionWith(subplan.DependentIUs);
        }
    }
    return ResolvedInputIUs;
}

const TUnorderedIUs& TExpression::GetRawInputIUs() const {
    Y_ENSURE(Node && Node->IsLambda() && Node->Head().ChildrenSize() == 1, "Expected a single-row lambda");
    if (RawInputIUsCacheKey != Node) {
        TUnorderedIUs ids;
        const auto& row = Node->Head().Head();
        VisitExpr(GetExpressionBody(), [&](const TExprNode::TPtr& node) {
            if (IsRowMember(*node, row)) {
                ids.Add(GetMemberId(*node));
                return false;
            }
            Y_ENSURE(node.Get() != &row, "Expected explicit IU references, not a whole-row use");
            return true;
        });
        RawInputIUs = std::move(ids);
        RawInputIUsCacheKey = Node;
    }
    return RawInputIUs;
}

TExpression TExpression::ApplyRenames(const TSubstitutions& substitutions) const {
    Y_ENSURE(Node && Node->IsLambda() && Node->Head().ChildrenSize() == 1, "Expected a single-row lambda");
    const auto& row = Node->Head().Head();
    TNodeOnNodeOwnedMap replacements;
    VisitExpr(GetExpressionBody(), [&](const TExprNode::TPtr& node) {
        if (!IsRowMember(*node, row)) {
            return true;
        }
        const auto id = GetMemberId(*node);
        if (const auto replacement = Substitute(id, substitutions); replacement != id) {
            replacements.emplace(node.Get(), Ctx->ChangeChild(*node, 1, Ctx->NewAtom(node->Tail().Pos(), replacement)));
        }
        return false;
    });
    if (replacements.empty()) {
        return *this;
    }
    return TExpression(RewriteRow(Node, replacements, *Ctx), Ctx, PlanProps);
}

TExpression TExpression::ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not lambda");
    TOptimizeExprSettings settings(&ctx.TypeCtx);
    TExprNode::TPtr output;
    RemapExpr(Node, output, map, ctx.ExprCtx, settings);
    YQL_CLOG(TRACE, CoreDq) << "After replace " << PrintRBOExpression(output, *Ctx);

    return TExpression(output, Ctx, PlanProps);
}

std::optional<TExpression> TExpression::TryExtractCommonConjuncts() const {
    Y_ENSURE(Node->IsLambda(), "Expression node is not lambda");
    Y_ENSURE(Ctx, "Expression context is null");

    auto newPredicate = FactorCommonExpressions(GetExpressionBody(), *Ctx);
    if (!newPredicate) {
        return std::nullopt;
    }

    return TExpression(*newPredicate, Ctx, PlanProps);
}

TString PrintRBOExpression(TExprNode::TPtr expr, TExprContext& ctx, const TInfoUnitRegistry* registry) {
    if (registry && expr->IsLambda() && expr->Head().ChildrenSize() == 1) {
        const auto& row = expr->Head().Head();
        TNodeOnNodeOwnedMap replacements;
        VisitExpr(expr->TailPtr(), [&](const TExprNode::TPtr& node) {
            if (IsRowMember(*node, row)) {
                replacements.emplace(node.Get(), ctx.ChangeChild(*node, 1,
                    ctx.NewAtom(node->Tail().Pos(), registry->GetDebugName(GetMemberId(*node)))));
                return false;
            }
            return true;
        });
        expr = ctx.ReplaceNodes(std::move(expr), replacements);
    }
    if (expr->IsLambda()) {
        expr = expr->Child(1);
    }
    try {
        TConvertToAstSettings settings;
        settings.AllowFreeArgs = true;
 
        auto ast = ConvertToAst(*expr, ctx, settings);
        TStringStream exprStream;
        YQL_ENSURE(ast.Root);
        ast.Root->PrintTo(exprStream);

        TString exprText = exprStream.Str();

        return exprText;
    } catch (const std::exception& e) {
        return TStringBuilder() << "Failed to render expression to pretty string: " << e.what();
    }
}

TString TExpression::ToString() const {
    return PrintRBOExpression(Node, *Ctx, PlanProps ? &PlanProps->InfoUnitRegistry : nullptr);
}

TString TExpression::ToExplainString(const TInfoUnitRegistry& registry) const {
    const auto* row = Node && Node->IsLambda() && Node->Head().ChildrenSize() == 1 ? &Node->Head().Head() : nullptr;
    if (auto simple = FormatSimpleExpression(Node, row, registry)) {
        return *simple;
    }
    return FormatExpressionDependencies(*this, registry);
}

TEquiJoinCondition::TEquiJoinCondition(const TExpression& expr) : Expr(expr) {
    auto body = Expr.GetExpressionBody();
    TExprNode::TPtr left;
    TExprNode::TPtr right;
    if (!TestAndExtractEqualityPredicate(body, left, right)) {
        Y_ENSURE(body->ChildrenSize() == 2, "Non-binary callable in join condition");
        left = body->ChildPtr(0);
        right = body->ChildPtr(1);
    }
    LeftIUs = TExpression(left, Expr.Ctx, Expr.PlanProps).GetInputIUs(false, true);
    RightIUs = TExpression(right, Expr.Ctx, Expr.PlanProps).GetInputIUs(false, true);
    const auto& row = Expr.Node->Head().Head();
    IncludesExpressions = !IsRowMember(*left, row) || !IsRowMember(*right, row);
}

TInfoUnitId TEquiJoinCondition::GetLeftIU() const {
    Y_ENSURE(LeftIUs.Size() == 1);
    return *LeftIUs.begin();
}

TInfoUnitId TEquiJoinCondition::GetRightIU() const {
    Y_ENSURE(RightIUs.Size() == 1);
    return *RightIUs.begin();
}

bool TEquiJoinCondition::ExtractExpressions(TNodeOnNodeOwnedMap& replacements,
    TMappedIUs<TExprNode::TPtr>& expressions)
{
    Y_ENSURE(Expr.PlanProps, "Plan properties null when extracting expressions from join condition");
    if (!IncludesExpressions) {
        return false;
    }
    auto body = Expr.GetExpressionBody();
    TExprNode::TPtr left;
    TExprNode::TPtr right;
    Y_ENSURE(TestAndExtractEqualityPredicate(body, left, right));
    const auto row = Expr.Node->Head().HeadPtr();
    for (const auto& side : {left, right}) {
        if (!IsRowMember(*side, *row)) {
            const auto id = Expr.PlanProps->InfoUnitRegistry.AddGenerated("join_key");
            // clang-format off
            replacements[side.Get()] = Build<TCoMember>(*Expr.Ctx, side->Pos())
                .Struct(row)
                .Name().Value(Expr.Ctx->GetIndexAsString(id)).Build()
                .Done().Ptr();
            // clang-format on
            expressions.Add(id, side);
        }
    }
    return true;
}

TExpression MakeColumnAccess(TInfoUnitId column, TPositionHandle pos, TExprContext* ctx, TPlanProps* props) {
    Y_ENSURE(column != TUnorderedIUs::InvalidBit, "Invalid IU ID");
    auto lambda_arg = Build<TCoArgument>(*ctx, pos).Name("arg").Done().Ptr();

    // clang-format off
    auto lambda = Build<TCoLambda>(*ctx, pos)
        .Args({lambda_arg})
        .Body<TCoMember>()
            .Struct(lambda_arg)
            .Name().Value(ctx->GetIndexAsString(column)).Build()
        .Build()
        .Done().Ptr();
    // clang-format on

    return TExpression(lambda, ctx, props);
}

TExpression MakeConstant(const TString& type, const TString& value, TPositionHandle pos, TExprContext* ctx) {
     auto constExpr = ctx->NewCallable(pos, type, {ctx->NewAtom(pos, value)});
     return TExpression(constExpr, ctx);
}

TExpression MakeNothing(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext* ctx) {
    Y_ENSURE(type);
    auto nullExpr = ctx->NewCallable(pos, "Nothing", {ExpandType(pos, *type, *ctx)});
    return TExpression(nullExpr, ctx);
}

TExpression MakeConjunction(const TVector<TExpression>& vec, bool pgSyntax) {
    Y_ENSURE(vec.size());

    // Fetch context and plan properties from one of the conjuncts
    TExprContext* ctx = nullptr;
    TPlanProps* props = nullptr;

    for (auto& expr : vec) {
        if (expr.Ctx) {
            ctx = expr.Ctx;
        }
        if (expr.PlanProps) {
            props = expr.PlanProps;
        }
    }

    Y_ENSURE(ctx);
    Y_ENSURE(props);
    auto pos = vec[0].Node->Pos();

    if (vec.size() == 1) {
        return TExpression(vec[0].Node, ctx, props);
    }

    // clang-format off
    auto lambda_arg = Build<TCoArgument>(*ctx, pos).Name("arg").Done().Ptr();
    TVector<TExprNode::TPtr> conjuncts;

    for (auto & expr : vec) {
        auto exprLambda = expr.GetExpressionBody();
        conjuncts.push_back(ReplaceArg(exprLambda, lambda_arg, *ctx));
    }

    auto conjunction = Build<TCoAnd>(*ctx, pos)
        .Add(conjuncts)
        .Done().Ptr();

    if (pgSyntax) {
        conjunction = ctx->Builder(pos).Callable("ToPg").Add(0, conjunction).Seal().Build();
    }

    auto lambda = Build<TCoLambda>(*ctx, pos)
        .Args({lambda_arg})
        .Body(conjunction)
        .Done().Ptr();
    // clang-format on

    return TExpression(lambda, ctx, props);
}

TExpression MakeNegation(const TExpression& expr) {
    Y_ENSURE(expr.Ctx);
    Y_ENSURE(expr.PlanProps);

    // clang-format off
    auto negation = Build<TCoNot>(*expr.Ctx, expr.Node->Pos())
        .Value(expr.GetExpressionBody())
        .Done().Ptr();
    // clang-format on

    return TExpression(negation, expr.Ctx, expr.PlanProps);
}

TExpression MakeBinaryPredicate(const TString& callable, const TExpression& left, const TExpression& right) {
    // Fetch context and plan properties from one of the arguments
    TExprContext* ctx = nullptr;
    TPlanProps* props = nullptr;

    if (left.Ctx) {
        ctx = left.Ctx;
    }
    if (left.PlanProps) {
        props = left.PlanProps;
    }
    if (right.Ctx) {
        ctx = right.Ctx;
    }
    if (right.PlanProps) {
        props = right.PlanProps;
    }

    auto pos = left.Node->Pos();

    Y_ENSURE(ctx);
    Y_ENSURE(props);

    auto lambda = ctx->NewCallable(pos, callable, {left.GetExpressionBody(), right.GetExpressionBody()});
    return TExpression(lambda, ctx, props);
}

TExpression MakeUnaryCallable(const TString& callable, const TExpression& arg) {
    Y_ENSURE(arg.Ctx);

    auto node = arg.Ctx->NewCallable(arg.Node->Pos(), callable, {arg.GetExpressionBody()});
    return TExpression(node, arg.Ctx, arg.PlanProps);
}

TExpression MakeEnsure(const TExpression& value, const TExpression& predicate, const TString& message) {
    // Fetch context and plan properties from one of the arguments
    TExprContext* ctx = nullptr;
    TPlanProps* props = nullptr;

    for (const auto* expr : {&value, &predicate}) {
        if (expr->Ctx) {
            ctx = expr->Ctx;
        }
        if (expr->PlanProps) {
            props = expr->PlanProps;
        }
    }

    Y_ENSURE(ctx);
    Y_ENSURE(props);

    auto pos = value.Node->Pos();
    auto messageNode = ctx->NewCallable(pos, "String", {ctx->NewAtom(pos, message)});
    auto ensure = ctx->NewCallable(pos, "Ensure", {value.GetExpressionBody(), predicate.GetExpressionBody(), messageNode});
    return TExpression(ensure, ctx, props);
}

TInfoUnitId GetMemberId(const TExprNode& member) {
    const auto id = FromString<TInfoUnitId>(member.Tail().Content());
    Y_ENSURE(id != TUnorderedIUs::InvalidBit, "Invalid IU ID");
    return id;
}

void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit> &IUs) {
    if (node->IsCallable("Member")) {
        auto member = TCoMember(node);
        IUs.push_back(TInfoUnit(member.Name().StringValue()));
        return;
    }

    for (auto c : node->Children()) {
        GetAllMembers(c, IUs);
    }
}

} // namespace NKikimr::NKqp
