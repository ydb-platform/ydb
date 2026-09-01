#include "generated_column.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <util/generic/hash_set.h>
#include <util/string/ascii.h>

namespace NYql {

TString AssembleGeneratedQuery(const TString& context, const TString& exprBody) {
    return TStringBuilder() << context << "SELECT " << exprBody << " FROM `__yql_generated_column_source`;";
}

namespace {

using namespace NNodes;

const NSQLTranslation::TParsedToken* FindFirstMeaningfulToken(const NSQLTranslation::TParsedTokenList& tokens) {
    for (const auto& token : tokens) {
        if (token.Name != "WS" && token.Name != "COMMENT" && token.Name != "EOF") {
            return &token;
        }
    }
    return nullptr;
}

bool DropParameterDeclarations(const TString& sqlText, const NSQLTranslationV1::TLexers& lexers,
    bool ansiLexer, TString& result, TExprContext& ctx)
{
    auto lexer = NSQLTranslationV1::MakeLexer(lexers, ansiLexer);

    TIssues issues;
    TVector<TString> statements;
    if (!NSQLTranslationV1::SplitQueryToStatements(sqlText, lexer, statements, issues)) {
        ctx.IssueManager.AddIssues(issues);
        return false;
    }

    TStringBuilder kept;
    for (const auto& statement : statements) {
        NSQLTranslation::TParsedTokenList tokens;
        if (!NSQLTranslation::Tokenize(*lexer, statement, "", tokens, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
            ctx.IssueManager.AddIssues(issues);
            return false;
        }

        const auto* first = FindFirstMeaningfulToken(tokens);
        if (first && AsciiEqualsIgnoreCase(first->Content, "declare")) {
            continue;
        }

        kept << statement << '\n';
    }

    result = kept;
    return true;
}

TExprNode::TPtr CompileText(const TString& sqlText, TExprContext& ctx, NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder,
    const IModuleResolver::TPtr& moduleResolver)
{
    auto translationSettings = settingsBuilder.Build(ctx);
    translationSettings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();

    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(settingsBuilder.GetIsAmbiguityError());
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

    TString query;
    if (!DropParameterDeclarations(sqlText, lexers, translationSettings.AnsiLexer, query, ctx)) {
        return nullptr;
    }

    NSQLTranslation::TTranslators translators(nullptr, NSQLTranslationV1::MakeTranslator(lexers, parsers), nullptr);

    auto queryAst = NSQLTranslation::SqlToYql(translators, query, translationSettings);
    ctx.IssueManager.AddIssues(queryAst.Issues);
    if (!queryAst.IsOk()) {
        return nullptr;
    }

    TExprNode::TPtr queryGraph;
    if (!CompileExpr(*queryAst.Root, queryGraph, ctx, moduleResolver.get(), nullptr)) {
        return nullptr;
    }

    return queryGraph;
}

const THashSet<TStringBuf>& NonRowCallables() {
    static const THashSet<TStringBuf> callables = {
        "TablePath",
        "TableName",
        "TableRecord",
        "TableRow",
        "JoinTableRow",
        "SystemMetadata",
        "FilePath",
        "FileContent",
        "FolderPath",
        "Files",
        "EvaluateAtom",
        "EvaluateExpr",
        "EvaluateType",
        "EvaluateCode",
        "CurrentOperationId",
        "CurrentOperationSharedId",
        "CurrentAuthenticatedUser",
        "CurrentLanguageVersion",
        "SecureParam",
    };
    return callables;
}

const THashSet<TStringBuf>& ReadsDataCallables() {
    static const THashSet<TStringBuf> callables = {
        "Read!",
        "Left!",
        "Right!",
        "Cons!",
        "WithWorld",
        "Write!",
        "Commit!",
        "Configure!",
        "Sync!",
        "DataSource",
        "DataSink",
        "HasItems",
    };
    return callables;
}

const THashSet<TStringBuf>& AggregationExtraCallables() {
    static const THashSet<TStringBuf> callables = {
        "AggOverState",
        "MultiAggregate",
        "SqlAggregateAll",
    };
    return callables;
}

const THashSet<TStringBuf>& WindowExtraCallables() {
    static const THashSet<TStringBuf> callables = {
        "CalcOverWindowGroup",
        "RowNumber",
        "Rank",
        "DenseRank",
        "PercentRank",
        "CumeDist",
        "NTile",
        "Lead",
        "Lag",
        "SessionWindowTraits",
        "HoppingTraits",
    };
    return callables;
}

bool IsAggregationCallable(const TStringBuf name) {
    return name.StartsWith("Aggregate") || name.StartsWith("Aggregation") || name.StartsWith("AggApply");
}

bool IsWindowCallable(const TStringBuf name) {
    return name.StartsWith("CalcOver") || name.StartsWith("WinOn");
}

bool IsReadsDataNode(const TExprNode& node) {
    return TCoRight::Match(&node) || TCoLeft::Match(&node) || TCoCons::Match(&node)
        || TCoRead::Match(&node) || TCoWrite::Match(&node) || TCoCommit::Match(&node)
        || TCoDataSource::Match(&node) || TCoDataSink::Match(&node) || TCoSync::Match(&node)
        || TCoConfigure::Match(&node) || TCoWithWorld::Match(&node) || TCoHasItems::Match(&node)
        || ReadsDataCallables().contains(node.Content());
}

bool IsAggregationNode(const TExprNode& node) {
    return TCoAggregateBase::Match(&node) || TCoAggApplyBase::Match(&node)
        || TCoAggregationTraits::Match(&node) || TCoAggOverState::Match(&node)
        || IsAggregationCallable(node.Content()) || AggregationExtraCallables().contains(node.Content());
}

bool IsWindowNode(const TExprNode& node) {
    return TCoCalcOverWindowBase::Match(&node) || TCoCalcOverWindowGroup::Match(&node)
        || TCoWinOnBase::Match(&node) || TCoSessionWindowTraits::Match(&node)
        || IsWindowCallable(node.Content()) || WindowExtraCallables().contains(node.Content());
}

bool IsNonRowNode(const TExprNode& node) {
    return TCoTablePropBase::Match(&node) || TCoSecureParam::Match(&node)
        || NonRowCallables().contains(node.Content());
}

struct TGeneratedFindings {
    ui32 Reads = 0;
    ui32 ProjectionItems = 0;
    bool HasStar = false;
    bool ReadsData = false;
    bool HasAggregation = false;
    bool HasWindow = false;
    bool HasParameter = false;
    TStringBuf NonRowCallable;
    TExprNode::TPtr ProjectionLambda;
};

TGeneratedFindings CollectFindings(const TExprNode::TPtr& root) {
    TGeneratedFindings findings;

    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (!node->IsCallable()) {
            return true;
        }

        const TStringBuf name = node->Content();
        if (name == "SqlProjectStarItem") {
            findings.HasStar = true;
            return true;
        }

        if (name == "SqlProjectItem") {
            ++findings.ProjectionItems;
            findings.ProjectionLambda = node->ChildPtr(2);
            return true;
        }

        if (name == "Read!") {
            ++findings.Reads;
        }

        if (IsReadsDataNode(*node)) {
            findings.ReadsData = true;
        } else if (IsAggregationNode(*node)) {
            findings.HasAggregation = true;
        } else if (IsWindowNode(*node)) {
            findings.HasWindow = true;
        } else if (TCoParameter::Match(node.Get())) {
            findings.HasParameter = true;
        } else if (IsNonRowNode(*node)) {
            if (findings.NonRowCallable.empty()) {
                findings.NonRowCallable = name;
            }
        }

        return true;
    });

    return findings;
}

bool UsesWholeRow(const TExprNode& node, const TExprNode* rowArg, TNodeSet& visited) {
    if (&node == rowArg) {
        return true;
    }

    if (!visited.insert(&node).second) {
        return false;
    }

    if (node.IsCallable("Member") && node.ChildrenSize() == 2 && node.Child(0) == rowArg && node.Child(1)->IsAtom()) {
        return false;
    }

    for (const auto& child : node.Children()) {
        if (UsesWholeRow(*child, rowArg, visited)) {
            return true;
        }
    }

    return false;
}

bool EmitOutOfRowError(const TGeneratedFindings& findings, bool readsDataIsSubquery,
    const TString& columnName, TExprContext& ctx, TPositionHandle pos)
{
    const auto rejectDependency = [&](const TStringBuf dependency) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
            << "Generated column " << columnName << " expression must depend only on the row being written,"
            << " but it uses " << dependency));
    };

    if (readsDataIsSubquery && findings.ReadsData) {
        rejectDependency("a subquery");
        return true;
    }

    if (findings.HasWindow) {
        rejectDependency("a window function");
        return true;
    }

    if (findings.HasAggregation) {
        rejectDependency("an aggregate function");
        return true;
    }

    if (findings.HasParameter) {
        rejectDependency("a query parameter");
        return true;
    }

    if (!findings.NonRowCallable.empty()) {
        rejectDependency(findings.NonRowCallable);
        return true;
    }

    return false;
}

}   // namespace

TExprNode::TPtr CompileGeneratedExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder, const IModuleResolver::TPtr& moduleResolver)
{
    auto queryGraph = CompileText(sqlText, ctx, settingsBuilder, moduleResolver);
    if (!queryGraph) {
        ctx.AddError(TIssue(TStringBuilder() << "Failed to compile the expression of generated column " << columnName));
        return nullptr;
    }

    auto checks = CollectFindings(queryGraph);

    if (checks.HasStar || checks.Reads > 1 || checks.ProjectionItems > 1) {
        ctx.AddError(TIssue(ctx.GetPosition(queryGraph->Pos()), TStringBuilder()
            << "Generated column " << columnName << " expression must depend only on the row being written,"
            << " but it uses a subquery"));
        return nullptr;
    }

    if (checks.ProjectionItems != 1 || !checks.ProjectionLambda || checks.ProjectionLambda->Type() != TExprNode::Lambda) {
        ctx.AddError(TIssue(ctx.GetPosition(queryGraph->Pos()), TStringBuilder()
            << "Generated column " << columnName << " must be defined by a single scalar expression"));
        return nullptr;
    }

    if (EmitOutOfRowError(checks, /* readsDataIsSubquery */ false, columnName, ctx, queryGraph->Pos())) {
        return nullptr;
    }

    const TGeneratedFindings body = CollectFindings(checks.ProjectionLambda->TailPtr());
    if (EmitOutOfRowError(body, /* readsDataIsSubquery */ true, columnName, ctx, checks.ProjectionLambda->Pos())) {
        return nullptr;
    }

    TNodeSet visited;
    if (checks.ProjectionLambda->Head().ChildrenSize() != 1
        || UsesWholeRow(checks.ProjectionLambda->Tail(), &checks.ProjectionLambda->Head().Head(), visited))
    {
        ctx.AddError(TIssue(ctx.GetPosition(checks.ProjectionLambda->Pos()), TStringBuilder()
            << "Generated column " << columnName << " expression must reference columns by name,"
            << " but it uses the whole row (for example TableRow() or JoinTableRow())"));
        return nullptr;
    }

    return checks.ProjectionLambda;
}

}   // namespace NYql
