#include "generated_column.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NYql {

namespace {

using namespace NNodes;

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

    NSQLTranslation::TTranslators translators(nullptr, NSQLTranslationV1::MakeTranslator(lexers, parsers), nullptr);

    auto queryAst = NSQLTranslation::SqlToYql(translators, sqlText, translationSettings);
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

const THashSet<TStringBuf>& AllowedGeneratedCallables() {
    static const THashSet<TStringBuf> callables = {
        // Type expressions used by constructors and casts.
        "DataType",
        "DictType",
        "ListType",
        "OptionalType",
        "PgType",
        "StructType",
        "TaggedType",
        "TupleType",
        "VariantType",
        "VoidType",

        // Values, optionals and row-local containers.
        "AsDict",
        "AsList",
        "AsStruct",
        "AsTagged",
        "AsVariant",
        "Dict",
        "Enum",
        "Just",
        "JsonVariables",
        "List",
        "Nothing",
        "Null",
        "Struct",
        "ToList",
        "ToOptional",
        "Untag",
        "Variant",
        "Void",

        // Scalar operations.
        "!=",
        "%",
        "*",
        "+",
        "-",
        "/",
        "<",
        "<=",
        "==",
        ">",
        ">=",
        "Abs",
        "AddTimezone",
        "And",
        "BitCast",
        "BitNot",
        "ByteAt",
        "CheckedAdd",
        "CheckedDiv",
        "CheckedMinus",
        "CheckedMod",
        "CheckedMul",
        "CheckedSub",
        "Coalesce",
        "Concat",
        "Contains",
        "Convert",
        "CountBits",
        "Dec",
        "Default",
        "EndsWith",
        "EndsWithIgnoreCase",
        "EqualsIgnoreCase",
        "Exists",
        "Find",
        "FromString",
        "If",
        "IfPresent",
        "IfStrict",
        "Inc",
        "IsDistinctFrom",
        "Length",
        "Max",
        "Member",
        "Min",
        "Minus",
        "Mod",
        "Mul",
        "Not",
        "Nth",
        "Or",
        "Plus",
        "RFind",
        "RemoveTimezone",
        "SafeCast",
        "ShiftLeft",
        "ShiftRight",
        "Size",
        "SqlConcat",
        "SqlIn",
        "StartsWith",
        "StartsWithIgnoreCase",
        "StringContains",
        "StringContainsIgnoreCase",
        "Sub",
        "Substring",
        "ToBytes",
        "ToString",
        "Xor",

        // Deterministic, order-preserving transformations of collections
        // constructed from the current row. Lambdas and their bodies are
        // validated recursively.
        "Append",
        "DictFromKeys",
        "Enumerate",
        "FlatListIf",
        "FlatOptionalIf",
        "Fold",
        "Fold1",
        "Head",
        "Insert",
        "Last",
        "ListIf",
        "Lookup",
        "OptionalIf",
        "OrderedExtend",
        "OrderedExtract",
        "OrderedFilter",
        "OrderedFlatMap",
        "OrderedFlatMapWarn",
        "OrderedMap",
        "Prepend",
        "Reverse",
        "Skip",
        "Take",
        "UniqStable",
        "Zip",
        "ZipAll",

        // Pure structural and optimizer-only wrappers.
        "AddMember",
        "DependsOn",
        "ForceRemoveMember",
        "Guess",
        "IfType",
        "InnerDependsOn",
        "Likely",
        "MatchType",
        "NoPush",
        "RemoveMember",
        "ReplaceMember",
        "TypeOf",
        "Unessential",
        "VariantItem",
        "Visit",
    };
    return callables;
}

bool IsAllowedGeneratedUdf(const TExprNode& udf) {
    if (!TCoUdf::Match(&udf)
        || udf.ChildrenSize() <= TCoUdf::idx_FileAlias
        || !udf.Head().IsAtom())
    {
        return false;
    }

    // A non-empty file alias denotes a user-supplied UDF. Being linked into
    // the server is necessary, but not sufficient: every function below is
    // still audited and listed by its normalized name.
    const auto* fileAlias = udf.Child(TCoUdf::idx_FileAlias);
    if (!fileAlias->IsAtom() || !fileAlias->Content().empty()) {
        return false;
    }

    return udf.Head().Content() == "Unicode.ToLower";
}

bool IsSafeOptionalMap(const TExprNode& node) {
    // UDF AutoMap uses the unordered spelling even for an Optional input.
    // Such an input has at most one item, so its result has no observable
    // ordering ambiguity. Map over a List/Stream remains default-denied.
    return node.IsCallable("Map")
        && node.ChildrenSize() == 2
        && node.Head().GetTypeAnn()
        && node.Head().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional;
}

bool HasLiteralJsonPath(const TExprNode& node) {
    return node.ChildrenSize() > 1 && node.Child(1)->IsCallable("Utf8");
}

bool IsSafeJsonCallable(const TExprNode& node) {
    if (!HasLiteralJsonPath(node)) {
        return false;
    }

    if (TCoJsonExists::Match(&node)) {
        // The absent fourth child represents ERROR ON ERROR.
        return node.ChildrenSize() == 4;
    }

    if (TCoJsonQuery::Match(&node)) {
        return node.ChildrenSize() == 6
            && node.Child(TCoJsonQuery::idx_OnEmpty)->IsAtom()
            && node.Child(TCoJsonQuery::idx_OnEmpty)->Content() != "Error"
            && node.Child(TCoJsonQuery::idx_OnError)->IsAtom()
            && node.Child(TCoJsonQuery::idx_OnError)->Content() != "Error";
    }

    if (TCoJsonValue::Match(&node)) {
        if (node.ChildrenSize() < 7
            || !node.Child(TCoJsonValue::idx_OnEmptyMode)->IsAtom()
            || node.Child(TCoJsonValue::idx_OnEmptyMode)->Content() == "Error"
            || !node.Child(TCoJsonValue::idx_OnErrorMode)->IsAtom()
            || node.Child(TCoJsonValue::idx_OnErrorMode)->Content() == "Error")
        {
            return false;
        }

        const auto& onError = *node.Child(TCoJsonValue::idx_OnError);
        if (onError.IsCallable("Null")) {
            return true;
        }

        // A failed cast of DEFAULT ON ERROR is lowered to Ensure(false).
        // Accept the handler only when that cast is complete for every value.
        return onError.GetTypeAnn() && node.GetTypeAnn()
            && CastResult<true>(onError.GetTypeAnn(), node.GetTypeAnn()) == NUdf::ECastOptions::Complete;
    }

    return false;
}

class TGeneratedExprValidator {
public:
    TGeneratedExprValidator(const TString& columnName, TExprContext& ctx)
        : ColumnName_(columnName)
        , Ctx_(ctx)
    {
    }

    bool Validate(const TExprNode& node) {
        if (!Visited_.insert(&node).second) {
            return true;
        }

        if (node.IsWorld()) {
            return Reject(node, "World");
        }

        if (!node.IsCallable()) {
            return ValidateChildren(node);
        }

        if (TCoApply::Match(&node)) {
            return ValidateApply(node);
        }

        if (IsSafeOptionalMap(node)) {
            // Its lambda body is checked by ValidateChildren below.
        } else if (TCoJsonQueryBase::Match(&node)) {
            if (!IsSafeJsonCallable(node)) {
                return Reject(node);
            }
        } else if (!TCoDataCtor::Match(&node) && !AllowedGeneratedCallables().contains(node.Content())) {
            return Reject(node);
        }

        return ValidateChildren(node);
    }

private:
    bool ValidateApply(const TExprNode& node) {
        if (node.ChildrenSize() == 0) {
            return Reject(node);
        }

        const auto& callable = node.Head();
        if (TCoUdf::Match(&callable)) {
            if (!IsAllowedGeneratedUdf(callable)) {
                return Reject(callable, callable.ChildrenSize() && callable.Head().IsAtom()
                    ? callable.Head().Content()
                    : callable.Content());
            }

            if (callable.ChildrenSize() > TCoUdf::idx_RunConfigValue
                && !Validate(*callable.Child(TCoUdf::idx_RunConfigValue)))
            {
                return false;
            }
        } else if (!callable.IsLambda()) {
            return Reject(node);
        } else if (!Validate(callable)) {
            return false;
        }

        for (ui32 i = 1; i < node.ChildrenSize(); ++i) {
            if (!Validate(*node.Child(i))) {
                return false;
            }
        }
        return true;
    }

    bool ValidateChildren(const TExprNode& node) {
        for (const auto& child : node.Children()) {
            if (!Validate(*child)) {
                return false;
            }
        }
        return true;
    }

    bool Reject(const TExprNode& node, TStringBuf callable = {}) {
        if (callable.empty()) {
            callable = node.Content();
        }
        Ctx_.AddError(TIssue(Ctx_.GetPosition(node.Pos()), TStringBuilder()
            << "Callable " << callable << " is not allowed in a generated column expression"
            << " for column " << ColumnName_));
        return false;
    }

private:
    const TString& ColumnName_;
    TExprContext& Ctx_;
    TNodeSet Visited_;
};

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

} // namespace

TString AssembleGeneratedQuery(const TString& exprBody) {
    return TStringBuilder() << "SELECT " << exprBody << " FROM `__yql_generated_column_source`;";
}

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

bool ValidateGeneratedExpr(const TExprNode& lambda, const TString& columnName, TExprContext& ctx) {
    return TGeneratedExprValidator(columnName, ctx).Validate(lambda);
}

}   // namespace NYql
