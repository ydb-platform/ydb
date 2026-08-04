#include <yql/essentials/ast/serialize/yql_expr_serialize.h>
#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/string.h>

namespace {

bool IsWhitespace(char c) {
    return c == ' ' || c == '\n' || c == '\r' || c == '\t';
}

bool EndsWithDanglingQuoteElement(TStringBuf input) {
    if (input.size() < 2 || input.back() != '\'') {
        return false;
    }

    // This target starts after successful AST parsing. Keep malformed raw AST
    // parser aborts in yql_ast_parse_roundtrip, not in expr compile triage.
    const char prev = input[input.size() - 2];
    return prev == '(' || prev == ')' || IsWhitespace(prev);
}

void ExerciseExprRoundTrip(const NYql::TExprNode& exprRoot, NYql::TExprContext& exprCtx) {
    NYql::CheckArguments(exprRoot);
    NYql::CheckCounts(exprRoot);

    auto ast = NYql::ConvertToAst(exprRoot, exprCtx, 0, true);
    if (ast.Root) {
        const TString printed = ast.Root->ToString(
            NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote | NYql::TAstPrintFlags::AdaptArbitraryContent);
        auto reparsed = NYql::ParseAst(printed);
        if (reparsed.Root) {
            (void)reparsed.Root->ToString();
        }
    }

    const TString serialized = NYql::SerializeGraph(
        exprRoot,
        exprCtx,
        NYql::TSerializedExprGraphComponents::Graph | NYql::TSerializedExprGraphComponents::Positions);
    NYql::TExprContext roundTripCtx;
    auto deserialized = NYql::DeserializeGraph(NYql::TPosition{}, serialized, roundTripCtx);
    if (deserialized) {
        NYql::CheckArguments(*deserialized);
        NYql::CheckCounts(*deserialized);
        auto ast2 = NYql::ConvertToAst(*deserialized, roundTripCtx, 0, true);
        if (ast2.Root) {
            (void)ast2.Root->ToString();
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    const TString input(reinterpret_cast<const char*>(data), size);
    if (EndsWithDanglingQuoteElement(input)) {
        return 0;
    }

    try {
        auto ast = NYql::ParseAst(input);
        if (!ast.Root) {
            return 0;
        }

        NYql::TExprContext exprCtx;
        NYql::TExprNode::TPtr exprRoot;
        if (!NYql::CompileExpr(*ast.Root, exprRoot, exprCtx, nullptr, nullptr, false) || !exprRoot) {
            return 0;
        }

        ExerciseExprRoundTrip(*exprRoot, exprCtx);
    } catch (...) {
    }

    return 0;
}
