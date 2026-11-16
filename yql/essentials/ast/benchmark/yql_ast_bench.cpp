#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/test_support/yql_ast_test_program.h>

#include <benchmark/benchmark.h>

#include <util/system/yassert.h>

namespace {

void ParseAst(benchmark::State& state) {
    for (auto _ : state) {
        NYql::TAstParseResult result = NYql::ParseAst(NYql::TEST_PROGRAM);
        Y_ENSURE(result.IsOk(), "AST parsing failed");
        Y_ENSURE(result.Root->IsList(), "AST root must be a list");
        Y_ENSURE(result.Issues.Empty(), "AST parsing produced issues");
        benchmark::DoNotOptimize(result.Root);
    }
}

} // namespace

BENCHMARK(ParseAst);
