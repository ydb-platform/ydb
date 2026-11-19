#pragma once

#include <ydb/tests/tools/parsed_query_bench/proto/execute_query.pb.h>
#include <yql/essentials/ast/yql_ast.h>

namespace NKikimr::NKqp::NParsedQueryBench {

class TEngine;
using TRequest = Ydb::Query::Benchmark::ExecuteQueryRequest;

NYql::TAstParseResult TranslateText(const TString& sql, const TRequest& envelope, const TEngine& engine);
void ExportGrammar(const TString& sql, const TRequest& envelope, const TEngine& engine,
    Ydb::Query::MainSqlV1ParserAst& output);
NYql::TAstParseResult TranslateGrammar(const Ydb::Query::MainSqlV1ParserAst& input,
    const TRequest& envelope, const TEngine& engine);
void ExportAst(const NYql::TAstNode& root, Ydb::Query::YqlAst& output);
NYql::TAstParseResult ImportAst(const Ydb::Query::YqlAst& input);

} // namespace NKikimr::NKqp::NParsedQueryBench
