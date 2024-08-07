#include "proto_parser.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/parser/proto_ast/collect_issues/collect_issues.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Parser.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_ansi/SQLv1Lexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_ansi/SQLv1Parser.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#include <ydb/library/yql/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif

using namespace NYql;

namespace NSQLTranslationV1 {


#if defined(_tsan_enabled_)
    TMutex SanitizerSQLTranslationMutex;
#endif

using namespace NSQLv1Generated;

google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, TIssues& err, size_t maxErrors, bool ansiLexer, bool anlr4Parser, google::protobuf::Arena* arena) {
    YQL_ENSURE(arena);
#if defined(_tsan_enabled_)
    TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
    NSQLTranslation::TErrorCollectorOverIssues collector(err, maxErrors, "");
    if (ansiLexer && !anlr4Parser) {
        NProtoAST::TProtoASTBuilder<NALPAnsi::SQLv1Parser, NALPAnsi::SQLv1Lexer> builder(query, queryName, arena);
        return builder.BuildAST(collector);
    } else if (!ansiLexer && !anlr4Parser) {
        NProtoAST::TProtoASTBuilder<NALPDefault::SQLv1Parser, NALPDefault::SQLv1Lexer> builder(query, queryName, arena);
        return builder.BuildAST(collector);
    } else if (ansiLexer && anlr4Parser) {
        NProtoAST::TProtoASTBuilder<NALPAnsi::SQLv1Antlr4Parser, NALPAnsi::SQLv1Antlr4Lexer> builder(query, queryName, arena);
        return builder.BuildAST(collector);
    } else {
        NProtoAST::TProtoASTBuilder<NALPDefault::SQLv1Antlr4Parser, NALPDefault::SQLv1Antlr4Lexer> builder(query, queryName, arena);
        return builder.BuildAST(collector);
    }
}

google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NProtoAST::IErrorCollector& err, bool ansiLexer, bool anlr4Parser, google::protobuf::Arena* arena) {
    YQL_ENSURE(arena);
#if defined(_tsan_enabled_)
    TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
    if (ansiLexer && !anlr4Parser) {
        NProtoAST::TProtoASTBuilder<NALPAnsi::SQLv1Parser, NALPAnsi::SQLv1Lexer> builder(query, queryName, arena);
        return builder.BuildAST(err);
    } else if (!ansiLexer && !anlr4Parser) {
        NProtoAST::TProtoASTBuilder<NALPDefault::SQLv1Parser, NALPDefault::SQLv1Lexer> builder(query, queryName, arena);
        return builder.BuildAST(err);
    } else if (ansiLexer && anlr4Parser) {
        NProtoAST::TProtoASTBuilder<NALPAnsi::SQLv1Antlr4Parser, NALPAnsi::SQLv1Antlr4Lexer> builder(query, queryName, arena);
        return builder.BuildAST(err);
    } else {
        NProtoAST::TProtoASTBuilder<NALPDefault::SQLv1Antlr4Parser, NALPDefault::SQLv1Antlr4Lexer> builder(query, queryName, arena);
        return builder.BuildAST(err);
    }
}

} // namespace NSQLTranslationV1
