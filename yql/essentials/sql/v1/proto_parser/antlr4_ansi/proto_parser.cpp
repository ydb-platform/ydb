#include "proto_parser.h"
#include <yql/essentials/parser/proto_ast/antlr4/proto_ast_antlr4.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NSQLTranslationV1 {

namespace {

class TParser : public NSQLTranslation::IParser {
public:
    google::protobuf::Message* Parse(
    const TString& query, const TString& queryName, NProtoAST::IErrorCollector& err,
        google::protobuf::Arena* arena) final {
        YQL_ENSURE(arena);
        NProtoAST::TProtoASTBuilder4<NALPAnsiAntlr4::SQLv1Antlr4Parser, NALPAnsiAntlr4::SQLv1Antlr4Lexer> builder(query, queryName, arena);
        return builder.BuildAST(err);
    }
};

class TFactory: public NSQLTranslation::IParserFactory {
public:
    std::unique_ptr<NSQLTranslation::IParser> MakeParser() const final {
        return std::make_unique<TParser>();
    }
};

}

NSQLTranslation::TParserFactoryPtr MakeAntlr4AnsiParserFactory() {
    return MakeIntrusive<TFactory>();
}

}
