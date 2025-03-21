#include "proto_parser.h"
#include <yql/essentials/parser/proto_ast/antlr3/proto_ast_antlr3.h>
#include <yql/essentials/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1/SQLv1Parser.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NSQLTranslationV1 {

namespace {

class TParser : public NSQLTranslation::IParser {
public:
    google::protobuf::Message* Parse(
    const TString& query, const TString& queryName, NProtoAST::IErrorCollector& err,
        google::protobuf::Arena* arena) final {
        YQL_ENSURE(arena);
        NProtoAST::TProtoASTBuilder3<NALPDefault::SQLv1Parser, NALPDefault::SQLv1Lexer> builder(query, queryName, arena);
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

NSQLTranslation::TParserFactoryPtr MakeAntlr3ParserFactory() {
    return MakeIntrusive<TFactory>();
}

}
