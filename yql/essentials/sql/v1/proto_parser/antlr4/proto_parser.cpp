#include "proto_parser.h"
#include <yql/essentials/parser/proto_ast/antlr4/proto_ast_antlr4.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NSQLTranslationV1 {

namespace {

class TParser: public NSQLTranslation::IParser {
public:
    TParser(bool isAmbiguityError, bool isAmbiguityDebugging, TMaybe<size_t> maxParseTreeDepth)
        : IsAmbiguityError_(isAmbiguityError)
        , IsAmbiguityDebugging_(isAmbiguityDebugging)
        , MaxParseTreeDepth_(maxParseTreeDepth)
    {
    }

    google::protobuf::Message* Parse(
        const TString& query, const TString& queryName, NProtoAST::IErrorCollector& err,
        google::protobuf::Arena* arena) final {
        YQL_ENSURE(arena);
        NProtoAST::TProtoASTBuilder4<
            NALPDefaultAntlr4::SQLv1Antlr4Parser,
            NALPDefaultAntlr4::SQLv1Antlr4Lexer>
            builder(query, queryName, arena, IsAmbiguityError_, IsAmbiguityDebugging_, MaxParseTreeDepth_);
        return builder.BuildAST(err);
    }

private:
    const bool IsAmbiguityError_;
    const bool IsAmbiguityDebugging_;
    const TMaybe<size_t> MaxParseTreeDepth_;
};

class TFactory: public NSQLTranslation::IParserFactory {
public:
    TFactory(bool isAmbiguityError, bool isAmbiguityDebugging, TMaybe<size_t> maxParseTreeDepth)
        : IsAmbiguityError_(isAmbiguityError)
        , IsAmbiguityDebugging_(isAmbiguityDebugging)
        , MaxParseTreeDepth_(maxParseTreeDepth)
    {
    }

    std::unique_ptr<NSQLTranslation::IParser> MakeParser() const final {
        return std::make_unique<TParser>(IsAmbiguityError_, IsAmbiguityDebugging_, MaxParseTreeDepth_);
    }

private:
    const bool IsAmbiguityError_;
    const bool IsAmbiguityDebugging_;
    const TMaybe<size_t> MaxParseTreeDepth_;
};

} // namespace

NSQLTranslation::TParserFactoryPtr MakeAntlr4ParserFactory(
    bool isAmbiguityError,
    bool isAmbiguityDebugging,
    TMaybe<size_t> maxParseTreeDepth)
{
    return MakeIntrusive<TFactory>(isAmbiguityError, isAmbiguityDebugging, maxParseTreeDepth);
}

} // namespace NSQLTranslationV1
