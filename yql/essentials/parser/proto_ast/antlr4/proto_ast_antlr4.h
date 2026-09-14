#pragma once

#include <yql/essentials/parser/common/error.h>
#include <yql/essentials/parser/common/antlr4/depth_limiting_listener.h>
#include <yql/essentials/parser/common/antlr4/error_listener.h>
#include <yql/essentials/parser/common/antlr4/lexer_tokens_collector.h>

#include <yql/essentials/parser/proto_ast/common.h>

#ifdef ERROR
    #undef ERROR
#endif
#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>

#include <util/generic/maybe.h>
#include <util/generic/scope.h>

namespace NProtoAST {
using namespace NAST;

template <typename InputType>
void InvalidCharacter(IOutputStream& err, const InputType* input);

template <typename TokenType>
inline void InvalidToken(IOutputStream& err, const TokenType* token);

template <>
inline void InvalidToken<antlr4::Token>(IOutputStream& err, const antlr4::Token* token) {
    if (token) {
        if (token->getInputStream()) {
            err << " '" << token->getText() << "'";
        } else {
            err << ABSENCE;
        }
    }
}

template <typename TParser, typename TLexer>
class TProtoASTBuilder4 {
public:
    explicit TProtoASTBuilder4(
        TStringBuf data,
        TString queryName = "query",
        google::protobuf::Arena* arena = nullptr,
        bool isAmbiguityError = false,
        bool isAmbiguityDebugging = false,
        TMaybe<size_t> maxParseTreeDepth = Nothing())
        : QueryName_(std::move(queryName))
        , IsAmbiguityError_(isAmbiguityError)
        , MaxParseTreeDepth_(maxParseTreeDepth)
        , InputStream_(data)
        , Lexer_(&InputStream_)
        , TokenStream_(&Lexer_)
        , Parser_(&TokenStream_, arena)
    {
        if (isAmbiguityDebugging) {
            Parser_
                .template getInterpreter<antlr4::atn::ParserATNSimulator>()
                ->setPredictionMode(antlr4::atn::PredictionMode::LL_EXACT_AMBIG_DETECTION);
        }
    }

    google::protobuf::Message* BuildAST(IErrorCollector& errors) {
        Parser_.removeErrorListeners();

        // TODO: find a better way to break on lexer errors
        typename antlr4::YqlErrorListener errorListener(&errors, &Parser_.error, IsAmbiguityError_);
        Parser_.addErrorListener(&errorListener);
        Y_DEFER {
            Parser_.removeErrorListener(&errorListener);
        };

        TMaybe<NAntlrAST::TDepthLimitingListener> depthLimiter;
        if (MaxParseTreeDepth_) {
            depthLimiter.ConstructInPlace(*MaxParseTreeDepth_);
            Parser_.addParseListener(depthLimiter.Get());
        }
        Y_DEFER {
            if (depthLimiter) {
                Parser_.removeParseListener(depthLimiter.Get());
            }
        };

        try {
            auto result = Parser_.Parse(&errors);
            if (Parser_.error) {
                result = nullptr;
            }
            Parser_.error = false;
            return result;
        } catch (const TTooManyErrors&) {
            Parser_.error = false;
            return nullptr;
        } catch (...) {
            errors.Error(0, 0, CurrentExceptionMessage());
            Parser_.error = false;
            return nullptr;
        }
    }

private:
    TString QueryName_;
    bool IsAmbiguityError_;
    TMaybe<size_t> MaxParseTreeDepth_;

    antlr4::ANTLRInputStream InputStream_;
    TLexer Lexer_;

    antlr4::CommonTokenStream TokenStream_;
    TParser Parser_;
};

} // namespace NProtoAST
