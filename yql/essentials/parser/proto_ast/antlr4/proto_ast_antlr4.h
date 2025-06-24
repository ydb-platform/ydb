#pragma once

#include <yql/essentials/parser/common/error.h>
#include <yql/essentials/parser/common/antlr4/error_listener.h>
#include <yql/essentials/parser/common/antlr4/lexer_tokens_collector.h>

#include <yql/essentials/parser/proto_ast/common.h>

#ifdef ERROR
#undef ERROR
#endif
#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>

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
        TProtoASTBuilder4(TStringBuf data, const TString& queryName = "query", google::protobuf::Arena* arena = nullptr)
            : QueryName_(queryName)
            , InputStream_(data)
            , Lexer_(&InputStream_)
            , TokenStream_(&Lexer_)
            , Parser_(&TokenStream_, arena)
        {
        }

        google::protobuf::Message* BuildAST(IErrorCollector& errors) {
            // TODO: find a better way to break on lexer errors
            typename antlr4::YqlErrorListener listener(&errors, &Parser_.error);
            Parser_.removeErrorListeners();
            Parser_.addErrorListener(&listener);
            try {
                auto result = Parser_.Parse(&errors);
                Parser_.removeErrorListener(&listener);
                Parser_.error = false;
                return result;
            } catch (const TTooManyErrors&) {
                Parser_.removeErrorListener(&listener);
                Parser_.error = false;
                return nullptr;
            } catch (...) {
                errors.Error(0, 0, CurrentExceptionMessage());
                Parser_.removeErrorListener(&listener);
                Parser_.error = false;
                return nullptr;
            }
        }

    private:
        TString QueryName_;

        antlr4::ANTLRInputStream InputStream_;
        TLexer Lexer_;

        antlr4::CommonTokenStream TokenStream_;
        TParser Parser_;
    };

} // namespace NProtoAST

