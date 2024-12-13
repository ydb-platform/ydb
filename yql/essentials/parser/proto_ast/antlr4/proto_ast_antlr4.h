#pragma once

#include <yql/essentials/parser/proto_ast/common.h>

#ifdef ERROR
#undef ERROR
#endif
#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>

namespace antlr4 {
    class ANTLR4CPP_PUBLIC YqlErrorListener : public BaseErrorListener {
        NProtoAST::IErrorCollector* errors;
        bool* error;
    public:
        YqlErrorListener(NProtoAST::IErrorCollector* errors, bool* error);

        virtual void syntaxError(Recognizer *recognizer, Token * offendingSymbol, size_t line, size_t charPositionInLine,
                                const std::string &msg, std::exception_ptr e) override;
    };
}

namespace NProtoAST {
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
            : QueryName(queryName)
            , InputStream(data)
            , Lexer(&InputStream)
            , TokenStream(&Lexer)
            , Parser(&TokenStream, arena)
        {
        }

        google::protobuf::Message* BuildAST(IErrorCollector& errors) {
            // TODO: find a better way to break on lexer errors
            typename antlr4::YqlErrorListener listener(&errors, &Parser.error);
            Parser.removeErrorListeners();
            Parser.addErrorListener(&listener);
            try {
                auto result = Parser.Parse(&errors);
                Parser.removeErrorListener(&listener);
                Parser.error = false;
                return result;
            } catch (const TTooManyErrors&) {
                Parser.removeErrorListener(&listener);
                Parser.error = false;
                return nullptr;
            } catch (...) {
                errors.Error(0, 0, CurrentExceptionMessage());
                Parser.removeErrorListener(&listener);
                Parser.error = false;
                return nullptr;
            }
        }

    private:
        TString QueryName;

        antlr4::ANTLRInputStream InputStream;
        TLexer Lexer;

        antlr4::CommonTokenStream TokenStream;
        TParser Parser;
    };

    template <typename TLexer>
    class TLexerTokensCollector4 {

    public:
        TLexerTokensCollector4(TStringBuf data, const TString& queryName = "query")
            : QueryName(queryName)
            , InputStream(std::string(data))
            , Lexer(&InputStream)
        {
        }

        void CollectTokens(IErrorCollector& errors, const NSQLTranslation::ILexer::TTokenCallback& onNextToken) {
            try {
                bool error = false;
                typename antlr4::YqlErrorListener listener(&errors, &error);
                Lexer.removeErrorListeners();
                Lexer.addErrorListener(&listener);

                for (;;) {
                    auto token = Lexer.nextToken();
                    auto type = token->getType();
                    const bool isEOF = type == TLexer::EOF;
                    NSQLTranslation::TParsedToken last;
                    last.Name = GetTokenName(type);
                    last.Content = token->getText();
                    last.Line = token->getLine();
                    last.LinePos = token->getCharPositionInLine();
                    onNextToken(std::move(last));
                    if (isEOF) {
                        break;
                    }
                }
            } catch (const TTooManyErrors&) {
            } catch (...) {
                errors.Error(0, 0, CurrentExceptionMessage());
            }
        }

    private:
        TString GetTokenName(size_t type) const {
            auto res = Lexer.getVocabulary().getSymbolicName(type);
            if (res != ""){
                return TString(res);
            }
            return TString(INVALID_TOKEN_NAME);
        }

        TString QueryName;
        antlr4::ANTLRInputStream InputStream;
        TLexer Lexer;
    };
} // namespace NProtoAST

