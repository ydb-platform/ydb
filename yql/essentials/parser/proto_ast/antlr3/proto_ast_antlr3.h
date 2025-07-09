#pragma once

#include <yql/essentials/parser/proto_ast/common.h>

#include <contrib/libs/antlr3_cpp_runtime/include/antlr3.hpp>

namespace NProtoAST {
    using namespace NAST;

    template <typename TParser, typename TLexer>
    class TProtoASTBuilder3 {
        typedef ANTLR_UINT8 TChar;

    public:
        TProtoASTBuilder3(TStringBuf data, const TString& queryName = "query", google::protobuf::Arena* arena = nullptr)
            : QueryName_(queryName)
            , InputStream_((const TChar*)data.data(), antlr3::ENC_8BIT, data.length(), (TChar*)QueryName_.begin())  // Why the hell antlr needs non-const ptr??
            , Lexer_(&InputStream_, static_cast<google::protobuf::Arena*>(nullptr))
            , TokenStream_(ANTLR_SIZE_HINT, Lexer_.get_tokSource())
            , Parser_(&TokenStream_, arena)
        {
        }

        google::protobuf::Message* BuildAST(IErrorCollector& errors) {
            // TODO: find a better way to break on lexer errors
            try {
                Lexer_.ReportErrors(&errors);
                return Parser_.Parse(Lexer_, &errors);
            } catch (const TTooManyErrors&) {
                return nullptr;
            } catch (...) {
                errors.Error(0, 0, CurrentExceptionMessage());
                return nullptr;
            }
        }

    private:
        TString QueryName_;

        typename TLexer::InputStreamType InputStream_;
        TLexer Lexer_;

        typename TParser::TokenStreamType TokenStream_;
        TParser Parser_;
    };

    template <typename TLexer>
    class TLexerTokensCollector3 {
        typedef ANTLR_UINT8 TChar;

    public:
        TLexerTokensCollector3(TStringBuf data, const char** tokenNames, const TString& queryName = "query")
            : TokenNames_(tokenNames)
            , QueryName_(queryName)
            , InputStream_((const TChar*)data.data(), antlr3::ENC_8BIT, data.length(), (TChar*)QueryName_.begin())
            , Lexer_(&InputStream_, static_cast<google::protobuf::Arena*>(nullptr))
        {
        }

        void CollectTokens(IErrorCollector& errors, const NSQLTranslation::ILexer::TTokenCallback& onNextToken) {
            try {
                Lexer_.ReportErrors(&errors);
                auto src = Lexer_.get_tokSource();

                for (;;) {
                    auto token = src->nextToken();
                    auto type = token->getType();
                    const bool isEOF = type == TLexer::CommonTokenType::TOKEN_EOF;
                    NSQLTranslation::TParsedToken last;
                    last.Name = isEOF ? "EOF" : TokenNames_[type];
                    last.Content = token->getText();
                    last.Line = token->get_line();
                    last.LinePos = token->get_charPositionInLine();

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
        const char** TokenNames_;
        TString QueryName_;
        typename TLexer::InputStreamType InputStream_;
        TLexer Lexer_;
    };
} // namespace NProtoAST
