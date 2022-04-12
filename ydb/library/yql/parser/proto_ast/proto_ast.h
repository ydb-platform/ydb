#pragma once

#include <ydb/library/yql/parser/lexer_common/lexer.h>

#include <contrib/libs/antlr3_cpp_runtime/include/antlr3.hpp>

#include <google/protobuf/message.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/charset/utf8.h>

namespace NProtoAST {
    template <typename InputType>
    void InvalidCharacter(IOutputStream& err, const InputType* input) {
        wchar32 rune = 0;
        size_t runeLen = 0;
        auto begin = input->get_nextChar();
        auto end = begin + input->get_sizeBuf();
        if (begin != end && SafeReadUTF8Char(rune, runeLen, begin, end) == RECODE_OK) {
            err << " '" << TStringBuf((const char*)begin, runeLen) << "' (Unicode character <" << ui32(rune) << ">)";
        }
    }

    template <typename TokenType>
    void InvalidToken(IOutputStream& err, const TokenType* token) {
        if (token) {
            if (token->get_input()) {
                err << " '" << token->getText() << "'";
            } else {
                err << " absence";
            }
        }
    }

    class TTooManyErrors : public yexception {
    };

    class IErrorCollector {
    public:
        explicit IErrorCollector(size_t maxErrors);
        virtual ~IErrorCollector();

        // throws TTooManyErrors
        void Error(ui32 line, ui32 col, const TString& message);

    private:
        virtual void AddError(ui32 line, ui32 col, const TString& message) = 0;

    protected:
        const size_t MaxErrors;
        size_t NumErrors;
    };

    class TErrorOutput: public IErrorCollector {
    public:
        TErrorOutput(IOutputStream& err, const TString& name, size_t maxErrors);
        virtual ~TErrorOutput();

    private:
        void AddError(ui32 line, ui32 col, const TString& message) override;

    public:
        IOutputStream& Err;
        TString Name;
    };

    template <typename TParser, typename TLexer>
    class TProtoASTBuilder {
        typedef ANTLR_UINT8 TChar;

    public:
        TProtoASTBuilder(TStringBuf data, const TString& queryName = "query", google::protobuf::Arena* arena = nullptr)
            : QueryName(queryName)
            , InputStream((const TChar*)data.data(), antlr3::ENC_8BIT, data.length(), (TChar*)QueryName.begin())  // Why the hell antlr needs non-const ptr??
            , Lexer(&InputStream, static_cast<google::protobuf::Arena*>(nullptr))
            , TokenStream(ANTLR_SIZE_HINT, Lexer.get_tokSource())
            , Parser(&TokenStream, arena)
        {
        }

        google::protobuf::Message* BuildAST(IErrorCollector& errors) {
            // TODO: find a better way to break on lexer errors
            try {
                Lexer.ReportErrors(&errors);
                return Parser.Parse(Lexer, &errors);
            } catch (const TTooManyErrors&) {
                return nullptr;
            } catch (const yexception& e) {
                errors.Error(0, 0, e.what());
                return nullptr;
            }
        }

    private:
        TString QueryName;

        typename TLexer::InputStreamType InputStream;
        TLexer Lexer;

        typename TParser::TokenStreamType TokenStream;
        TParser Parser;
    };

    template <typename TLexer>
    class TLexerTokensCollector {
        typedef ANTLR_UINT8 TChar;

    public:
        TLexerTokensCollector(TStringBuf data, const char** tokenNames, const TString& queryName = "query")
            : TokenNames(tokenNames)
            , QueryName(queryName)
            , InputStream((const TChar*)data.data(), antlr3::ENC_8BIT, data.length(), (TChar*)QueryName.begin())
            , Lexer(&InputStream, static_cast<google::protobuf::Arena*>(nullptr))
        {
        }

        void CollectTokens(IErrorCollector& errors, const NSQLTranslation::ILexer::TTokenCallback& onNextToken) {
            try {
                Lexer.ReportErrors(&errors);
                auto src = Lexer.get_tokSource();
                for (;;) {
                    auto token = src->nextToken();
                    auto type = token->getType();
                    const bool isEOF = type == TLexer::CommonTokenType::TOKEN_EOF;
                    NSQLTranslation::TParsedToken last;
                    last.Name = isEOF ? "EOF" : TokenNames[type];
                    last.Content = token->getText();
                    last.Line = token->get_line();
                    last.LinePos = token->get_charPositionInLine();
                    onNextToken(std::move(last));
                    if (isEOF) {
                        break;
                    }
                }
            } catch (const TTooManyErrors&) {
            } catch (const yexception& e) {
                errors.Error(0, 0, e.what());
            }
        }

    private:
        const char** TokenNames;
        TString QueryName;
        typename TLexer::InputStreamType InputStream;
        TLexer Lexer;
    };
} // namespace NProtoAST
