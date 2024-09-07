#pragma once

#include <ydb/library/yql/parser/lexer_common/lexer.h>


#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>
#include <contrib/libs/antlr3_cpp_runtime/include/antlr3.hpp>

#include <google/protobuf/message.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/charset/utf8.h>
#include <type_traits>

template<typename T>
struct IsDerivedFromLexer
{
    static constexpr bool value = std::is_base_of<antlr4::Lexer, T>::value;
};

template<typename T>
struct IsDerivedFromParser
{
    static constexpr bool value = std::is_base_of<antlr4::Parser, T>::value;
};


namespace NProtoAST {
    static const char* INVALID_TOKEN_NAME = "nothing";
    static const char* ABSENCE = " absence";

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
    inline void InvalidToken(IOutputStream& err, const TokenType* token) {
        if (token) {
            if (token->get_input()) {
                err << " '" << token->getText() << "'";
            } else {
                err << ABSENCE;
            }
        }
    }

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

} // namespace NProtoAST

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

    template <typename TParser, typename TLexer,  bool IsAntlr4 = IsDerivedFromLexer<TLexer>::value && IsDerivedFromParser<TParser>::value>
    class TProtoASTBuilder;

    template <typename TParser, typename TLexer>
    class TProtoASTBuilder<TParser, TLexer, false> {
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
            } catch (...) {
                errors.Error(0, 0, CurrentExceptionMessage());
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

    template <typename TParser, typename TLexer>
    class TProtoASTBuilder<TParser, TLexer, true> {

    public:
        TProtoASTBuilder(TStringBuf data, const TString& queryName = "query", google::protobuf::Arena* arena = nullptr)
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

    template <typename TLexer, bool IsAntlr4 = IsDerivedFromLexer<TLexer>::value>
    class TLexerTokensCollector;

    template <typename TLexer>
    class TLexerTokensCollector<TLexer, false> {
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
            } catch (...) {
                errors.Error(0, 0, CurrentExceptionMessage());
            }
        }

    private:
        const char** TokenNames;
        TString QueryName;
        typename TLexer::InputStreamType InputStream;
        TLexer Lexer;
    };

    template <typename TLexer>
    class TLexerTokensCollector<TLexer, true> {

    public:
        TLexerTokensCollector(TStringBuf data, const TString& queryName = "query")
            : QueryName(queryName)
            , InputStream(std::string(data))
            , Lexer(&InputStream)
        {
        }

        void CollectTokens(IErrorCollector& errors, const NSQLTranslation::ILexer::TTokenCallback& onNextToken) {
            try {
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
