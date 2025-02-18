#pragma once

#include <yql/essentials/parser/lexer_common/lexer.h>

#include <google/protobuf/message.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/charset/utf8.h>

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

namespace NSQLTranslation {
    class IParser {
    public:
        virtual ~IParser() = default;

        virtual google::protobuf::Message* Parse(
            const TString& query, const TString& queryName, NProtoAST::IErrorCollector& err,
            google::protobuf::Arena* arena) = 0;
    };

    class IParserFactory : public TThrRefBase {
    public:
        virtual ~IParserFactory() = default;

        virtual std::unique_ptr<IParser> MakeParser() const = 0;
    };

    using TParserFactoryPtr = TIntrusivePtr<IParserFactory>;
} // namespace NSQLTranslation
