#pragma once

#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/parser/common/error.h>

#include <google/protobuf/message.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/charset/utf8.h>

namespace NAST {

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

} // namespace NAST

namespace NSQLTranslation {

    class IParser {
    public:
        virtual ~IParser() = default;

        virtual google::protobuf::Message* Parse(
            const TString& query, const TString& queryName, NAST::IErrorCollector& err,
            google::protobuf::Arena* arena) = 0;
    };

    class IParserFactory : public TThrRefBase {
    public:
        virtual ~IParserFactory() = default;

        virtual std::unique_ptr<IParser> MakeParser() const = 0;
    };

    using TParserFactoryPtr = TIntrusivePtr<IParserFactory>;

} // namespace NSQLTranslation
