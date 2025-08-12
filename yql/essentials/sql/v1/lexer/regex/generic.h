#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/generic/ylimits.h>

#include <functional>

namespace NSQLTranslationV1 {

    struct TGenericToken {
        static constexpr const char* Error = "<ERROR>";

        TString Name;
        TStringBuf Content;
        size_t Begin = 0; // In bytes
    };

    class IGenericLexer: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IGenericLexer>;
        using TTokenCallback = std::function<void(TGenericToken&& token)>;

        static constexpr size_t MaxErrorsLimit = Max<size_t>();

        virtual ~IGenericLexer() = default;
        virtual bool Tokenize(
            TStringBuf text,
            const TTokenCallback& onNext,
            size_t maxErrors = IGenericLexer::MaxErrorsLimit) const = 0;
    };

    using TTokenMatcher = std::function<TMaybe<TGenericToken>(TStringBuf prefix)>;

    using TGenericLexerGrammar = TVector<TTokenMatcher>;

    struct TRegexPattern {
        TString Body;
        TString After = "";
        TString Before = "";
        bool IsCaseInsensitive = false;
    };

    TTokenMatcher Compile(TString name, const TRegexPattern& regex);
    TRegexPattern Merged(TVector<TRegexPattern> patterns);

    IGenericLexer::TPtr MakeGenericLexer(TGenericLexerGrammar grammar);

    TVector<TGenericToken> Tokenize(IGenericLexer::TPtr& lexer, TStringBuf text);

} // namespace NSQLTranslationV1
