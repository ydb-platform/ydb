#pragma once

#include "sql_highlight.h"

#include <util/generic/ptr.h>
#include <util/generic/ylimits.h>

#include <functional>

namespace NSQLHighlight {

    struct TToken {
        EUnitKind Kind;
        size_t Begin;  // In bytes
        size_t Length; // In bytes
    };

    class IHighlighter: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IHighlighter>;
        using TTokenCallback = std::function<void(TToken&& token)>;

        virtual ~IHighlighter() = default;
        virtual bool Tokenize(
            TStringBuf text,
            const TTokenCallback& onNext,
            size_t maxErrors = Max<size_t>()) const = 0;
    };

    TVector<TToken> Tokenize(IHighlighter& highlighter, TStringBuf text);

    IHighlighter::TPtr MakeHighlighter(const THighlighting& highlighting);

} // namespace NSQLHighlight
