#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/fwd.h>

namespace NYdb::NConsoleClient {

    using TColor = replxx::Replxx::Color;

    // Colors are provided as for a UTF32 string
    using TColors = replxx::Replxx::colors_t;

    struct TColorSchema {
        TColor keyword;
        TColor operation;
        struct {
            TColor function;
            TColor type;
            TColor variable;
            TColor quoted;
        } identifier;
        TColor string;
        TColor number;
        TColor comment;
        TColor unknown;

        static TColorSchema Monaco();
        static TColorSchema Debug();
    };

    class IYQLHighlighter {
    public:
        using TPtr = THolder<IYQLHighlighter>;

        virtual void Apply(TStringBuf queryUtf8, TColors& colors) = 0;
        virtual ~IYQLHighlighter() = default;
    };

    IYQLHighlighter::TPtr MakeYQLHighlighter(TColorSchema color);

} // namespace NYdb::NConsoleClient
