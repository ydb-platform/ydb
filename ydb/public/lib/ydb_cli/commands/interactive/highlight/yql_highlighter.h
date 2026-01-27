#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/fwd.h>

namespace NYdb::NConsoleClient {

    // Colors are provided as for a UTF32 string
    using TColors = replxx::Replxx::colors_t;

    class IYQLHighlighter {
    public:
        using TPtr = THolder<IYQLHighlighter>;

        virtual void Apply(TStringBuf queryUtf8, TColors& colors) const = 0;
        virtual ~IYQLHighlighter() = default;
    };

    TString PrintYqlHighlightAnsiColors(const TString& queryUtf8, const TColors& colors);

    ftxui::Element PrintYqlHighlightFtxuiColors(const TString& queryUtf8, const TColors& colors);

    IYQLHighlighter::TPtr MakeYQLHighlighter(TColorSchema color);

} // namespace NYdb::NConsoleClient
