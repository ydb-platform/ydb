#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <ydb/public/lib/ydb_cli/commands/interactive/highlight/color/schema.h>

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

    IYQLHighlighter::TPtr MakeYQLHighlighter(TColorSchema color);

} // namespace NYdb::NConsoleClient
