#pragma once

#include <yql/essentials/parser/lexer_common/lexer.h>

#include <util/system/types.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NConsoleClient {

    using NSQLTranslation::TParsedToken;

    class TYQLPositionMapping final {
    public:
        // Translates (Line, LinePos) position into RawPos that
        // is an absolute symbol position in utf8 symbols array
        ui32 RawPos(const TParsedToken& token) const;

    public:
        static TYQLPositionMapping Build(const TString& queryUtf8);

    private:
        explicit TYQLPositionMapping(TVector<ui32> SymbolsCountBeforeLine);

    private:
        TVector<ui32> SymbolsCountBeforeLine;
    };

} // namespace NYdb::NConsoleClient
