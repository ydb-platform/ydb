#include "yql_position.h"

#include <yql/essentials/utils/line_split.h>

#include <util/charset/utf8.h>
#include <util/stream/str.h>

namespace NYdb::NConsoleClient {

    ui32 TYQLPositionMapping::RawPos(const TParsedToken& token) const {
        return SymbolsCountBeforeLine.at(token.Line) + token.LinePos;
    }

    TYQLPositionMapping TYQLPositionMapping::Build(const TString& queryUtf8) {
        TVector<ui32> symbolsCountBeforeLine = {0, 0};

        TStringStream stream(queryUtf8);
        TLineSplitter lines(stream);

        size_t read;
        for (TString line; (read = lines.Next(line)) != 0;) {
            const auto index = symbolsCountBeforeLine.size();
            const auto previous = symbolsCountBeforeLine.at(index - 1);

            const auto newlineWidth = read - line.size();
            Y_ASSERT(0 <= newlineWidth && newlineWidth <= 2);

            const auto current = GetNumberOfUTF8Chars(line) + newlineWidth;
            symbolsCountBeforeLine.emplace_back(previous + current);
        }

        return TYQLPositionMapping(std::move(symbolsCountBeforeLine));
    }

    TYQLPositionMapping::TYQLPositionMapping(TVector<ui32> SymbolsCountBeforeLine)
        : SymbolsCountBeforeLine(std::move(SymbolsCountBeforeLine))
    {
    }

} // namespace NYdb::NConsoleClient
