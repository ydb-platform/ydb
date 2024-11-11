#include "yql_position.h"

#include <ydb/library/yql/utils/line_split.h>

#include <util/charset/utf8.h>
#include <util/stream/str.h>

namespace NYdb {
    namespace NConsoleClient {

        ui32 YQLPositionMapping::RawPos(const TParsedToken& token) const {
            return SymbolsCountBeforeLine.at(token.Line) + token.LinePos;
        }

        YQLPositionMapping YQLPositionMapping::Build(const TString& queryUtf8) {
            (void)queryUtf8;
            TVector<ui32> symbolsCountBeforeLine = {0, 0};

            return YQLPositionMapping(std::move(symbolsCountBeforeLine));
        }

        YQLPositionMapping::YQLPositionMapping(TVector<ui32> SymbolsCountBeforeLine)
            : SymbolsCountBeforeLine(std::move(SymbolsCountBeforeLine))
        {
        }

    }
}
