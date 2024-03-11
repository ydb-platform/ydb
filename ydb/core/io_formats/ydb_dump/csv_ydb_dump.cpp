#include "csv_ydb_dump.h"

#include <ydb/core/io_formats/cell_maker/cell_maker.h>

namespace NKikimr::NFormats {

bool TYdbDump::ParseLine(TStringBuf line, const std::vector<std::pair<i32, NScheme::TTypeInfo>>& columnOrderTypes, TMemoryPool& pool,
                         TVector<TCell>& keys, TVector<TCell>& values, TString& strError, ui64& numBytes)
{
    for (const auto& [keyOrder, pType] : columnOrderTypes) {
        TStringBuf value = line.NextTok(',');
        if (!value) {
            strError = "Empty token";
            return false;
        }

        TCell* cell = nullptr;

        if (keyOrder != -1) {
            if ((int)keys.size() < (keyOrder + 1)) {
                keys.resize(keyOrder + 1);
            }

            cell = &keys.at(keyOrder);
        } else {
            cell = &values.emplace_back();
        }

        Y_ABORT_UNLESS(cell);
        
        TString parseError;
        if (!MakeCell(*cell, value, pType, pool, parseError)) {
            strError = TStringBuilder() << "Value parse error: '" << value << "' " << parseError;
            return false;
        }

        if (!CheckCellValue(*cell, pType)) {
            strError = TStringBuilder() << "Value check error: '" << value << "'";
            return false;
        }

        numBytes += cell->Size();
    }

    return true;
}

}
