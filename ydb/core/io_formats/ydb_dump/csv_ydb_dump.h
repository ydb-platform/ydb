#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/memory/pool.h>

namespace NKikimr::NFormats {

struct TYdbDump {
    // Parse YdbDump-formatted line.
    // Returns true in case of success, false otherwise.
    // numBytes will be increased by the size of the processed cells
    static bool ParseLine(
        TStringBuf line,
        const std::vector<std::pair<i32, NScheme::TTypeInfo>>& columnOrderTypes,
        TMemoryPool& pool,
        TVector<TCell>& keys,
        TVector<TCell>& values,
        TString& strError,
        ui64& numBytes);
};

}
