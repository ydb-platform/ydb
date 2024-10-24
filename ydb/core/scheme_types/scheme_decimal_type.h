#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/strbuf.h>

#include <optional>

namespace NKikimr::NScheme {

class TDecimalType {
public:
    TDecimalType(ui32 precision, ui32 scale);

    bool operator==(const TDecimalType& other) const;

    TString CellValueToString(const std::pair<ui64, i64>& cellValue) const;
    void CellValueToStream(const std::pair<ui64, i64>& cellValue, IOutputStream& out) const;    

    static const std::optional<TDecimalType> ParseTypeName(const TStringBuf& typeName);
    static bool Validate(ui32 precision, ui32 scale, TString& error);

    static TDecimalType Default() {
        return TDecimalType(DECIMAL_PRECISION, DECIMAL_SCALE);
    }
private:
    YDB_READONLY_CONST(ui32, Precision);
    YDB_READONLY_CONST(ui32, Scale);
};

} // namespace NKikimr::NScheme

