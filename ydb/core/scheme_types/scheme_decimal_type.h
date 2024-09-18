#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/strbuf.h>

#include <optional>

namespace NKikimr::NScheme {

class TDecimalType {
public:
    constexpr TDecimalType(ui32 precision, ui32 scale)
        : Precision(precision)
        , Scale(scale)
    {
        // TODO Uncomment after parametrized decimal in KQP
        //Y_ABORT_UNLESS(Precision);
        //Y_ABORT_UNLESS(Scale);
    }

    TString CellValueToString(const std::pair<ui64, i64>& cellValue) const;
    void CellValueToStream(const std::pair<ui64, i64>& cellValue, IOutputStream& out) const;    

    static const std::optional<TDecimalType> ParseTypeName(const TStringBuf& typeName);
private:
    YDB_READONLY_CONST(ui32, Precision);
    YDB_READONLY_CONST(ui32, Scale);
};

} // namespace NKikimr::NScheme

