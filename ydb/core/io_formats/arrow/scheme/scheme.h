#pragma once

#include <ydb/library/formats/arrow/csv/converter/csv_arrow.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

namespace NKikimr::NFormats {

class TArrowCSVScheme: public TArrowCSV {
    using TArrowCSV::TArrowCSV;
public:
    /// If header is true read column names from first line after skipRows. Parse columns as strings in this case.
    /// @note It's possible to skip header with skipRows and use typed columns instead.
    static arrow::Result<TArrowCSV> Create(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, bool header = false, const std::set<std::string>& notNullColumns = {});

};

}