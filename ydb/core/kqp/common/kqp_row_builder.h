#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <yql/essentials/public/udf/udf_value.h>

namespace NKikimr {
namespace NKqp {

class TRowBuilder {
private:
    struct TCellInfo {
        NScheme::TTypeInfo Type;
        NUdf::TUnboxedValuePod Value;
        TString PgBinaryValue;

        NYql::NDecimal::TInt128 DecimalBuf;
    };

public:
    explicit TRowBuilder(size_t size);

    TRowBuilder& AddCell(const size_t index, const NScheme::TTypeInfo& type,
        const NUdf::TUnboxedValuePod& value, const TString& typeMod);

    TConstArrayRef<TCell> BuildCells();

private:
    TCell BuildCell(const TCellInfo& cellInfo);

    TVector<TCellInfo> CellsInfo;
    TVector<TCell> Cells;

    TOwnedCellVecBatch Batch;
};

}
}
