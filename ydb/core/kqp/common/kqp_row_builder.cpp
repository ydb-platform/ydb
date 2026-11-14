#include "kqp_row_builder.h"

#include <ydb/core/engine/mkql_keys.h>


namespace NKikimr {
namespace NKqp {

TRowBuilder::TRowBuilder(size_t size) : CellsInfo(size) {
}

TRowBuilder& TRowBuilder::AddCell(const size_t index, const NScheme::TTypeInfo& type,
    const NUdf::TUnboxedValuePod& value, const TString&)
{
    CellsInfo[index].Type = type;
    CellsInfo[index].Value = value;

    if (CellsInfo[index].Type.GetTypeId() == NUdf::TDataType<NUdf::TDecimal>::Id && value) {
        CellsInfo[index].DecimalBuf = value.GetInt128();
    }

    return *this;
}

TConstArrayRef<TCell> TRowBuilder::BuildCells() {
    Cells.clear();
    Cells.reserve(CellsInfo.size());

    for (const auto& cellInfo : CellsInfo) {
        Cells.emplace_back(BuildCell(cellInfo));
    }
    return Cells;
}

TCell TRowBuilder::BuildCell(const TCellInfo& cellInfo) {
    if (!cellInfo.Value) {
        return TCell();
    }

    switch(cellInfo.Type.GetTypeId()) {
#define MAKE_PRIMITIVE_TYPE_CELL_CASE(type, layout) \
    case NUdf::TDataType<type>::Id: return NMiniKQL::MakeCell<layout>(cellInfo.Value);
        KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_CELL_CASE)
    case NUdf::TDataType<NUdf::TDecimal>::Id:
        {
            constexpr auto valueSize = sizeof(cellInfo.DecimalBuf);
            return TCell(reinterpret_cast<const char*>(&cellInfo.DecimalBuf), valueSize);
        }
    }

    const auto ref = cellInfo.Value.AsStringRef();

    return TCell(ref.Data(), ref.Size());
}

}
}
