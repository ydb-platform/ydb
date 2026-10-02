#include "kqp_row_builder.h"

#include <ydb/core/engine/mkql_keys.h>

#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

TRowBuilder::TRowBuilder(size_t size) : CellsInfo(size) {
}

TRowBuilder& TRowBuilder::AddCell(const size_t index, const NScheme::TTypeInfo& type,
    const NUdf::TUnboxedValuePod& value, const TString& typeMod)
{
    CellsInfo[index].Type = type;
    CellsInfo[index].Value = value;

    if (CellsInfo[index].Type.GetTypeId() == NUdf::TDataType<NUdf::TDecimal>::Id && value) {
        CellsInfo[index].DecimalBuf = value.GetInt128();
    }

    if (type.GetTypeId() == NScheme::NTypeIds::Pg && value) {
        auto typeDesc = type.GetPgTypeDesc();

        if (!typeMod.empty() && NPg::TypeDescNeedsCoercion(typeDesc)) {
            auto typeModResult = NPg::BinaryTypeModFromTextTypeMod(typeMod, type.GetPgTypeDesc());

            if (typeModResult.Error) {
                ythrow yexception() << "BinaryTypeModFromTextTypeMod error: " << *typeModResult.Error;
            }

            AFL_ENSURE(typeModResult.Typmod != -1);

            TMaybe<TString> err;
            CellsInfo[index].PgBinaryValue = NYql::NCommon::PgValueCoerce(value, NPg::PgTypeIdFromTypeDesc(typeDesc), typeModResult.Typmod, &err);

            if (err) {
                ythrow yexception() << "PgValueCoerce error: " << *err;
            }
        } else {
            CellsInfo[index].PgBinaryValue = NYql::NCommon::PgValueToNativeBinary(value, NPg::PgTypeIdFromTypeDesc(typeDesc));
        }
    } else {
        CellsInfo[index].PgBinaryValue.clear();
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

    const bool isPg = cellInfo.Type.GetTypeId() == NScheme::NTypeIds::Pg;

    const auto ref = isPg
        ? NYql::NUdf::TStringRef(cellInfo.PgBinaryValue)
        : cellInfo.Value.AsStringRef();

    return TCell(ref.Data(), ref.Size());
}

}
}
