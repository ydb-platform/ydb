#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/dynumber/dynumber.h>

namespace NKikimr {
namespace NDataShard {

//YdbOld and Ydb.v1 have same value representation
template <typename TOutValue>
Y_FORCE_INLINE bool AddCell(TOutValue& row, NScheme::TTypeInfo type, const TCell& cell, TString& err)
{
    auto &val = *row.add_items();

    if (cell.IsNull()) {
        val.set_null_flag_value(::google::protobuf::NULL_VALUE);
        return true;
    }

    switch (type.GetTypeId()) {
    case NUdf::TDataType<bool>::Id: {
        bool value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_bool_value(value);
        break;
    }
    case NUdf::TDataType<ui8>::Id: {
        ui8 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_uint32_value(value);
        break;
    }
    case NUdf::TDataType<i8>::Id: {
        i8 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_int32_value(value);
        break;
    }
    case NUdf::TDataType<ui16>::Id: {
        ui16 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_uint32_value(value);
        break;
    }
    case NUdf::TDataType<i16>::Id: {
        i16 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_int32_value(value);
        break;
    }
    case NUdf::TDataType<ui32>::Id: {
        ui32 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_uint32_value(value);
        break;
    }
    case NUdf::TDataType<i32>::Id: {
        i32 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_int32_value(value);
        break;
    }
    case NUdf::TDataType<ui64>::Id: {
        ui64 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_uint64_value(value);
        break;
    }
    case NUdf::TDataType<i64>::Id: {
        i64 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_int64_value(value);
        break;
    }
    case NUdf::TDataType<float>::Id: {
        float value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_float_value(value);
        break;
    }
    case NUdf::TDataType<double>::Id: {
        double value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_double_value(value);
        break;
    }
    case NUdf::TDataType<NUdf::TJson>::Id:
    case NUdf::TDataType<NUdf::TUtf8>::Id: {
        val.set_text_value(cell.Data(), cell.Size());
        break;
    }
    case NUdf::TDataType<NUdf::TYson>::Id: {
        val.set_bytes_value(cell.Data(), cell.Size());
        break;
    }
    case NUdf::TDataType<NUdf::TUuid>::Id:
    case NUdf::TDataType<NUdf::TDecimal>::Id: {
        struct TCellData {
            ui64 Low;
            ui64 High;
        } value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_low_128(value.Low);
        val.set_high_128(value.High);
        break;
    }
    case NUdf::TDataType<NUdf::TDate>::Id: {
        ui16 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_uint32_value(value);
        break;
    }
    case NUdf::TDataType<NUdf::TDatetime>::Id: {
        ui32 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_uint32_value(value);
        break;
    }
    case NUdf::TDataType<NUdf::TTimestamp>::Id: {
        ui64 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_uint64_value(value);
        break;
    }
    case NUdf::TDataType<NUdf::TInterval>::Id: {
        i64 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_int64_value(value);
        break;
    }
    case NUdf::TDataType<NUdf::TDate32>::Id: {
        i32 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_int32_value(value);
        break;
    }
    case NUdf::TDataType<NUdf::TDatetime64>::Id: 
    case NUdf::TDataType<NUdf::TTimestamp64>::Id:
    case NUdf::TDataType<NUdf::TInterval64>::Id: {
        i64 value;
        if (!cell.ToValue(value, err))
            return false;
        val.set_int64_value(value);
        break;
    }    
    case NUdf::TDataType<NUdf::TJsonDocument>::Id: {
        const auto json = NBinaryJson::SerializeToJson(TStringBuf(cell.Data(), cell.Size()));
        val.set_text_value(json);
        break;
    }
    case NUdf::TDataType<NUdf::TDyNumber>::Id: {
        const auto number = NDyNumber::DyNumberToString(TStringBuf(cell.Data(), cell.Size()));
        if (!number.Defined()) {
            err = "Invalid DyNumber binary representation";
            return false;
        }
        val.set_text_value(*number);
        break;
    }
    case NScheme::NTypeIds::Pg: {
        auto result = NPg::PgNativeTextFromNativeBinary(cell.AsBuf(), type.GetPgTypeDesc());
        if (result.Error) {
            err = Sprintf("Failed to add cell to Ydb::Value: %s", (*result.Error).c_str());
            return false;
        }
        val.set_text_value(result.Str);
        break;
    }
    default:
        val.set_bytes_value(cell.Data(), cell.Size());
    }
    return true;
}

} // namespace NDataShard
} // namespace NKikimr
