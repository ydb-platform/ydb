#include "cell_maker.h"

#include <ydb/library/binary_json/write.h>
#include <ydb/library/dynumber/dynumber.h>
#include <ydb/library/uuid/uuid.h>

#include <ydb/library/yql/minikql/dom/yson.h>
#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/utils/utf8.h>

#include <contrib/libs/double-conversion/double-conversion/double-conversion.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/datetime/base.h>
#include <util/string/cast.h>

#include <typeinfo>

namespace NKikimr::NFormats {

namespace {

    bool CheckedUnescape(TStringBuf value, TString& result) {
        if (value.size() < 2 || value.front() != '"' || value.back() != '"') {
            return false;
        }

        result = CGIUnescapeRet(value.Skip(1).Chop(1));
        return true;
    }

    template <typename T>
    TString MakeError() {
        return TStringBuilder() << "" << typeid(T).name() << " is expected.";
    }

    template <typename T>
    bool TryParse(TStringBuf value, T& result) {
        return TryFromString(value, result);
    }

    template <>
    bool TryParse(TStringBuf value, double& result) {
        struct TCvt: public double_conversion::StringToDoubleConverter {
            inline TCvt()
                : StringToDoubleConverter(ALLOW_TRAILING_JUNK | ALLOW_HEX | ALLOW_LEADING_SPACES, 0.0, NAN, "inf", "nan")
            {
            }
        };

        int processed = 0;
        result = Singleton<TCvt>()->StringToDouble(value.Data(), value.Size(), &processed);

        return static_cast<size_t>(processed) == value.Size();
    }

    template <>
    bool TryParse(TStringBuf value, float& result) {
        double tmp;
        if (TryParse(value, tmp)) {
            result = static_cast<float>(tmp);
            return true;
        }

        return false;
    }

    template <>
    bool TryParse(TStringBuf value, TInstant& result) {
        return TInstant::TryParseIso8601(value, result);
    }

    template <>
    bool TryParse(TStringBuf value, TString& result) {
        return CheckedUnescape(value, result);
    }

    template <>
    bool TryParse(TStringBuf value, NYql::NDecimal::TInt128& result) {
        if (!NYql::NDecimal::IsValid(value)) {
            return false;
        }

        result = NYql::NDecimal::FromString(value, NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE);
        return true;
    }

    template <>
    bool TryParse(TStringBuf value, TMaybe<NBinaryJson::TBinaryJson>& result) {
        TString unescaped;
        if (!CheckedUnescape(value, unescaped)) {
            return false;
        }

        result = NBinaryJson::SerializeToBinaryJson(unescaped);
        return result.Defined();
    }

    template <>
    bool TryParse(TStringBuf value, TMaybe<TString>& result) {
        result = NDyNumber::ParseDyNumberString(value);
        return result.Defined();
    }

    template <typename T>
    bool TryParse(TStringBuf value, T& result, TString& err, void* parseParam) {
        Y_UNUSED(value);
        Y_UNUSED(result);
        Y_UNUSED(err);
        Y_UNUSED(parseParam);
        Y_ABORT("TryParse with parseParam is unimplemented");
    }

    template <>
    bool TryParse(TStringBuf value, NPg::TConvertResult& result, TString& err, void* typeDesc) {
        TString unescaped;
        if (!CheckedUnescape(value, unescaped)) {
            err = MakeError<NPg::TConvertResult>();
            return false;
        }

        result = NPg::PgNativeBinaryFromNativeText(unescaped, typeDesc);
        if (result.Error) {
            err = *result.Error;
            return false;
        }

        return true;
    }

    struct TUuidHolder {
        union {
            ui16 Array[8];
            char Str[16];
        } Buf;
    };

    template <>
    bool TryParse(TStringBuf value, TUuidHolder& result) {
        if (!NUuid::ParseUuidToArray(value, result.Buf.Array, false)) {
            return false;
        }
        return true;
    }

    template <typename T, typename U>
    using TConverter = std::function<U(const T&)>;

    template <typename T, typename U>
    U Implicit(const T& v) {
        return v;
    }

    ui16 Days(const TInstant& v) {
        return v.Days();
    }

    ui32 Seconds(const TInstant& v) {
        return v.Seconds();
    }

    ui64 MicroSeconds(const TInstant& v) {
        return v.MicroSeconds();
    }

    std::pair<ui64, ui64> Int128ToPair(const NYql::NDecimal::TInt128& v) {
        return NYql::NDecimal::MakePair(v);
    }

    TStringBuf BinaryJsonToStringBuf(const TMaybe<NBinaryJson::TBinaryJson>& v) {
        Y_ABORT_UNLESS(v.Defined());
        return TStringBuf(v->Data(), v->Size());
    }

    TStringBuf DyNumberToStringBuf(const TMaybe<TString>& v) {
        Y_ABORT_UNLESS(v.Defined());
        return TStringBuf(*v);
    }

    TStringBuf PgToStringBuf(const NPg::TConvertResult& v) {
        Y_ABORT_UNLESS(!v.Error);
        return v.Str;
    }

    TStringBuf UuidToStringBuf(const TUuidHolder& uuid) {
        return TStringBuf(uuid.Buf.Str, 16);
    }

    template <typename T, typename U = T>
    struct TCellMaker {
        static bool Make(TCell& c, TStringBuf v, TMemoryPool& pool, TString& err, TConverter<T, U> conv = &Implicit<T, U>) {
            T t;
            if (!TryParse<T>(v, t)) {
                err = MakeError<T>();
                return false;
            }

            return Conv(c, t, pool, conv);
        }

        static bool MakeDirect(TCell& c, const T& v, TMemoryPool& pool, TString&, TConverter<T, U> conv = &Implicit<T, U>) {
            return Conv(c, v, pool, conv);
        }

    private:
        static bool Conv(TCell& c, const T& t, TMemoryPool& pool, TConverter<T, U> conv) {
            auto& u = *pool.Allocate<U>();
            u = conv(t);
            c = TCell(reinterpret_cast<const char*>(&u), sizeof(u));

            return true;
        }
    };

    template <typename T>
    struct TCellMaker<T, TStringBuf> {
        static bool Make(TCell& c, TStringBuf v, TMemoryPool& pool, TString& err, TConverter<T, TStringBuf> conv = &Implicit<T, TStringBuf>) {
            T t;
            if (!TryParse<T>(v, t)) {
                err = MakeError<T>();
                return false;
            }

            return Conv(c, t, pool, conv);
        }

        static bool MakeDirect(TCell& c, const T& v, TMemoryPool& pool, TString&, TConverter<T, TStringBuf> conv = &Implicit<T, TStringBuf>) {
            return Conv(c, v, pool, conv);
        }

        static bool Make(TCell& c, TStringBuf v, TMemoryPool& pool, TString& err, TConverter<T, TStringBuf> conv, void* parseParam) {
            T t;
            if (!TryParse<T>(v, t, err, parseParam)) {
                return false;
            }

            return Conv(c, t, pool, conv);
        }

    private:
        static bool Conv(TCell& c, const T& t, TMemoryPool& pool, TConverter<T, TStringBuf> conv) {
            const auto u = pool.AppendString(conv(t));
            c = TCell(u.data(), u.size());

            return true;
        }
    };

    NJson::TJsonWriterConfig DefaultJsonConfig() {
        NJson::TJsonWriterConfig jsonConfig;
        jsonConfig.ValidateUtf8 = false;
        jsonConfig.WriteNanAsString = true;
        return jsonConfig;
    }

    TString WriteJson(const NJson::TJsonValue& json) {
        TStringStream str;
        NJson::WriteJson(&str, &json, DefaultJsonConfig());
        return str.Str();
    }

} // anonymous

bool MakeCell(TCell& cell, TStringBuf value, NScheme::TTypeInfo type, TMemoryPool& pool, TString& err) {
    if (value == "null") {
        return true;
    }

    switch (type.GetTypeId()) {
    case NScheme::NTypeIds::Bool:
        return TCellMaker<bool>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Int8:
        return TCellMaker<i8>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Uint8:
        return TCellMaker<ui8>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Int16:
        return TCellMaker<i16>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Uint16:
        return TCellMaker<ui16>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Int32:
        return TCellMaker<i32>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Uint32:
        return TCellMaker<ui32>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Int64:
        return TCellMaker<i64>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Uint64:
        return TCellMaker<ui64>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Float:
        return TCellMaker<float>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Double:
        return TCellMaker<double>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Date:
        return TCellMaker<TInstant, ui16>::Make(cell, value, pool, err, &Days);
    case NScheme::NTypeIds::Datetime:
        return TCellMaker<TInstant, ui32>::Make(cell, value, pool, err, &Seconds);
    case NScheme::NTypeIds::Timestamp:
        return TCellMaker<TInstant, ui64>::Make(cell, value, pool, err, &MicroSeconds);
    case NScheme::NTypeIds::Interval:
        return TCellMaker<i64>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Date32:
        return TCellMaker<i32>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
    case NScheme::NTypeIds::Interval64:
        return TCellMaker<i64>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::String:
    case NScheme::NTypeIds::String4k:
    case NScheme::NTypeIds::String2m:
    case NScheme::NTypeIds::Utf8:
    case NScheme::NTypeIds::Yson:
    case NScheme::NTypeIds::Json:
        return TCellMaker<TString, TStringBuf>::Make(cell, value, pool, err);
    case NScheme::NTypeIds::JsonDocument:
        return TCellMaker<TMaybe<NBinaryJson::TBinaryJson>, TStringBuf>::Make(cell, value, pool, err, &BinaryJsonToStringBuf);
    case NScheme::NTypeIds::DyNumber:
        return TCellMaker<TMaybe<TString>, TStringBuf>::Make(cell, value, pool, err, &DyNumberToStringBuf);
    case NScheme::NTypeIds::Decimal:
        return TCellMaker<NYql::NDecimal::TInt128, std::pair<ui64, ui64>>::Make(cell, value, pool, err, &Int128ToPair);
    case NScheme::NTypeIds::Pg:
        return TCellMaker<NPg::TConvertResult, TStringBuf>::Make(cell, value, pool, err, &PgToStringBuf, type.GetTypeDesc());
    case NScheme::NTypeIds::Uuid:
        return TCellMaker<TUuidHolder, TStringBuf>::Make(cell, value, pool, err, &UuidToStringBuf);
    default:
        return false;
    }
}

bool MakeCell(TCell& cell, const NJson::TJsonValue& value, NScheme::TTypeInfo type, TMemoryPool& pool, TString& err) {
    if (value.IsNull()) {
        return true;
    }

    try {
        switch (type.GetTypeId()) {
        case NScheme::NTypeIds::Bool:
            return TCellMaker<bool>::MakeDirect(cell, value.GetBooleanSafe(), pool, err);
        case NScheme::NTypeIds::Int8:
            return TCellMaker<i8>::MakeDirect(cell, value.GetIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Uint8:
            return TCellMaker<ui8>::MakeDirect(cell, value.GetUIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Int16:
            return TCellMaker<i16>::MakeDirect(cell, value.GetIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Uint16:
            return TCellMaker<ui16>::MakeDirect(cell, value.GetUIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Int32:
            return TCellMaker<i32>::MakeDirect(cell, value.GetIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Uint32:
            return TCellMaker<ui32>::MakeDirect(cell, value.GetUIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Int64:
            return TCellMaker<i64>::MakeDirect(cell, value.GetIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Uint64:
            return TCellMaker<ui64>::MakeDirect(cell, value.GetUIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Float:
            return TCellMaker<float>::MakeDirect(cell, value.GetDoubleSafe(), pool, err);
        case NScheme::NTypeIds::Double:
            return TCellMaker<double>::MakeDirect(cell, value.GetDoubleSafe(), pool, err);
        case NScheme::NTypeIds::Date:
            return TCellMaker<TInstant, ui16>::Make(cell, value.GetStringSafe(), pool, err, &Days);
        case NScheme::NTypeIds::Datetime:
            return TCellMaker<TInstant, ui32>::Make(cell, value.GetStringSafe(), pool, err, &Seconds);
        case NScheme::NTypeIds::Timestamp:
            return TCellMaker<TInstant, ui64>::Make(cell, value.GetStringSafe(), pool, err, &MicroSeconds);
        case NScheme::NTypeIds::Interval:
            return TCellMaker<i64>::MakeDirect(cell, value.GetIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Date32:
            return TCellMaker<i32>::MakeDirect(cell, value.GetIntegerSafe(), pool, err);
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
            return TCellMaker<i64>::MakeDirect(cell, value.GetIntegerSafe(), pool, err);
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
            return TCellMaker<TString, TStringBuf>::MakeDirect(cell, Base64Decode(value.GetStringSafe()), pool, err);
        case NScheme::NTypeIds::Utf8:
            return TCellMaker<TString, TStringBuf>::MakeDirect(cell, value.GetStringSafe(), pool, err);
        case NScheme::NTypeIds::Yson:
            return TCellMaker<TString, TStringBuf>::MakeDirect(cell, NJson2Yson::SerializeJsonValueAsYson(value), pool, err);
        case NScheme::NTypeIds::Json:
            return TCellMaker<TString, TStringBuf>::MakeDirect(cell, NFormats::WriteJson(value), pool, err);
        case NScheme::NTypeIds::JsonDocument:
            if (const auto& result = NBinaryJson::SerializeToBinaryJson(NFormats::WriteJson(value))) {
                return TCellMaker<TMaybe<NBinaryJson::TBinaryJson>, TStringBuf>::MakeDirect(cell, result, pool, err, &BinaryJsonToStringBuf);
            } else {
                return false;
            }
        case NScheme::NTypeIds::DyNumber:
            return TCellMaker<TMaybe<TString>, TStringBuf>::Make(cell, value.GetStringSafe(), pool, err, &DyNumberToStringBuf);
        case NScheme::NTypeIds::Decimal:
            return TCellMaker<NYql::NDecimal::TInt128, std::pair<ui64, ui64>>::Make(cell, value.GetStringSafe(), pool, err, &Int128ToPair);
        case NScheme::NTypeIds::Uuid:
            return TCellMaker<TUuidHolder, TStringBuf>::Make(cell, value.GetStringSafe(), pool, err, &UuidToStringBuf);
        default:
            return false;
        }
    } catch (const yexception&) {
        return false;
    }
}

bool CheckCellValue(const TCell& cell, NScheme::TTypeInfo type) {
    if (cell.IsNull()) {
        return true;
    }

    switch (type.GetTypeId()) {
    case NScheme::NTypeIds::Bool:
    case NScheme::NTypeIds::Int8:
    case NScheme::NTypeIds::Uint8:
    case NScheme::NTypeIds::Int16:
    case NScheme::NTypeIds::Uint16:
    case NScheme::NTypeIds::Int32:
    case NScheme::NTypeIds::Uint32:
    case NScheme::NTypeIds::Int64:
    case NScheme::NTypeIds::Uint64:
    case NScheme::NTypeIds::Float:
    case NScheme::NTypeIds::Double:
    case NScheme::NTypeIds::String:
    case NScheme::NTypeIds::String4k:
    case NScheme::NTypeIds::String2m:
    case NScheme::NTypeIds::JsonDocument: // checked at parsing time
    case NScheme::NTypeIds::DyNumber: // checked at parsing time
    case NScheme::NTypeIds::Pg:       // checked at parsing time
    case NScheme::NTypeIds::Uuid:       // checked at parsing time
        return true;
    case NScheme::NTypeIds::Date:
        return cell.AsValue<ui16>() < NUdf::MAX_DATE;
    case NScheme::NTypeIds::Datetime:
        return cell.AsValue<ui32>() < NUdf::MAX_DATETIME;
    case NScheme::NTypeIds::Timestamp:
        return cell.AsValue<ui64>() < NUdf::MAX_TIMESTAMP;
    case NScheme::NTypeIds::Interval:
        return (ui64)std::abs(cell.AsValue<i64>()) < NUdf::MAX_TIMESTAMP;
    case NScheme::NTypeIds::Date32:
        return cell.AsValue<i32>() < NUdf::MAX_DATE32;
    case NScheme::NTypeIds::Datetime64:
        return cell.AsValue<i64>() < NUdf::MAX_DATETIME64;
    case NScheme::NTypeIds::Timestamp64:
        return cell.AsValue<i64>() < NUdf::MAX_TIMESTAMP64;
    case NScheme::NTypeIds::Interval64:
        return std::abs(cell.AsValue<i64>()) < NUdf::MAX_INTERVAL64;
    case NScheme::NTypeIds::Utf8:
        return NYql::IsUtf8(cell.AsBuf());
    case NScheme::NTypeIds::Yson:
        return NYql::NDom::IsValidYson(cell.AsBuf());
    case NScheme::NTypeIds::Json:
        return NYql::NDom::IsValidJson(cell.AsBuf());
    case NScheme::NTypeIds::Decimal:
        return !NYql::NDecimal::IsError(cell.AsValue<NYql::NDecimal::TInt128>());
    default:
        return false;
    }
}

}
