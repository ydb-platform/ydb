#include "types.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/generic/ylimits.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

namespace NKikimr::NSql {

namespace {

TResolvedType Ok(TString name) {
    TResolvedType type;
    type.Name = std::move(name);
    return type;
}

TResolvedType Reject(TString message) {
    TResolvedType type;
    type.SqlState = "0A000";
    type.Message = std::move(message);
    return type;
}

bool Same(const TString& name, TStringBuf expected) {
    return name == expected;
}

} // namespace

TResolvedType ResolveColumnType(const TPgType& type) {
    if (type.Quoted) {
        if (type.Precision && type.Scale) {
            return Ok(Sprintf("%s(%u,%u)", type.Name.c_str(), *type.Precision, *type.Scale));
        }
        if (type.Precision) {
            return Ok(Sprintf("%s(%u)", type.Name.c_str(), *type.Precision));
        }
        return Ok(type.Name);
    }

    const TString& name = type.Name;
    if (Same(name, "bool") || Same(name, "boolean")) {
        return Ok("Bool");
    }
    if (Same(name, "smallint") || Same(name, "int2")) {
        return Ok("Int16");
    }
    if (Same(name, "integer") || Same(name, "int") || Same(name, "int4")) {
        return Ok("Int32");
    }
    if (Same(name, "bigint") || Same(name, "int8")) {
        return Ok("Int64");
    }
    if (Same(name, "real") || Same(name, "float4")) {
        return Ok("Float");
    }
    if (Same(name, "double precision") || Same(name, "float8")) {
        return Ok("Double");
    }
    if (Same(name, "text") || Same(name, "varchar") || Same(name, "character varying")
        || Same(name, "char") || Same(name, "character") || Same(name, "bpchar"))
    {
        return Ok("Utf8");
    }
    if (Same(name, "bytea")) {
        return Ok("String");
    }
    if (Same(name, "uuid")) {
        return Ok("Uuid");
    }
    if (Same(name, "json")) {
        return Ok("Json");
    }
    if (Same(name, "jsonb")) {
        return Ok("JsonDocument");
    }
    if (Same(name, "numeric") || Same(name, "decimal")) {
        if (!type.Precision) {
            return Ok(Sprintf("Decimal(%u,%u)", NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE));
        }
        const ui32 precision = *type.Precision;
        const ui32 scale = type.Scale ? *type.Scale : 0;
        if (precision < 1 || precision > 35 || scale > precision) {
            return Reject(name);
        }
        return Ok(Sprintf("Decimal(%u,%u)", precision, scale));
    }
    if (Same(name, "date")) {
        return Reject("date");
    }
    if (Same(name, "timestamp") || Same(name, "timestamp without time zone")) {
        if (type.Precision && *type.Precision == 0) {
            return Ok("Datetime64");
        }
        return Ok("Timestamp64");
    }
    if (Same(name, "interval")) {
        return Ok("Interval64");
    }
    return Reject(name);
}

bool FillDefaultLiteral(Ydb::TypedValue& value, const TString& ydbType, const TLiteral& literal, TString& message) {
    auto reject = [&]() {
        message = "default";
        return false;
    };
    auto setInt = [&](Ydb::Type::PrimitiveTypeId typeId, i64 minValue, i64 maxValue) {
        if (literal.Kind != TLiteral::EKind::Integer) {
            return reject();
        }
        i64 number = 0;
        if (!TryFromString(literal.Text, number) || number < minValue || number > maxValue) {
            return reject();
        }
        value.mutable_type()->set_type_id(typeId);
        if (typeId == Ydb::Type::INT64) {
            value.mutable_value()->set_int64_value(number);
        } else {
            value.mutable_value()->set_int32_value(static_cast<i32>(number));
        }
        return true;
    };
    if (ydbType == "Int8") {
        return setInt(Ydb::Type::INT8, Min<i8>(), Max<i8>());
    }
    if (ydbType == "Int16") {
        return setInt(Ydb::Type::INT16, Min<i16>(), Max<i16>());
    }
    if (ydbType == "Int32") {
        return setInt(Ydb::Type::INT32, Min<i32>(), Max<i32>());
    }
    if (ydbType == "Int64") {
        return setInt(Ydb::Type::INT64, Min<i64>(), Max<i64>());
    }
    if (ydbType == "Utf8" || ydbType == "Json" || ydbType == "JsonDocument" || ydbType == "String") {
        if (literal.Kind != TLiteral::EKind::String) {
            return reject();
        }
        Ydb::Type::PrimitiveTypeId typeId = Ydb::Type::UTF8;
        if (ydbType == "Json") {
            typeId = Ydb::Type::JSON;
        } else if (ydbType == "JsonDocument") {
            typeId = Ydb::Type::JSON_DOCUMENT;
        } else if (ydbType == "String") {
            typeId = Ydb::Type::STRING;
        }
        value.mutable_type()->set_type_id(typeId);
        if (ydbType == "String") {
            value.mutable_value()->set_bytes_value(literal.Text);
        } else {
            value.mutable_value()->set_text_value(literal.Text);
        }
        return true;
    }
    return reject();
}

} // namespace NKikimr::NSql
