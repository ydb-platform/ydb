#include "ydb_value_operator.h"

namespace NKikimr::NMetadata::NInternal {

bool TYDBValue::IsSameType(const Ydb::Value& v, const Ydb::Type& type) {
    Y_ABORT_UNLESS(type.has_type_id());
    if (type.type_id() == Ydb::Type::BOOL) {
        return v.has_bool_value();
    } else if (type.type_id() == Ydb::Type::INT32) {
        return v.has_int32_value();
    } else if (type.type_id() == Ydb::Type::UINT32) {
        return v.has_uint32_value();
    } else if (type.type_id() == Ydb::Type::INT64) {
        return v.has_int64_value();
    } else if (type.type_id() == Ydb::Type::UINT64) {
        return v.has_uint64_value();
    } else if (type.type_id() == Ydb::Type::STRING) {
        return v.has_bytes_value();
    } else if (type.type_id() == Ydb::Type::UTF8) {
        return v.has_text_value();
    }
    Y_ABORT_UNLESS(false);
}

bool TYDBValue::IsSameType(const Ydb::Value& l, const Ydb::Value& r) {
    if (l.has_bool_value()) {
        return r.has_bool_value();
    }
    if (l.has_bytes_value()) {
        return r.has_bytes_value();
    }
    if (l.has_text_value()) {
        return r.has_text_value();
    }
    Y_ABORT_UNLESS(false);
}

bool TYDBValue::Compare(const Ydb::Value& l, const Ydb::Value& r) {
    if (!IsSameType(l, r)) {
        return false;
    }
    if (l.has_bool_value()) {
        return l.bool_value() == r.bool_value();
    }
    if (l.has_bytes_value()) {
        return l.bytes_value() == r.bytes_value();
    }
    if (l.has_text_value()) {
        return l.text_value() == r.text_value();
    }
    Y_ABORT_UNLESS(false);
}

TString TYDBValue::TypeToString(const Ydb::Type& type) {
    Y_ABORT_UNLESS(type.has_type_id());
    if (type.type_id() == Ydb::Type::BOOL) {
        return "Bool";
    } else if (type.type_id() == Ydb::Type::INT32) {
        return "Int32";
    } else if (type.type_id() == Ydb::Type::UINT32) {
        return "Uint32";
    } else if (type.type_id() == Ydb::Type::INT64) {
        return "Uint64";
    } else if (type.type_id() == Ydb::Type::UINT64) {
        return "Uint64";
    } else if (type.type_id() == Ydb::Type::STRING) {
        return "String";
    } else if (type.type_id() == Ydb::Type::UTF8) {
        return "Utf8";
    } else {
        Y_ABORT_UNLESS(false);
    }
}

Ydb::Value TYDBValue::NullValue() {
    Ydb::Value result;
    result.set_null_flag_value(::google::protobuf::NULL_VALUE);
    return result;
}

Ydb::Value TYDBValue::RawBytes(const TString& value) {
    Ydb::Value result;
    result.set_bytes_value(value);
    return result;
}

Ydb::Value TYDBValue::RawBytes(const TStringBuf& value) {
    Ydb::Value result;
    result.set_bytes_value(TString(value.data(), value.size()));
    return result;
}

Ydb::Value TYDBValue::RawBytes(const char* value) {
    Ydb::Value result;
    result.set_bytes_value(TString(value));
    return result;
}

Ydb::Value TYDBValue::Utf8(const TString& value) {
    Ydb::Value result;
    result.set_text_value(value);
    return result;
}

Ydb::Value TYDBValue::Utf8(const TStringBuf& value) {
    Ydb::Value result;
    result.set_text_value(TString(value.data(), value.size()));
    return result;
}

Ydb::Value TYDBValue::Utf8(const char* value) {
    Ydb::Value result;
    result.set_text_value(TString(value));
    return result;
}

Ydb::Value TYDBValue::UInt64(const ui64 value) {
    Ydb::Value result;
    result.set_uint64_value(value);
    return result;
}

Ydb::Value TYDBValue::Int64(const i64 value) {
    Ydb::Value result;
    result.set_int64_value(value);
    return result;
}

Ydb::Value TYDBValue::Bool(const bool value) {
    Ydb::Value result;
    result.set_bool_value(value);
    return result;
}

Ydb::Value TYDBValue::UInt32(const ui32 value) {
    Ydb::Value result;
    result.set_uint32_value(value);
    return result;
}

Ydb::Column TYDBColumn::RawBytes(const TString& columnId) {
    Ydb::Column result;
    result.set_name(columnId);
    result.mutable_type()->set_type_id(Ydb::Type::STRING);
    return result;
}

Ydb::Column TYDBColumn::Utf8(const TString& columnId) {
    Ydb::Column result;
    result.set_name(columnId);
    result.mutable_type()->set_type_id(Ydb::Type::UTF8);
    return result;
}

Ydb::Column TYDBColumn::Boolean(const TString& columnId) {
    Ydb::Column result;
    result.set_name(columnId);
    result.mutable_type()->set_type_id(Ydb::Type::BOOL);
    return result;
}

Ydb::Column TYDBColumn::UInt64(const TString& columnId) {
    Ydb::Column result;
    result.set_name(columnId);
    result.mutable_type()->set_type_id(Ydb::Type::UINT64);
    return result;
}

Ydb::Column TYDBColumn::UInt32(const TString& columnId) {
    Ydb::Column result;
    result.set_name(columnId);
    result.mutable_type()->set_type_id(Ydb::Type::UINT32);
    return result;
}

Ydb::Type TYDBType::Primitive(const Ydb::Type::PrimitiveTypeId type) {
    Ydb::Type result;
    result.set_type_id(type);
    return result;
}

std::optional<Ydb::Type::PrimitiveTypeId> TYDBType::ConvertArrowToYDB(const arrow::Type::type type) {
    switch (type) {
        case arrow::Type::INT64:
            return Ydb::Type::INT64;
        case arrow::Type::INT32:
            return Ydb::Type::INT32;
        case arrow::Type::INT16:
            return Ydb::Type::INT16;
        case arrow::Type::INT8:
            return Ydb::Type::INT8;
        case arrow::Type::STRING:
            return Ydb::Type::UTF8;
        case arrow::Type::BINARY:
            return Ydb::Type::STRING;
        case arrow::Type::UINT64:
            return Ydb::Type::UINT64;
        case arrow::Type::UINT32:
            return Ydb::Type::UINT32;
        case arrow::Type::UINT16:
            return Ydb::Type::UINT16;
        case arrow::Type::UINT8:
            return Ydb::Type::UINT8;
        case arrow::Type::TIMESTAMP:
            return Ydb::Type::TIMESTAMP;
        default:
            return {};
    }
}

std::optional<Ydb::Type::PrimitiveTypeId> TYDBType::ConvertYQLToYDB(const NScheme::TTypeId type) {
    switch (type) {
        case NScheme::NTypeIds::Int8:
            return Ydb::Type::INT8;
        case NScheme::NTypeIds::Int16:
            return Ydb::Type::INT16;
        case NScheme::NTypeIds::Int32:
            return Ydb::Type::INT32;
        case NScheme::NTypeIds::Int64:
            return Ydb::Type::INT64;
        case NScheme::NTypeIds::Uint8:
            return Ydb::Type::UINT8;
        case NScheme::NTypeIds::Uint16:
            return Ydb::Type::UINT16;
        case NScheme::NTypeIds::Uint32:
            return Ydb::Type::UINT32;
        case NScheme::NTypeIds::Uint64:
            return Ydb::Type::UINT64;
        case NScheme::NTypeIds::String:
            return Ydb::Type::STRING;
        case NScheme::NTypeIds::Utf8:
            return Ydb::Type::UTF8;
        case NScheme::NTypeIds::Timestamp:
            return Ydb::Type::TIMESTAMP;
        case NScheme::NTypeIds::Timestamp64:
            return Ydb::Type::TIMESTAMP64;
        case NScheme::NTypeIds::Date32:
            return Ydb::Type::DATE32;
        case NScheme::NTypeIds::Datetime64:
            return Ydb::Type::DATETIME64;
        case NScheme::NTypeIds::Interval64:
            return Ydb::Type::INTERVAL64;
        default:
            return {};
    }
}

std::optional<NKikimr::NScheme::TTypeId> TYDBType::ConvertYDBToYQL(const Ydb::Type::PrimitiveTypeId type) {
    switch (type) {
        case Ydb::Type::INT8:
            return NScheme::NTypeIds::Int8;
        case Ydb::Type::INT16:
            return NScheme::NTypeIds::Int16;
        case Ydb::Type::INT32:
            return NScheme::NTypeIds::Int32;
        case Ydb::Type::INT64:
            return NScheme::NTypeIds::Int64;
        case Ydb::Type::UINT8:
            return NScheme::NTypeIds::Uint8;
        case Ydb::Type::UINT16:
            return NScheme::NTypeIds::Uint16;
        case Ydb::Type::UINT32:
            return NScheme::NTypeIds::Uint32;
        case Ydb::Type::UINT64:
            return NScheme::NTypeIds::Uint64;
        case Ydb::Type::STRING:
            return NScheme::NTypeIds::String;
        case Ydb::Type::UTF8:
            return NScheme::NTypeIds::Utf8;
        case Ydb::Type::TIMESTAMP:
            return NScheme::NTypeIds::Timestamp;
        case Ydb::Type::DATE32:
            return NScheme::NTypeIds::Date32;
        case Ydb::Type::DATETIME64:
            return NScheme::NTypeIds::Datetime64;
        case Ydb::Type::TIMESTAMP64:
            return NScheme::NTypeIds::Timestamp64;
        case Ydb::Type::INTERVAL64:
            return NScheme::NTypeIds::Interval64;
        default:
            return {};
    }
}

std::optional<TVector<std::pair<TString, NScheme::TTypeInfo>>> TYDBType::ConvertYDBToYQL(const std::vector<std::pair<TString, Ydb::Type>>& input) {
    TVector<std::pair<TString, NScheme::TTypeInfo>> resultLocal;
    resultLocal.reserve(input.size());
    for (auto&& i : input) {
        if (!i.second.has_type_id()) {
            return {};
        }
        auto yqlId = ConvertYDBToYQL(i.second.type_id());
        if (!yqlId) {
            return {};
        }
        resultLocal.emplace_back(std::make_pair(i.first, NScheme::TTypeInfo(*yqlId)));
    }
    return resultLocal;
}

}
