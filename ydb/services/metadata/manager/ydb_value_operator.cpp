#include "ydb_value_operator.h"

namespace NKikimr::NMetadata::NInternal {

bool TYDBValue::IsSameType(const Ydb::Value& v, const Ydb::Type& type) {
    Y_VERIFY(type.has_type_id());
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
    Y_VERIFY(false);
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
    Y_VERIFY(false);
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
    Y_VERIFY(false);
}

TString TYDBValue::TypeToString(const Ydb::Type& type) {
    Y_VERIFY(type.has_type_id());
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
        Y_VERIFY(false);
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

}
