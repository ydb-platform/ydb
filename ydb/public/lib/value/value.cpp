#include "value.h"

#include <ydb/library/yql/public/decimal/yql_decimal.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/charset/utf8.h>
#include <util/string/cast.h>
#include <util/string/escape.h>
#include <util/string/printf.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

namespace NKikimr {
namespace NClient {

const NKikimrMiniKQL::TValue TValue::Null;

TValue::TValue(NKikimrMiniKQL::TValue& value, NKikimrMiniKQL::TType& type)
    : Type(type)
    , Value(value)
{}

TValue TValue::Create(const NKikimrMiniKQL::TValue& value, const NKikimrMiniKQL::TType& type) {
    return TValue(const_cast<NKikimrMiniKQL::TValue&>(value), const_cast<NKikimrMiniKQL::TType&>(type));
}

TValue TValue::Create(const NKikimrMiniKQL::TResult& result) {
    return TValue::Create(result.GetValue(), result.GetType());
}

bool TValue::HaveValue() const {
    return !IsNull();
}

bool TValue::IsNull() const {
    if (&Value == &Null)
        return true;
    return Value.ByteSize() == 0;
}

TValue TValue::operator [](const char* name) const {
    return this->operator [](TStringBuf(name));
}

int TValue::GetMemberIndex(TStringBuf name) const {
    // TODO: add support for Dict
    Y_ASSERT(Type.HasStruct());
    const auto& structField = Type.GetStruct();
    size_t size = structField.MemberSize();
    for (size_t n = 0; n < size; ++n) {
        const auto& memberField = structField.GetMember(n);
        if (memberField.GetName() == name) {
            return n;
        }
    }
    ythrow yexception() << "Unknown Struct member name: " << name;
}

TValue TValue::operator [](const TStringBuf name) const {
    // TODO: add support for Dict
    Y_ASSERT(Type.HasStruct());
    const auto& structField = Type.GetStruct();
    size_t size = structField.MemberSize();
    for (size_t n = 0; n < size; ++n) {
        const auto& memberField = structField.GetMember(n);
        if (memberField.GetName() == name) {
            if (!HaveValue()) {
                return TValue::Create(Null, memberField.GetType()).EatOptional();
            }
            Y_ASSERT(Value.StructSize() > n);
            return TValue::Create(Value.GetStruct(n), memberField.GetType()).EatOptional();
        }
    }
    ythrow yexception() << "Unknown Struct member name: " << name;
}

TValue TValue::operator [](int index) const {
    Y_ASSERT(Type.HasList() || Type.HasTuple() || Type.HasStruct());
    if (Type.HasList()) {
        Y_ASSERT(Value.ListSize() > (size_t)index);
        return TValue::Create(Value.GetList(index), Type.GetList().GetItem()).EatOptional();
    } else if (Type.HasTuple()) {
        Y_ASSERT(Value.TupleSize() > (size_t)index);
        return TValue::Create(Value.GetTuple(index), Type.GetTuple().GetElement(index)).EatOptional();
    } else {
        Y_ASSERT(Value.StructSize() > (size_t)index);
        return TValue::Create(Value.GetStruct(index), Type.GetStruct().GetMember(index).GetType()).EatOptional();
    }
}

TString TValue::GetMemberName(int index) const {
    Y_ASSERT(Type.HasStruct());
    const auto& structField = Type.GetStruct();
    size_t size = structField.MemberSize();
    Y_ASSERT((size_t)index < size);
    return structField.GetMember(index).GetName();
}

TVector<TString> TValue::GetMembersNames() const {
    Y_ASSERT(Type.HasStruct());
    const auto& structField = Type.GetStruct();
    size_t size = structField.MemberSize();
    TVector<TString> members;
    members.reserve(size);
    for (int index = 0; (size_t)index < size; ++index) {
        members.emplace_back(structField.GetMember(index).GetName());
    }
    return members;
}

TWriteValue TWriteValue::Create(NKikimrMiniKQL::TValue& value, NKikimrMiniKQL::TType& type) {
    return TWriteValue(value, type);
}

TWriteValue::TWriteValue(NKikimrMiniKQL::TValue& value, NKikimrMiniKQL::TType& type)
    : TValue(value, type)
{}

TWriteValue TWriteValue::operator [](const char* name) {
    return this->operator [](TStringBuf(name));
}

TWriteValue TWriteValue::operator [](const TStringBuf name) {
    Y_ASSERT(!Type.HasList() && !Type.HasTuple() && !Type.HasDict());
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    auto& structField = *Type.MutableStruct();
    size_t size = structField.MemberSize();
    for (size_t n = 0; n < size; ++n) {
        auto& memberField = *structField.MutableMember(n);
        if (memberField.GetName() == name) {
            if (Value.StructSize() <= n) {
                auto& str = *Value.MutableStruct();
                str.Reserve(n + 1);
                while (Value.StructSize() <= n) {
                    str.Add();
                }
            }
            return TWriteValue::Create(*Value.MutableStruct(n), *memberField.MutableType());
        }
    }
    auto& memberField = *structField.AddMember();
    memberField.SetName(TString(name));
    return TWriteValue::Create(*Value.AddStruct(), *memberField.MutableType());
}

TWriteValue TWriteValue::operator [](int index) {
    Y_ASSERT(!Type.HasDict() && !Type.HasStruct());
    if (!Type.HasTuple()) {
        Type.SetKind(NKikimrMiniKQL::ETypeKind::List);
        NKikimrMiniKQL::TType* type = Type.MutableList()->MutableItem();
        NKikimrMiniKQL::TValue* value = Value.MutableList(index);
        return TWriteValue::Create(*value, *type);
    } else {
        Type.SetKind(NKikimrMiniKQL::ETypeKind::Tuple);
        NKikimrMiniKQL::TType* type = Type.MutableTuple()->MutableElement(index);
        NKikimrMiniKQL::TValue* value = Value.MutableTuple(index);
        return TWriteValue::Create(*value, *type);
    }
}

TWriteValue TWriteValue::Optional() {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Optional);
    return TWriteValue::Create(*Value.MutableOptional(), *Type.MutableOptional()->MutableItem());
}

TWriteValue TWriteValue::AddListItem() {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::List);
    return TWriteValue::Create(*Value.AddList(), *Type.MutableList()->MutableItem());
}

TWriteValue TWriteValue::AddTupleItem() {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Tuple);
    NKikimrMiniKQL::TType* type = Type.MutableTuple()->AddElement();
    NKikimrMiniKQL::TValue* value = Value.AddTuple();
    return TWriteValue::Create(*value, *type);
}

TWriteValue& TWriteValue::Void() {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Void);
    return *this;
}

TWriteValue& TWriteValue::Bytes(const char* value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::String);
    Value.SetBytes(value);
    return *this;
}

TWriteValue& TWriteValue::Bytes(const TString& value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::String);
    Value.SetBytes(value);
    return *this;
}

TWriteValue& TWriteValue::Yson(const char* value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Yson);
    Value.SetBytes(value);
    return *this;
}

TWriteValue& TWriteValue::Yson(const TString& value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Yson);
    Value.SetBytes(value);
    return *this;
}

TWriteValue& TWriteValue::Json(const char* value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Json);
    Value.SetText(value);
    return *this;
}

TWriteValue& TWriteValue::Json(const TString& value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Json);
    Value.SetText(value);
    return *this;
}

TWriteValue& TWriteValue::Date(ui16 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Date);
    Value.SetUint32(value);
    return *this;
}

TWriteValue& TWriteValue::Datetime(ui32 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Datetime);
    Value.SetUint32(value);
    return *this;
}

TWriteValue& TWriteValue::Timestamp(ui64 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Timestamp);
    Value.SetUint64(value);
    return *this;
}

TWriteValue& TWriteValue::Interval(i64 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Interval);
    Value.SetInt64(value);
    return *this;
}

TWriteValue& TWriteValue::Date32(i32 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Date32);
    Value.SetInt32(value);
    return *this;
}

TWriteValue& TWriteValue::Datetime64(i64 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Datetime64);
    Value.SetInt64(value);
    return *this;
}

TWriteValue& TWriteValue::Timestamp64(i64 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Timestamp64);
    Value.SetInt64(value);
    return *this;
}

TWriteValue& TWriteValue::Interval64(i64 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Interval);
    Value.SetInt64(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(bool value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Bool);
    Value.SetBool(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(ui8 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Uint8);
    Value.SetUint32(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(i8 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Int8);
    Value.SetInt32(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(ui16 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Uint16);
    Value.SetUint32(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(i16 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Int16);
    Value.SetInt32(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(ui64 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Uint64);
    Value.SetUint64(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(i64 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Int64);
    Value.SetInt64(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(ui32 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Uint32);
    Value.SetUint32(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(i32 value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Int32);
    Value.SetInt32(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(double value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Double);
    Value.SetDouble(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(float value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Float);
    Value.SetFloat(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(const TString& value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Utf8);
    Value.SetText(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(const char* value) {
    Type.SetKind(NKikimrMiniKQL::ETypeKind::Data);
    Type.MutableData()->SetScheme(NScheme::NTypeIds::Utf8);
    Value.SetText(value);
    return *this;
}

TWriteValue& TWriteValue::operator =(const TValue& value) {
    Type.CopyFrom(value.GetType());
    Value.CopyFrom(value.GetValue());
    return *this;
}

ui32 TWriteValue::GetValueBytesSize() const {
    return (ui32)Value.ByteSize();
}

ui32 TWriteValue::GetTypeBytesSize() const {
    return (ui32)Type.ByteSize();
}

ui32 TWriteValue::GetBytesSize() const {
    return GetValueBytesSize() + GetTypeBytesSize();
}

size_t TValue::Size() const {
    if (Type.HasList())
        return Value.ListSize();
    if (Type.HasTuple())
        return Value.TupleSize();
    if (Type.HasStruct())
        return Value.StructSize();
    return 0;
}

NScheme::TTypeId TValue::GetDataType() const {
    // TODO: support for complex types
    Y_ASSERT(Type.GetKind() == NKikimrMiniKQL::ETypeKind::Data);
    return Type.GetData().GetScheme();
}

TString TValue::GetDataText() const {
    Y_ASSERT(Type.GetKind() == NKikimrMiniKQL::ETypeKind::Data);
    switch (Type.GetData().GetScheme()) {
    case NScheme::NTypeIds::Bool:
        return Value.GetBool() ? "true" : "false";
    case NScheme::NTypeIds::Uint64:
        return ToString(Value.GetUint64());
    case NScheme::NTypeIds::Int64:
        return ToString(Value.GetInt64());
    case NScheme::NTypeIds::Uint32:
    case NScheme::NTypeIds::Uint16:
    case NScheme::NTypeIds::Uint8:
        return ToString(Value.GetUint32());
    case NScheme::NTypeIds::Int32:
    case NScheme::NTypeIds::Int16:
    case NScheme::NTypeIds::Int8:
        return ToString(Value.GetInt32());
    case NScheme::NTypeIds::Double:
        return ToString(Value.GetDouble());
    case NScheme::NTypeIds::Float:
        return ToString(Value.GetFloat());
    case NScheme::NTypeIds::Utf8:
    case NScheme::NTypeIds::Json:
        return Value.GetText();
    case NScheme::NTypeIds::String:
    case NScheme::NTypeIds::String4k:
    case NScheme::NTypeIds::String2m:
        return Value.GetBytes();
    case NScheme::NTypeIds::Decimal:
        {
            NYql::NDecimal::TInt128 val;
            auto p = reinterpret_cast<char*>(&val);
            reinterpret_cast<ui64*>(p)[0] = Value.GetLow128();
            reinterpret_cast<ui64*>(p)[1] = Value.GetHi128();
            return NYql::NDecimal::ToString(val,
                                            Type.GetData().GetDecimalParams().GetPrecision(),
                                            Type.GetData().GetDecimalParams().GetScale());
        }
    case NScheme::NTypeIds::Date:
    case NScheme::NTypeIds::Datetime:
        return ToString(Value.GetUint32());
    case NScheme::NTypeIds::Timestamp:
        return ToString(Value.GetUint64());
    case NScheme::NTypeIds::Interval:
        return ToString(Value.GetInt64());
    case NScheme::NTypeIds::Date32:
        return ToString(Value.GetInt32());
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
    case NScheme::NTypeIds::Interval64:
        return ToString(Value.GetInt64());        
    case NScheme::NTypeIds::JsonDocument:
        return "\"<JsonDocument>\"";
    case NScheme::NTypeIds::Uuid:
        {
            NYdb::TUuidValue val(Value.GetLow128(), Value.GetHi128());
            return val.ToString();
        }
    }

    return TStringBuilder() << "\"<unknown type "  << Type.GetData().GetScheme() << ">\"";
}

TString TValue::GetPgText() const {
    Y_ASSERT(Type.GetKind() == NKikimrMiniKQL::ETypeKind::Pg);
    if (Value.HasNullFlagValue()) {
        return TString("null");
    }
    Y_ENSURE(Value.HasText());
    return Value.GetText();
}

TString TValue::GetSimpleValueText() const {
    if (Type.GetKind() == NKikimrMiniKQL::ETypeKind::Pg) {
        return GetPgText();
    }
    if (Type.GetKind() == NKikimrMiniKQL::ETypeKind::Data) {
        return GetDataText();
    }
    Y_ENSURE(false, TStringBuilder() << "unexpected NKikimrMiniKQL::ETypeKind: " << ETypeKind_Name(GetType().GetKind()));
}


template <> TString TValue::GetTypeText<TFormatCxx>(const TFormatCxx& format) const {
    switch(Type.GetKind()) {
    case NKikimrMiniKQL::ETypeKind::Void:
        return "void";
    case NKikimrMiniKQL::ETypeKind::Data:
        return NScheme::TypeName(GetDataType());
    case NKikimrMiniKQL::ETypeKind::Optional:
        return "optional<" + TValue::Create(Null, Type.GetOptional().GetItem()).GetTypeText<TFormatCxx>(format) + ">";
    case NKikimrMiniKQL::ETypeKind::List:
        return "list<" + TValue::Create(Null, Type.GetList().GetItem()).GetTypeText<TFormatCxx>(format) + ">";
    case NKikimrMiniKQL::ETypeKind::Tuple:
    {
        TString typeName = "tuple<";
        const auto& element = Type.GetTuple().GetElement();
        for (auto it = element.begin(); it != element.end(); ++it) {
            if (it != element.begin())
                typeName += ',';
            typeName += TValue::Create(Null, *it).GetTypeText<TFormatCxx>(format);
        }
        typeName += ">";
        return typeName;
    }
    case NKikimrMiniKQL::ETypeKind::Struct:
    {
        TString typeName = "struct{";
        const auto& member = Type.GetStruct().GetMember();
        for (auto it = member.begin(); it != member.end(); ++it) {
            typeName += TValue::Create(Null, it->GetType()).GetTypeText<TFormatCxx>(format);
            typeName += ' ';
            typeName += it->GetName();
            typeName += ';';
        }
        typeName += "}";
        return typeName;
    }
    case NKikimrMiniKQL::ETypeKind::Dict:
        return "dict<" + TValue::Create(Null, Type.GetDict().GetKey()).GetTypeText<TFormatCxx>(format) + "," + TValue::Create(Null, Type.GetDict().GetPayload()).GetTypeText<TFormatCxx>(format) + ">";
    default:
        return "<unknown>";
    }
}

TString EscapeJsonUTF8(const TString& s) {
    TString result;
    result.reserve(s.size());
    const char* b = s.begin();
    const char* e = s.end();
    const char* p = b;
    while (p < e) {
        size_t len = 1;
        RECODE_RESULT res = GetUTF8CharLen(
                    len,
                    reinterpret_cast<const unsigned char*>(p),
                    reinterpret_cast<const unsigned char*>(e)
                    );
        if (res == RECODE_OK && len != 1) {
            len = std::min<decltype(len)>(len, e - p);
            result.append(p, len);
            p += len;
        } else {
            char c = *p;
            if (c < '\x20') {
                result.append(Sprintf("\\u%04x", int(c)));
            } else if (c == '"') {
                result.append("\\\"");
            } else if (c == '\\') {
                result.append("\\\\");
            } else {
                result.append(c);
            }
            ++p;
        }
    }
    return result;
}

TString EscapeJsonASCII(const TString& s) {
    TString result;
    result.reserve(s.size());
    for (char c : s) {
        if (c < '\x20' || c > '\x7f') /*c is signed type, check for >0x7f is not necessary*/ {
            result.append(Sprintf("\\u%04x", static_cast<unsigned int>(c)));
        } else if (c == '"') {
            result.append("\\\"");
        } else if (c == '\\') {
            result.append("\\\\");
        } else {
            result.append(c);
        }
    }
    return result;
}

TString TFormatCSV::EscapeString(const TString& s) {
    TString result;
    result.reserve(s.size());
    const char* b = s.begin();
    const char* e = s.end();
    const char* p = b;
    while (p < e) {
        char c = *p;
        auto len = UTF8RuneLen(c);
        if (len < 2) {
            if (c < ' ') {
                result += Sprintf("\\u%04x", int(c));
            } else if (c == '"') {
                result += "\"\"";
            } else {
                result += c;
            }
            ++p;
        } else {
            len = std::min<decltype(len)>(len, e - p);
            result.append(p, len);
            p += len;
        }
    }
    return result;
}

TString PrintCsvHeader(const NKikimrMiniKQL::TType& type,
                       const TFormatCSV &format) {
    TString hdr = "";

    switch(type.GetKind()) {
    case NKikimrMiniKQL::ETypeKind::Void:
    case NKikimrMiniKQL::ETypeKind::Data:
    case NKikimrMiniKQL::ETypeKind::Optional:
        break;
    case NKikimrMiniKQL::ETypeKind::List:
        return PrintCsvHeader(type.GetList().GetItem(), format);
    case NKikimrMiniKQL::ETypeKind::Tuple:
    case NKikimrMiniKQL::ETypeKind::Dict:
        break;
    case NKikimrMiniKQL::ETypeKind::Struct:
        {
            const auto& member = type.GetStruct().GetMember();
            for (auto it = member.begin(); it != member.end(); ++it) {
                if (it != member.begin())
                    hdr += format.Delim;
                hdr += it->GetName();
            }
            break;
        }
    default:
        hdr = "<unknown>";
        break;
    }

    return hdr;
}

template <> TString TValue::GetValueText<TFormatJSON>(const TFormatJSON& format) const {
    switch(Type.GetKind()) {
    case NKikimrMiniKQL::ETypeKind::Void:
        return "null";
    case NKikimrMiniKQL::ETypeKind::Data: {
        switch (Type.GetData().GetScheme()) {
        case NScheme::NTypeIds::Utf8:
            return "\"" + EscapeJsonUTF8(GetDataText()) + "\"";
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
            if (format.BinaryAsBase64) {
                return "\"" + Base64Encode(GetDataText()) + "\"";
            } else {
                return "\"" + EscapeJsonASCII(GetDataText()) + "\"";
            }
        case NScheme::NTypeIds::Uint64:
        case NScheme::NTypeIds::Int64:
        case NScheme::NTypeIds::Timestamp:
        case NScheme::NTypeIds::Interval:
            return format.UI64AsString ? ("\"" + GetDataText() + "\"") : GetDataText();
        default:
            return GetDataText();
        }
    }
    case NKikimrMiniKQL::ETypeKind::Optional: {
        if (Value.HasOptional()) {
            return TValue::Create(Value.GetOptional(), Type.GetOptional().GetItem()).GetValueText<TFormatJSON>(format);
        } else {
            return "null";
        }
    }
    case NKikimrMiniKQL::ETypeKind::List:
    {
        TString valueText = "[";
        const auto& list = Value.GetList();
        const auto& type = Type.GetList().GetItem();
        for (auto it = list.begin(); it != list.end(); ++it) {
            if (it != list.begin())
                valueText += ", ";
            valueText += TValue::Create(*it, type).GetValueText<TFormatJSON>(format);
        }
        valueText += ']';
        return valueText;
    }
    case NKikimrMiniKQL::ETypeKind::Tuple:
    {
        TString valueText = "[";
        const auto& tuple = Value.GetTuple();
        const auto& type = Type.GetTuple().GetElement();
        size_t maxElements = type.size();
        for (size_t element = 0; element < maxElements; ++element) {
            if (element != 0)
                valueText += ", ";
            valueText += TValue::Create(tuple.Get(element), type.Get(element)).GetValueText<TFormatJSON>(format);
        }
        valueText += ']';
        return valueText;
    }
    case NKikimrMiniKQL::ETypeKind::Struct:
    {
        TString valueText = "{";
        const auto& str = Value.GetStruct();
        const auto& type = Type.GetStruct().GetMember();
        size_t maxElements = type.size();
        for (size_t element = 0; element < maxElements; ++element) {
            if (element != 0)
                valueText += ", ";
            valueText += '"';
            valueText += type.Get(element).GetName();
            valueText += "\": ";
            valueText += TValue::Create(str.Get(element), type.Get(element).GetType()).GetValueText<TFormatJSON>(format);
        }
        valueText += '}';
        return valueText;
    }
    case NKikimrMiniKQL::ETypeKind::Dict:
    {
        TString valueText = "{";
        const auto& dict = Value.GetDict();
        const auto& type = Type.GetDict();
        for (auto it = dict.begin(); it != dict.end(); ++it) {
            if (it != dict.begin())
                valueText += ", ";
            valueText += "\"Key\": ";
            valueText += TValue::Create(it->GetKey(), type.GetKey()).GetValueText<TFormatJSON>(format);
            valueText += ", ";
            valueText += "\"Payload\": ";
            valueText += TValue::Create(it->GetPayload(), type.GetPayload()).GetValueText<TFormatJSON>(format);
        }
        valueText += '}';
        return valueText;
    }
    default:
        // TODO
        return "null";
    }
}

template <> TString TValue::GetValueText<TFormatRowset>(const TFormatRowset& format) const {
    if (HaveValue()) {
        switch(Type.GetKind()) {
        case NKikimrMiniKQL::ETypeKind::Void:
            return "<void>";
        case NKikimrMiniKQL::ETypeKind::Data:
            return GetDataText();
        case NKikimrMiniKQL::ETypeKind::Optional:
            return TValue::Create(Value.GetOptional(), Type.GetOptional().GetItem()).GetValueText<TFormatRowset>(format);
        case NKikimrMiniKQL::ETypeKind::List:
        {
            TString valueText;
            if (Type.GetList().GetItem().GetKind() == NKikimrMiniKQL::ETypeKind::Struct) {
                const auto& list_type = Type.GetList();
                const auto& type = list_type.GetItem().GetStruct().GetMember();
                size_t maxElements = type.size();
                for (size_t element = 0; element < maxElements; ++element) {
                    if (element != 0)
                        valueText += "\t"; // TODO: alignment
                    valueText += type.Get(element).GetName();
                }
                valueText += "\n";
                size_t maxItems = Value.ListSize();
                for (size_t items = 0; items < maxItems; ++items) {
                    const auto& list = Value.GetList(items);
                    const auto& str = list.GetStruct();
                    for (size_t element = 0; element < maxElements; ++element) {
                        if (element != 0)
                            valueText += "\t"; // TODO: alignment
                        valueText += TValue::Create(str.Get(element), type.Get(element).GetType()).GetValueText<TFormatRowset>(format);
                    }
                    valueText += "\n";
                }
            } else {
                const auto& list = Value.GetList();
                const auto& type = Type.GetList().GetItem();
                for (auto it = list.begin(); it != list.end(); ++it) {
                    valueText += TValue::Create(*it, type).GetValueText<TFormatRowset>(format);
                    valueText += "\n";
                }
            }
            return valueText;
        }
        case NKikimrMiniKQL::ETypeKind::Tuple:
        {
            TString valueText = "[";
            const auto& tuple = Value.GetTuple();
            const auto& type = Type.GetTuple().GetElement();
            size_t maxElements = type.size();
            for (size_t element = 0; element < maxElements; ++element) {
                if (element != 0)
                    valueText += ", ";
                valueText += TValue::Create(tuple.Get(element), type.Get(element)).GetValueText<TFormatRowset>(format);
            }
            valueText += ']';
            return valueText;
        }
        case NKikimrMiniKQL::ETypeKind::Struct:
        {
            TString valueText = "";
            const auto& str = Value.GetStruct();
            const auto& type = Type.GetStruct().GetMember();
            size_t maxElements = type.size();
            for (size_t element = 0; element < maxElements; ++element) {
                if (element != 0)
                    valueText += "\t"; // TODO: alignment
                valueText += type.Get(element).GetName();
            }
            valueText += "\n";
            for (size_t element = 0; element < maxElements; ++element) {
                if (element != 0)
                    valueText += "\t"; // TODO: alignment
                valueText += TValue::Create(str.Get(element), type.Get(element).GetType()).GetValueText<TFormatRowset>(format);
            }
            valueText += "\n";
            return valueText;
        }
        case NKikimrMiniKQL::ETypeKind::Dict:
        {
            TString valueText = "";
            const auto& dict = Value.GetDict();
            const auto& type = Type.GetDict();
            valueText += "Key";
            valueText += "\t"; // TODO: alignment
            valueText += "Payload";
            valueText += "\n";
            for (auto it = dict.begin(); it != dict.end(); ++it) {
                valueText += TValue::Create(it->GetKey(), type.GetKey()).GetValueText<TFormatRowset>(format);
                valueText += "\t"; // TODO: alignment
                valueText += TValue::Create(it->GetPayload(), type.GetPayload()).GetValueText<TFormatRowset>(format);
                valueText += "\n";
            }
            return valueText;
        }
        default:
            return "<unknown>";
        }
    }
    return "<null>";
}

template <> TString TValue::GetValueText<TFormatCSV>(const TFormatCSV &format) const {
    if (format.PrintHeader) {
        auto hdr = PrintCsvHeader(Type, format) + "\n";
        TFormatCSV fmt = format;
        fmt.PrintHeader = false;
        return hdr + GetValueText(fmt);
    }

    switch(Type.GetKind()) {
    case NKikimrMiniKQL::ETypeKind::Void:
        return "null";
    case NKikimrMiniKQL::ETypeKind::Data: {
        switch (Type.GetData().GetScheme()) {
        case NScheme::NTypeIds::Utf8:
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
            return "\"" + format.EscapeString(GetDataText()) + "\"";
        default:
            return GetDataText();
        }
    }
    case NKikimrMiniKQL::ETypeKind::Optional: {
        if (Value.HasOptional()) {
            return TValue::Create(Value.GetOptional(), Type.GetOptional().GetItem()).GetValueText(format);
        } else {
            return "null";
        }
    }
    case NKikimrMiniKQL::ETypeKind::List:
    {
        TString valueText = "";
        const auto& list = Value.GetList();
        const auto& type = Type.GetList().GetItem();
        for (auto it = list.begin(); it != list.end(); ++it) {
            if (it != list.begin())
                valueText += "\n";
            valueText += TValue::Create(*it, type).GetValueText(format);
        }
        return valueText;
    }
    case NKikimrMiniKQL::ETypeKind::Tuple:
    {
        TString valueText = "[";
        const auto& tuple = Value.GetTuple();
        const auto& type = Type.GetTuple().GetElement();
        size_t maxElements = type.size();
        for (size_t element = 0; element < maxElements; ++element) {
            if (element != 0)
                valueText += ", ";
            valueText += TValue::Create(tuple.Get(element), type.Get(element)).GetValueText(format);
        }
        valueText += "]";
        return valueText;
    }
    case NKikimrMiniKQL::ETypeKind::Struct:
    {
        TString valueText = "";
        const auto& str = Value.GetStruct();
        const auto& type = Type.GetStruct().GetMember();
        size_t maxElements = type.size();
        for (size_t element = 0; element < maxElements; ++element) {
            if (element != 0)
                valueText += format.Delim;
            valueText += TValue::Create(str.Get(element), type.Get(element).GetType()).GetValueText(format);
        }
        return valueText;
    }
    case NKikimrMiniKQL::ETypeKind::Dict:
    {
        TString valueText = "{";
        const auto& dict = Value.GetDict();
        const auto& type = Type.GetDict();
        for (auto it = dict.begin(); it != dict.end(); ++it) {
            if (it != dict.begin())
                valueText += ", ";
            valueText += "\"Key\": ";
            valueText += TValue::Create(it->GetKey(), type.GetKey()).GetValueText(format);
            valueText += ", ";
            valueText += "\"Payload\": ";
            valueText += TValue::Create(it->GetPayload(), type.GetPayload()).GetValueText(format);
        }
        valueText += '}';
        return valueText;
    }
    default:
        return "null";
    }
}

TValue TValue::EatOptional() const {
    if (Type.HasOptional() && (Value.HasOptional() || !HaveValue()))
        return TValue::Create(Value.HasOptional() ? Value.GetOptional() : Null, Type.GetOptional().GetItem()).EatOptional();
    return *this;
}

TValue::operator ui64() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Uint64);
    Y_ASSERT(Value.HasUint64());
    return Value.GetUint64();
}

TValue::operator i64() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Int64);
    Y_ASSERT(Value.HasInt64());
    return Value.GetInt64();
}

TValue::operator ui32() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Uint32);
    Y_ASSERT(Value.HasUint32());
    return Value.GetUint32();
}

TValue::operator i32() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Int32);
    Y_ASSERT(Value.HasInt32());
    return Value.GetInt32();
}

TValue::operator double() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Double);
    Y_ASSERT(Value.HasDouble());
    return Value.GetDouble();
}

TValue::operator float() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Float);
    Y_ASSERT(Value.HasFloat());
    return Value.GetFloat();
}

TValue::operator bool() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Bool);
    Y_ASSERT(Value.HasBool());
    return Value.GetBool();
}

TValue::operator ui8() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Uint8);
    Y_ASSERT(Value.HasUint32());
    return Value.GetUint32();
}

TValue::operator ui16() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Uint16);
    Y_ASSERT(Value.HasUint32());
    return Value.GetUint32();
}

TValue::operator i8() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Int8);
    Y_ASSERT(Value.HasInt32());
    return Value.GetInt32();
}

TValue::operator i16() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Int16);
    Y_ASSERT(Value.HasInt32());
    return Value.GetInt32();
}

TValue::operator TString() const {
    Y_ASSERT(Type.GetData().GetScheme() == NScheme::NTypeIds::Utf8
            || Type.GetData().GetScheme() == NScheme::NTypeIds::String
            || Type.GetData().GetScheme() == NScheme::NTypeIds::String4k
            || Type.GetData().GetScheme() == NScheme::NTypeIds::String2m
            || Type.GetData().GetScheme() == NScheme::NTypeIds::Yson
            || Type.GetData().GetScheme() == NScheme::NTypeIds::Json
            );
    Y_ASSERT(Value.HasText() || Value.HasBytes());
    return (Type.GetData().GetScheme() == NScheme::NTypeIds::Utf8
           || Type.GetData().GetScheme() == NScheme::NTypeIds::Json)
               ? Value.GetText() : Value.GetBytes();
}


}
}
