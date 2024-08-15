#include "get_value.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

void PrintValue(IOutputStream& out, const NYdb::TValue& v) {
    NYdb::TValueParser value(v);

    while (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
        if (value.IsNull()) {
            out << "<NULL>";
            return;
        } else {
            value.OpenOptional();
        }
    }

    if (value.IsNull()) {
        out << "<NULL>";
        return;
    }

    switch (value.GetPrimitiveType()) {
        case NYdb::EPrimitiveType::Uint32:
        {
            out << value.GetUint32();
            break;
        }
        case NYdb::EPrimitiveType::Uint64:
        {
            out << value.GetUint64();
            break;
        }
        case NYdb::EPrimitiveType::Int64:
        {
            out << value.GetInt64();
            break;
        }
        case NYdb::EPrimitiveType::Uint8:
        {
            out << value.GetInt8();
            break;
        }
        case NYdb::EPrimitiveType::Utf8:
        {
            out << value.GetUtf8();
            break;
        }
        case NYdb::EPrimitiveType::Timestamp:
        {
            out << value.GetTimestamp();
            break;
        }
        case NYdb::EPrimitiveType::Bool:
        {
            out << value.GetBool();
            break;
        }
        default:
        {
            UNIT_ASSERT_C(false, "PrintValue not iplemented for this type");
        }
    }
}

ui64 GetUint32(const NYdb::TValue& v) {
    NYdb::TValueParser value(v);
    if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
        return *value.GetOptionalUint32();
    } else {
        return value.GetUint32();
    }
}

ui64 GetUint64(const NYdb::TValue& v) {
    NYdb::TValueParser value(v);
    if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
        return *value.GetOptionalUint64();
    } else {
        return value.GetUint64();
    }
}

TString GetUtf8(const NYdb::TValue& v) {
    NYdb::TValueParser value(v);
    if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
        return *value.GetOptionalUtf8();
    } else {
        return value.GetUtf8();
    }
}

TInstant GetTimestamp(const NYdb::TValue& v) {
    NYdb::TValueParser value(v);
    if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
        return *value.GetOptionalTimestamp();
    } else {
        return value.GetTimestamp();
    }
}

void PrintRow(IOutputStream& out, const THashMap<TString, NYdb::TValue>& fields) {
    for (const auto& f : fields) {
        out << f.first << ": ";
        PrintValue(out, f.second);
        out << " ";
    }
}

void PrintRows(IOutputStream& out, const TVector<THashMap<TString, NYdb::TValue>>& rows) {
    for (const auto& r : rows) {
        PrintRow(out, r);
        out << "\n";
    }
}

}