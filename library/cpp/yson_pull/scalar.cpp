#include "scalar.h"

#include <library/cpp/yson_pull/detail/cescape.h>

#include <util/stream/output.h>

using namespace NYsonPull;

template <>
void Out<TScalar>(IOutputStream& out, const TScalar& value) {
    out << '(' << value.Type();
    if (value.Type() != EScalarType::Entity) {
        out << ' ';
    }
    switch (value.Type()) {
        case EScalarType::Boolean:
            out << (value.AsBoolean() ? "true" : "false");
            break;
        case EScalarType::String:
            out << NYsonPull::NDetail::NCEscape::quote(value.AsString());
            break;
        case EScalarType::Int64:
            out << value.AsInt64();
            break;
        case EScalarType::UInt64:
            out << value.AsUInt64();
            break;
        case EScalarType::Float64:
            out << value.AsFloat64();
            break;
        default:
            break;
    }
    out << ')';
}

bool NYsonPull::operator==(const TScalar& left, const TScalar& right) noexcept {
    if (left.Type() != right.Type()) {
        return false;
    }
    switch (left.Type()) {
        case EScalarType::Boolean:
            return left.AsBoolean() == right.AsBoolean();
        case EScalarType::String:
            return left.AsString() == right.AsString();
        case EScalarType::Int64:
            return left.AsInt64() == right.AsInt64();
        case EScalarType::UInt64:
            return left.AsUInt64() == right.AsUInt64();
        case EScalarType::Float64:
            return left.AsFloat64() == right.AsFloat64();
        case EScalarType::Entity:
            return true;
        default:
            Y_UNREACHABLE();
    }
}
