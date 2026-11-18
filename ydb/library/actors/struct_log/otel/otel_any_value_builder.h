#pragma once

#include <ydb/library/actors/struct_log/native_types_support.h>

#include <opentelemetry/proto/common/v1/common.pb.h>

#include <chrono>

namespace NActors::NStructuredLog {

using TOtelAnyValue = opentelemetry::proto::common::v1::AnyValue;

template <typename T>
struct TOtelAnyValueBuilder {
    static TOtelAnyValue Build(const T& value) {
        TOtelAnyValue v;
        v.set_string_value( TNativeTypeSupport<T>::ToString(value));
        return v;
    }
};

template <>
struct TOtelAnyValueBuilder<bool> {
    static TOtelAnyValue Build(const bool& value) {
        TOtelAnyValue v;
        v.set_bool_value(value);
        return v;
    }
};

template <>
struct TOtelAnyValueBuilder<i64> {
    static TOtelAnyValue Build(const i64& value) {
        TOtelAnyValue v;
        v.set_int_value(value);
        return v;
    }
};

template <>
struct TOtelAnyValueBuilder<i8> {
    static TOtelAnyValue Build(const i8& value) {
        return TOtelAnyValueBuilder<i64>::Build(static_cast<i64>(value));
    }
};

template <>
struct TOtelAnyValueBuilder<ui8> {
    static TOtelAnyValue Build(const ui8& value) {
        return TOtelAnyValueBuilder<i64>::Build(static_cast<i64>(value));
    }
};

template <>
struct TOtelAnyValueBuilder<i16> {
    static TOtelAnyValue Build(const i16& value) {
        return TOtelAnyValueBuilder<i64>::Build(static_cast<i64>(value));
    }
};

template <>
struct TOtelAnyValueBuilder<ui16> {
    static TOtelAnyValue Build(const ui16& value) {
        return TOtelAnyValueBuilder<i64>::Build(static_cast<i64>(value));
    }
};

template <>
struct TOtelAnyValueBuilder<i32> {
    static TOtelAnyValue Build(const i32& value) {
        return TOtelAnyValueBuilder<i64>::Build(static_cast<i64>(value));
    }
};

template <>
struct TOtelAnyValueBuilder<ui32> {
    static TOtelAnyValue Build(const ui32& value) {
        return TOtelAnyValueBuilder<i64>::Build(static_cast<i64>(value));
    }
};

template <>
struct TOtelAnyValueBuilder<ui64> {
    static TOtelAnyValue Build(const ui64& value) {
        return TOtelAnyValueBuilder<i64>::Build(static_cast<i64>(value));
    }
};

template <>
struct TOtelAnyValueBuilder<double> {
    static TOtelAnyValue Build(const double& value) {
        TOtelAnyValue v;
        v.set_double_value(value);
        return v;
    }
};

template <>
struct TOtelAnyValueBuilder<float> {
    static TOtelAnyValue Build(const float& value) {
        return TOtelAnyValueBuilder<double>::Build(static_cast<double>(value));
    }
};

template <>
struct TOtelAnyValueBuilder<long double> {
    static TOtelAnyValue Build(const long double& value) {
        return TOtelAnyValueBuilder<double>::Build(static_cast<double>(value));
    }
};

template <>
struct TOtelAnyValueBuilder<TString> {
    static TOtelAnyValue Build(const TString& text) {
        TOtelAnyValue v;
        v.set_string_value(text);
        return v;
    }
};

}  // namespace NActors::NStructuredLog
