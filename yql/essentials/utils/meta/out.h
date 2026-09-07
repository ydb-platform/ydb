#pragma once

#include "reflection.h"

#include <util/generic/scope.h>
#include <util/stream/output.h>

namespace NYql {

template <typename T>
struct TOut;

template <typename T>
concept COut = requires(IOutputStream& stream, const T& value) {
    { TOut<T>{}(stream, value) };
};

template <typename T>
concept CYOut = requires(IOutputStream& stream, const T& value) {
    { stream << value };
};

template <CYOut T>
    requires(!NReflection::CReflecting<T>)
struct TOut<T> {
    void operator()(IOutputStream& stream, const T& value) {
        stream << value;
    }
};

template <NReflection::CReflecting T>
struct TOut<T> {
    void operator()(IOutputStream& stream, const T& value) {
        static constexpr auto R = NReflection::TReflection<T>::SelfType();

        stream << "{";
        Y_DEFER {
            stream << "}";
        };

        bool isFirst = true;
        R.ForEachFieldValue(value, [&]<size_t Index, auto k>(const auto& value) {
            Y_UNUSED(Index);

            using TValue = std::remove_cvref_t<decltype(value)>;

            if (!isFirst) {
                stream << ", ";
            }

            isFirst = false;
            stream << "." << static_cast<TStringBuf>(k) << " = ";
            TOut<TValue>{}(stream, value);
        });
    }
};

template <COut T>
struct TOut<TMaybe<T>> {
    void operator()(IOutputStream& stream, const TMaybe<T>& value) {
        if (value) {
            TOut<T>{}(stream, *value);
        } else {
            stream << "Nothing()";
        }
    }
};

template <COut T>
struct TOut<TVector<T>> {
    void operator()(IOutputStream& stream, const TVector<T>& value) {
        stream << "{";
        Y_DEFER {
            stream << "}";
        };

        bool isFirst = true;
        for (const auto& value : value) {
            if (!isFirst) {
                stream << ", ";
            }

            isFirst = false;
            TOut<T>{}(stream, value);
        }
    }
};

} // namespace NYql

#define YQL_DERIVE_OUT_SPEC(type)               \
    Y_DECLARE_OUT_SPEC(, type, stream, value) { \
        NYql::TOut<type>{}(stream, value);      \
    }
