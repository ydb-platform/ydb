#pragma once

#include "reflection.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/stream/output.h>

#include <tuple>

namespace NYql {

namespace NDetail {

template <typename TElementOut>
class TRangeOut {
public:
    template <typename TRange>
    void operator()(IOutputStream& stream, const TRange& range) const {
        stream << "{";
        Y_DEFER {
            stream << "}";
        };

        bool isFirst = true;
        for (const auto& value : range) {
            if (!isFirst) {
                stream << ", ";
            }

            isFirst = false;
            TElementOut{}(stream, value);
        }
    }
};

template <typename... TElementOut>
class TTupleOut {
public:
    template <typename TTuple>
    void operator()(IOutputStream& stream, const TTuple& value) const {
        stream << "{";
        Y_DEFER {
            stream << "}";
        };

        bool isFirst = true;
        std::apply(
            [&](const auto&... values) {
                (OutElement<TElementOut>(stream, isFirst, values), ...);
            },
            value);
    }

private:
    template <typename TOut, typename TValue>
    static void OutElement(IOutputStream& stream, bool& isFirst, const TValue& value) {
        if (!isFirst) {
            stream << ", ";
        }

        isFirst = false;
        TOut{}(stream, value);
    }
};

} // namespace NDetail

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
struct TOut<TVector<T>>: NDetail::TRangeOut<TOut<T>> {
};

template <COut T, typename THasher, typename TEqual, typename TAllocator>
struct TOut<THashSet<T, THasher, TEqual, TAllocator>>: NDetail::TRangeOut<TOut<T>> {
};

template <COut TKey, COut TValue, typename THasher, typename TEqual, typename TAllocator>
struct TOut<THashMap<TKey, TValue, THasher, TEqual, TAllocator>>
    : NDetail::TRangeOut<NDetail::TTupleOut<TOut<TKey>, TOut<TValue>>> {
};

} // namespace NYql

#define YQL_DERIVE_OUT_SPEC(type)               \
    Y_DECLARE_OUT_SPEC(, type, stream, value) { \
        NYql::TOut<type>{}(stream, value);      \
    }
