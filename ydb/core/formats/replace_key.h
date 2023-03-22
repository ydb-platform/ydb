#pragma once
#include <ydb/core/base/defs.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

namespace NKikimr::NArrow {

template<typename TArrayVecPtr>
class TReplaceKeyTemplate {
public:
    TReplaceKeyTemplate(TArrayVecPtr columns, int position)
        : Columns(columns)
        , Position(position)
    {
        Y_VERIFY_DEBUG(Size() > 0 && Position < Column(0).length());
    }

    size_t Hash() const {
        return TypedHash(Column(0), Position, Column(0).type_id());
    }

    // TODO: NULLs
    template<typename T>
    bool operator == (const TReplaceKeyTemplate<T>& key) const {
        Y_VERIFY_DEBUG(Size() == key.Size());

        for (int i = 0; i < Size(); ++i) {
            Y_VERIFY_DEBUG(Column(i).type_id() == key.Column(i).type_id());

            if (!TypedEquals(Column(i), Position, key.Column(i), key.Position)) {
                return false;
            }
        }
        return true;
    }

    template<typename T>
    bool operator < (const TReplaceKeyTemplate<T>& key) const {
        Y_VERIFY_DEBUG(Size() == key.Size());

        for (int i = 0; i < Size(); ++i) {
            int cmp = CompareColumnValue(i, key, i);
            if (cmp < 0) {
                return true;
            } else if (cmp > 0) {
                return false;
            }
        }
        return false;
    }

    template<typename T>
    bool LessNotNull(const TReplaceKeyTemplate<T>& key) const {
        Y_VERIFY_DEBUG(Size() == key.Size());

        for (int i = 0; i < Size(); ++i) {
            int cmp = CompareColumnValue(i, key, i, true);
            if (cmp < 0) {
                return true;
            } else if (cmp > 0) {
                return false;
            }
        }
        return false;
    }

    template<typename T>
    int CompareColumnValue(int column, const TReplaceKeyTemplate<T>& key, int keyColumn, bool notNull = false) const {
        Y_VERIFY_DEBUG(Column(column).type_id() == key.Column(keyColumn).type_id());

        if (notNull) {
            return TypedCompare<true>(Column(column), Position, key.Column(keyColumn), key.Position);
        } else {
            return TypedCompare<false>(Column(column), Position, key.Column(keyColumn), key.Position);
        }
    }

    int Size() const {
        return Columns->size();
    }

    int GetPosition() const {
        return Position;
    }

    const arrow::Array& Column(int i) const {
        return *(*Columns)[i];
    }

private:
    TArrayVecPtr Columns;
    int Position;

    static size_t TypedHash(const arrow::Array& ar, int pos, arrow::Type::type typeId) {
        // TODO: more types
        switch (typeId) {
            case arrow::Type::TIMESTAMP:
                return THash<size_t>()((size_t)static_cast<const arrow::TimestampArray&>(ar).Value(pos));
            default:
                break;
        }
        return 0;
    }

    static bool TypedEquals(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        arrow::Type::type typeId = lhs.type_id();
        switch (typeId) {
            case arrow::Type::NA:
            case arrow::Type::BOOL:
                break;
            case arrow::Type::UINT8:
                return EqualValue<arrow::UInt8Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT8:
                return EqualValue<arrow::Int8Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT16:
                return EqualValue<arrow::UInt16Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT16:
                return EqualValue<arrow::Int16Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT32:
                return EqualValue<arrow::UInt32Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT32:
                return EqualValue<arrow::Int32Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT64:
                return EqualValue<arrow::UInt64Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT64:
                return EqualValue<arrow::Int64Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::HALF_FLOAT:
                break;
            case arrow::Type::FLOAT:
                return EqualValue<arrow::FloatArray>(lhs, lpos, rhs, rpos);
            case arrow::Type::DOUBLE:
                return EqualValue<arrow::DoubleArray>(lhs, lpos, rhs, rpos);
            case arrow::Type::STRING:
                return EqualView<arrow::StringArray>(lhs, lpos, rhs, rpos);
            case arrow::Type::BINARY:
                return EqualView<arrow::BinaryArray>(lhs, lpos, rhs, rpos);
            case arrow::Type::FIXED_SIZE_BINARY:
                break;
            case arrow::Type::DATE32:
                return EqualView<arrow::Date32Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::DATE64:
                return EqualView<arrow::Date64Array>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIMESTAMP:
                return EqualValue<arrow::TimestampArray>(lhs, lpos, rhs, rpos);
            case arrow::Type::DURATION:
                return EqualValue<arrow::DurationArray>(lhs, lpos, rhs, rpos);
            case arrow::Type::DECIMAL256:
            case arrow::Type::DECIMAL:
            case arrow::Type::DENSE_UNION:
            case arrow::Type::DICTIONARY:
            case arrow::Type::EXTENSION:
            case arrow::Type::FIXED_SIZE_LIST:
            case arrow::Type::INTERVAL_DAY_TIME:
            case arrow::Type::INTERVAL_MONTHS:
            case arrow::Type::LARGE_BINARY:
            case arrow::Type::LARGE_LIST:
            case arrow::Type::LARGE_STRING:
            case arrow::Type::LIST:
            case arrow::Type::MAP:
            case arrow::Type::MAX_ID:
            case arrow::Type::SPARSE_UNION:
            case arrow::Type::STRUCT:
            case arrow::Type::TIME32:
            case arrow::Type::TIME64:
                break;
        }
        return false;
    }

    template <bool notNull>
    static int TypedCompare(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        arrow::Type::type typeId = lhs.type_id();
        switch (typeId) {
            case arrow::Type::NA:
            case arrow::Type::BOOL:
                break;
            case arrow::Type::UINT8:
                return CompareValue<arrow::UInt8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT8:
                return CompareValue<arrow::Int8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT16:
                return CompareValue<arrow::UInt16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT16:
                return CompareValue<arrow::Int16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT32:
                return CompareValue<arrow::UInt32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT32:
                return CompareValue<arrow::Int32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT64:
                return CompareValue<arrow::UInt64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT64:
                return CompareValue<arrow::Int64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::HALF_FLOAT:
                break;
            case arrow::Type::FLOAT:
                return CompareValue<arrow::FloatArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DOUBLE:
                return CompareValue<arrow::DoubleArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::STRING:
                return CompareView<arrow::StringArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::BINARY:
                return CompareView<arrow::BinaryArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::FIXED_SIZE_BINARY:
            case arrow::Type::DATE32:
            case arrow::Type::DATE64:
                break;
            case arrow::Type::TIMESTAMP:
                return CompareValue<arrow::TimestampArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIME32:
                return CompareValue<arrow::Time32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIME64:
                return CompareValue<arrow::Time64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DURATION:
                return CompareValue<arrow::DurationArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DECIMAL256:
            case arrow::Type::DECIMAL:
            case arrow::Type::DENSE_UNION:
            case arrow::Type::DICTIONARY:
            case arrow::Type::EXTENSION:
            case arrow::Type::FIXED_SIZE_LIST:
            case arrow::Type::INTERVAL_DAY_TIME:
            case arrow::Type::INTERVAL_MONTHS:
            case arrow::Type::LARGE_BINARY:
            case arrow::Type::LARGE_LIST:
            case arrow::Type::LARGE_STRING:
            case arrow::Type::LIST:
            case arrow::Type::MAP:
            case arrow::Type::MAX_ID:
            case arrow::Type::SPARSE_UNION:
            case arrow::Type::STRUCT:
                break;
        }
        return false;
    }

    // TODO: NULLs
    template <typename T>
    static bool EqualValue(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        auto& left = static_cast<const T&>(lhs);
        auto& right = static_cast<const T&>(rhs);
        return left.Value(lpos) == right.Value(rpos);
    }

    // TODO: NULLs
    template <typename T>
    static bool EqualView(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        auto& left = static_cast<const T&>(lhs);
        auto& right = static_cast<const T&>(rhs);
        return left.GetView(lpos) == right.GetView(rpos);
    }

    template <typename T, bool notNull>
    static int CompareValue(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        auto& left = static_cast<const T&>(lhs);
        auto& right = static_cast<const T&>(rhs);
        if constexpr (notNull) {
            return CompareValueNotNull(left.Value(lpos), right.Value(rpos));
        } else {
            return CompareValue(left.Value(lpos), right.Value(rpos), left.IsNull(lpos), right.IsNull(rpos));
        }
    }

    template <typename T, bool notNull>
    static int CompareView(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        auto& left = static_cast<const T&>(lhs);
        auto& right = static_cast<const T&>(rhs);
        if constexpr (notNull) {
            return CompareValueNotNull(left.GetView(lpos), right.GetView(rpos));
        } else {
            return CompareValue(left.GetView(lpos), right.GetView(rpos), left.IsNull(lpos), right.IsNull(rpos));
        }
    }

    template <typename T>
    static int CompareValue(const T& x, const T& y, bool xIsNull, bool yIsNull) {
        if (xIsNull) {
            return -1;
        }
        if (yIsNull) {
            return 1;
        }
        return CompareValueNotNull(x, y);
    }

    template <typename T>
    static int CompareValueNotNull(const T& x, const T& y) {
        if constexpr (std::is_same_v<T, arrow::util::string_view>) {
            size_t minSize = (x.size() < y.size()) ? x.size() : y.size();
            int cmp = memcmp(x.data(), y.data(), minSize);
            if (cmp < 0) {
                return -1; // avoid INT_MIN as negative cmp. We require "-negative is positive" for result.
            } else if (cmp > 0) {
                return 1;
            }
            return CompareValueNotNull(x.size(), y.size());
        } else {
            if (x < y) {
                return -1;
            } else if (x > y) {
                return 1;
            }
            return 0;
        }
    }
};

using TArrayVec = std::vector<std::shared_ptr<arrow::Array>>;
using TReplaceKey = TReplaceKeyTemplate<std::shared_ptr<TArrayVec>>;
using TRawReplaceKey = TReplaceKeyTemplate<const TArrayVec*>;

}

template<>
struct THash<NKikimr::NArrow::TReplaceKey> {
    inline ui64 operator()(const NKikimr::NArrow::TReplaceKey& x) const noexcept {
        return x.Hash();
    }
};

template<>
struct THash<NKikimr::NArrow::TRawReplaceKey> {
    inline ui64 operator()(const NKikimr::NArrow::TRawReplaceKey& x) const noexcept {
        return x.Hash();
    }
};
