#pragma once
#include <ydb/core/base/defs.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <compare>

namespace NKikimr::NArrow {

bool IsGoodScalar(const std::shared_ptr<arrow::Scalar>& x);

using TArrayVec = std::vector<std::shared_ptr<arrow::Array>>;

template<typename TArrayVecPtr>
class TReplaceKeyTemplate {
public:
    static constexpr bool IsOwning = std::is_same_v<TArrayVecPtr, std::shared_ptr<TArrayVec>>;

    TReplaceKeyTemplate(TArrayVecPtr columns, int position)
        : Columns(columns)
        , Position(position)
    {
        Y_VERIFY_DEBUG(Size() > 0 && Position < Column(0).length());
    }

    template<typename T = TArrayVecPtr> requires IsOwning
    TReplaceKeyTemplate(TArrayVec&& columns, int position)
        : Columns(std::make_shared<TArrayVec>(std::move(columns)))
        , Position(position)
    {
        Y_VERIFY_DEBUG(Size() > 0 && Position < Column(0).length());
    }

    size_t Hash() const {
        return TypedHash(Column(0), Position, Column(0).type_id());
    }

    template<typename T>
    bool operator == (const TReplaceKeyTemplate<T>& key) const {
        Y_VERIFY_DEBUG(Size() == key.Size());

        for (int i = 0; i < Size(); ++i) {
            auto cmp = CompareColumnValue(i, key, i);
            if (std::is_neq(cmp)) {
                return false;
            }
        }
        return true;
    }

    template<typename T>
    std::partial_ordering operator <=> (const TReplaceKeyTemplate<T>& key) const {
        Y_VERIFY_DEBUG(Size() == key.Size());

        for (int i = 0; i < Size(); ++i) {
            auto cmp = CompareColumnValue(i, key, i);
            if (std::is_neq(cmp)) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template<typename T>
    std::partial_ordering CompareNotNull(const TReplaceKeyTemplate<T>& key) const {
        Y_VERIFY_DEBUG(Size() == key.Size());

        for (int i = 0; i < Size(); ++i) {
            auto cmp = CompareColumnValueNotNull(i, key, i);
            if (std::is_neq(cmp)) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template<typename T>
    bool LessNotNull(const TReplaceKeyTemplate<T>& key) const {
        return CompareNotNull(key) == std::partial_ordering::less;
    }

    template<typename T>
    std::partial_ordering CompareColumnValueNotNull(int column, const TReplaceKeyTemplate<T>& key, int keyColumn) const {
        Y_VERIFY_DEBUG(Column(column).type_id() == key.Column(keyColumn).type_id());

        return TypedCompare<true>(Column(column), Position, key.Column(keyColumn), key.Position);
    }

    template<typename T>
    std::partial_ordering CompareColumnValue(int column, const TReplaceKeyTemplate<T>& key, int keyColumn) const {
        Y_VERIFY_DEBUG(Column(column).type_id() == key.Column(keyColumn).type_id());

        return TypedCompare<false>(Column(column), Position, key.Column(keyColumn), key.Position);
    }

    int Size() const {
        Y_VERIFY_DEBUG(Columns);
        return Columns->size();
    }

    int GetPosition() const {
        return Position;
    }

    const arrow::Array& Column(int i) const {
        Y_VERIFY_DEBUG(Columns);
        return *(*Columns)[i];
    }

    TReplaceKeyTemplate<const TArrayVec*> ToRaw() const {
        if constexpr (IsOwning) {
            return TReplaceKeyTemplate<const TArrayVec*>(Columns.get(), Position);
        } else {
            return *this;
        }
    }

    template<typename T = TArrayVecPtr> requires IsOwning
    static TReplaceKeyTemplate<TArrayVecPtr> FromBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                       const std::shared_ptr<arrow::Schema>& key, int row) {
        Y_VERIFY(key->num_fields() <= batch->num_columns());

        TArrayVec columns;
        columns.reserve(key->num_fields());
        for (int i = 0; i < key->num_fields(); ++i) {
            auto& keyField = key->field(i);
            auto array = batch->GetColumnByName(keyField->name());
            Y_VERIFY(array);
            Y_VERIFY(keyField->type()->Equals(array->type()));
            columns.push_back(array);
        }

        return TReplaceKeyTemplate<TArrayVecPtr>(std::move(columns), row);
    }

    template<typename T = TArrayVecPtr> requires IsOwning
    static TReplaceKeyTemplate<TArrayVecPtr> FromBatch(const std::shared_ptr<arrow::RecordBatch>& batch, int row) {
        auto columns = std::make_shared<TArrayVec>(batch->columns());
        return TReplaceKeyTemplate<TArrayVecPtr>(columns, row);
    }

    static TReplaceKeyTemplate<TArrayVecPtr> FromScalar(const std::shared_ptr<arrow::Scalar>& s) {
        Y_VERIFY_DEBUG(IsGoodScalar(s));
        auto res = MakeArrayFromScalar(*s, 1);
        Y_VERIFY(res.status().ok(), "%s", res.status().ToString().c_str());
        return TReplaceKeyTemplate<TArrayVecPtr>(std::make_shared<TArrayVec>(1, *res), 0);
    }

    static std::shared_ptr<arrow::Scalar> ToScalar(const TReplaceKeyTemplate<TArrayVecPtr>& key) {
        Y_VERIFY_DEBUG(key.Size() == 1);
        auto& column = key.Column(0);
        auto res = column.GetScalar(key.GetPosition());
        Y_VERIFY(res.status().ok(), "%s", res.status().ToString().c_str());
        Y_VERIFY_DEBUG(IsGoodScalar(*res));
        return *res;
    }

private:
    TArrayVecPtr Columns = nullptr;
    int Position = 0;

    static size_t TypedHash(const arrow::Array& ar, int pos, arrow::Type::type typeId) {
        switch (typeId) {
            case arrow::Type::NA:
            case arrow::Type::BOOL:
                break;
            case arrow::Type::UINT8:
                return THash<ui8>()(static_cast<const arrow::UInt8Array&>(ar).Value(pos));
            case arrow::Type::INT8:
                return THash<i8>()(static_cast<const arrow::Int8Array&>(ar).Value(pos));
            case arrow::Type::UINT16:
                return THash<ui16>()(static_cast<const arrow::UInt16Array&>(ar).Value(pos));
            case arrow::Type::INT16:
                return THash<i16>()(static_cast<const arrow::Int16Array&>(ar).Value(pos));
            case arrow::Type::UINT32:
                return THash<ui32>()(static_cast<const arrow::UInt32Array&>(ar).Value(pos));
            case arrow::Type::INT32:
                return THash<i32>()(static_cast<const arrow::Int32Array&>(ar).Value(pos));
            case arrow::Type::UINT64:
                return THash<ui64>()(static_cast<const arrow::UInt64Array&>(ar).Value(pos));
            case arrow::Type::INT64:
                return THash<i64>()(static_cast<const arrow::Int64Array&>(ar).Value(pos));
            case arrow::Type::HALF_FLOAT:
                break;
            case arrow::Type::FLOAT:
                return THash<float>()(static_cast<const arrow::FloatArray&>(ar).Value(pos));
            case arrow::Type::DOUBLE:
                return THash<double>()(static_cast<const arrow::DoubleArray&>(ar).Value(pos));
            case arrow::Type::STRING: {
                const auto& str = static_cast<const arrow::StringArray&>(ar).GetView(pos);
                return THash<std::string_view>()(std::string_view(str.data(), str.size()));
            }
            case arrow::Type::BINARY: {
                const auto& str = static_cast<const arrow::BinaryArray&>(ar).GetView(pos);
                return THash<std::string_view>()(std::string_view(str.data(), str.size()));
            }
            case arrow::Type::FIXED_SIZE_BINARY:
            case arrow::Type::DATE32:
            case arrow::Type::DATE64:
                break;
            case arrow::Type::TIMESTAMP:
                return THash<i64>()(static_cast<const arrow::TimestampArray&>(ar).Value(pos));
            case arrow::Type::TIME32:
                return THash<i32>()(static_cast<const arrow::Time32Array&>(ar).Value(pos));
            case arrow::Type::TIME64:
                return THash<i64>()(static_cast<const arrow::Time64Array&>(ar).Value(pos));
            case arrow::Type::DURATION:
                return THash<i64>()(static_cast<const arrow::DurationArray&>(ar).Value(pos));
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
                Y_FAIL("not implemented");
                break;
        }
        return 0;
    }

    template <bool notNull>
    static std::partial_ordering TypedCompare(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        arrow::Type::type typeId = lhs.type_id();
        switch (typeId) {
            case arrow::Type::NA:
            case arrow::Type::BOOL:
                break;
            case arrow::Type::UINT8:
                return CompareView<arrow::UInt8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT8:
                return CompareView<arrow::Int8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT16:
                return CompareView<arrow::UInt16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT16:
                return CompareView<arrow::Int16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT32:
                return CompareView<arrow::UInt32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT32:
                return CompareView<arrow::Int32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT64:
                return CompareView<arrow::UInt64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT64:
                return CompareView<arrow::Int64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::HALF_FLOAT:
                break;
            case arrow::Type::FLOAT:
                return CompareView<arrow::FloatArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DOUBLE:
                return CompareView<arrow::DoubleArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::STRING:
                return CompareView<arrow::StringArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::BINARY:
                return CompareView<arrow::BinaryArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::FIXED_SIZE_BINARY:
            case arrow::Type::DATE32:
            case arrow::Type::DATE64:
                break;
            case arrow::Type::TIMESTAMP:
                return CompareView<arrow::TimestampArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIME32:
                return CompareView<arrow::Time32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIME64:
                return CompareView<arrow::Time64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DURATION:
                return CompareView<arrow::DurationArray, notNull>(lhs, lpos, rhs, rpos);
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
                Y_FAIL("not implemented");
                break;
        }
        return std::partial_ordering::equivalent;
    }

    template <typename T, bool notNull>
    static std::partial_ordering CompareView(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        auto& left = static_cast<const T&>(lhs);
        auto& right = static_cast<const T&>(rhs);
        if constexpr (notNull) {
            return CompareValueNotNull(left.GetView(lpos), right.GetView(rpos));
        } else {
            return CompareValue(left.GetView(lpos), right.GetView(rpos), left.IsNull(lpos), right.IsNull(rpos));
        }
    }

    template <typename T>
    static std::partial_ordering CompareValue(const T& x, const T& y, bool xIsNull, bool yIsNull) {
        // TODO: std::partial_ordering::unordered for both nulls?
        if (xIsNull) {
            return std::partial_ordering::less;
        }
        if (yIsNull) {
            return std::partial_ordering::greater;
        }
        return CompareValueNotNull(x, y);
    }

    template <typename T>
    static std::partial_ordering CompareValueNotNull(const T& x, const T& y) {
        if constexpr (std::is_same_v<T, arrow::util::string_view>) {
            size_t minSize = (x.size() < y.size()) ? x.size() : y.size();
            int cmp = memcmp(x.data(), y.data(), minSize);
            if (cmp < 0) {
                return std::partial_ordering::less;
            } else if (cmp > 0) {
                return std::partial_ordering::greater;
            }
            return CompareValueNotNull(x.size(), y.size());
        } else {
            return x <=> y;
        }
    }
};

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
