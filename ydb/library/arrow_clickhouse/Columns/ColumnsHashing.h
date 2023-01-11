// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "arrow_clickhouse_types.h"

#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Columns/ColumnsHashingImpl.h>

#include <common/unaligned.h>

#include <memory>
#include <cassert>


namespace CH
{

namespace ColumnsHashing
{

/// For the case when there is one numeric key.
/// UInt8/16/32/64 for any type with corresponding bit width.
template <typename Value, typename Mapped, typename FieldType, bool use_cache = true, bool need_offset = false>
struct HashMethodOneNumber
    : public columns_hashing_impl::HashMethodBase<HashMethodOneNumber<Value, Mapped, FieldType, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodOneNumber<Value, Mapped, FieldType, use_cache, need_offset>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;
    using ArrowType = typename arrow::CTypeTraits<FieldType>::ArrowType;
    using ArrowArrayType = arrow::NumericArray<ArrowType>;

    const FieldType * vec{};

    /// If the keys of a fixed length then key_sizes contains their lengths, empty otherwise.
    HashMethodOneNumber(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
    {
        vec = assert_same_size_cast<const ArrowArrayType *>(key_columns[0])->raw_values();
    }

    HashMethodOneNumber(const IColumn * column)
    {
        vec = assert_same_size_cast<const ArrowArrayType *>(column)->raw_values();
    }

    /// Creates context. Method is called once and result context is used in all threads.
    using Base::createContext; /// (const HashMethodContext::Settings &) -> HashMethodContextPtr

    /// Emplace key into HashTable or HashMap. If Data is HashMap, returns ptr to value, otherwise nullptr.
    /// Data is a HashTable where to insert key from column's row.
    /// For Serialized method, key may be placed in pool.
    using Base::emplaceKey; /// (Data & data, size_t row, Arena & pool) -> EmplaceResult

    /// Find key into HashTable or HashMap. If Data is HashMap and key was found, returns ptr to value, otherwise nullptr.
    using Base::findKey;  /// (Data & data, size_t row, Arena & pool) -> FindResult

    /// Get hash value of row.
    using Base::getHash; /// (const Data & data, size_t row, Arena & pool) -> size_t

    /// Is used for default implementation in HashMethodBase.
    FieldType getKeyHolder(size_t row, Arena &) const { return unalignedLoad<FieldType>(vec + row); }

    const FieldType * getKeyData() const { return reinterpret_cast<const FieldType *>(vec); }
};


/// For the case when there is one string key.
template <typename Value, typename Mapped, bool place_string_to_arena = true, bool use_cache = true, bool need_offset = false>
struct HashMethodString
    : public columns_hashing_impl::HashMethodBase<HashMethodString<Value, Mapped, place_string_to_arena, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodString<Value, Mapped, place_string_to_arena, use_cache, need_offset>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

    const int * offsets{};
    const uint8_t * chars{};

    HashMethodString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
    {
        const IColumn & column = *key_columns[0];
        const auto & column_string = assert_cast<const ColumnBinary &>(column);
        offsets = column_string.raw_value_offsets();
        chars = column_string.raw_data();
    }

    auto getKeyHolder(ssize_t row, [[maybe_unused]] Arena & pool) const
    {
        StringRef key(chars + offsets[row - 1], offsets[row] - offsets[row - 1] - 1);

        if constexpr (place_string_to_arena)
        {
            return ArenaKeyHolder{key, pool};
        }
        else
        {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
};


/// For the case when there is one fixed-length string key.
template <typename Value, typename Mapped, bool place_string_to_arena = true, bool use_cache = true, bool need_offset = false>
struct HashMethodFixedString
    : public columns_hashing_impl::
          HashMethodBase<HashMethodFixedString<Value, Mapped, place_string_to_arena, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodFixedString<Value, Mapped, place_string_to_arena, use_cache, need_offset>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

    size_t n{};
    const uint8_t * chars{};

    HashMethodFixedString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
    {
        const IColumn & column = *key_columns[0];
        const ColumnFixedString & column_string = assert_cast<const ColumnFixedString &>(column);
        n = column_string.byte_width();
        chars = column_string.raw_values();
    }

    auto getKeyHolder(size_t row, [[maybe_unused]] Arena & pool) const
    {
        StringRef key(&chars[row * n], n);

        if constexpr (place_string_to_arena)
        {
            return ArenaKeyHolder{key, pool};
        }
        else
        {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
};


/// For the case when all keys are of fixed length, and they fit in N (for example, 128) bits.
template <
    typename Value,
    typename Key,
    typename Mapped,
    bool has_nullable_keys_ = false,
    bool has_low_cardinality_ = false,
    bool use_cache = true,
    bool need_offset = false>
struct HashMethodKeysFixed
    : private columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>
    , public columns_hashing_impl::HashMethodBase<HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, has_low_cardinality_, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, has_low_cardinality_, use_cache, need_offset>;
    using BaseHashed = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;
    using Base = columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>;

    static constexpr bool has_nullable_keys = has_nullable_keys_;

    //Sizes key_sizes;
    size_t keys_size;

#if 0 // shuffleKeyColumns disabled
    PaddedPODArray<Key> prepared_keys;

    static bool usePreparedKeys(const Sizes & key_sizes)
    {
        if (has_nullable_keys || sizeof(Key) > 16)
            return false;

        for (auto size : key_sizes)
            if (size != 1 && size != 2 && size != 4 && size != 8 && size != 16)
                return false;

        return true;
    }
#endif

    HashMethodKeysFixed(const ColumnRawPtrs & key_columns, const Sizes & key_sizes_, const HashMethodContextPtr &)
        : Base(key_columns), /*key_sizes(key_sizes_),*/ keys_size(key_columns.size())
    {
        {   // patch: there's no such logic in original CH
            // It's a protection for coverity generated out-of-bounds access in packFixed() called from getKeyHolder()
            // Check if we have enough memory in Key type to store a key from key_columns
            // GROUP BY key is sum(key_sizes_) long with additional bitmap for nulls

            size_t key_sum_size = has_nullable_keys ? std::tuple_size<KeysNullMap<Key>>::value : 0;
            if constexpr (has_nullable_keys)
                if (!key_sum_size)
                    throw Exception("Empty bitmap for nullable GROUP BY key");

            for (auto size : key_sizes_)
                key_sum_size += size;
            if (key_sum_size > sizeof(Key))
                throw Exception("Wrong fixed size key");
        }
#if 0
        if (usePreparedKeys(key_sizes))
            packFixedBatch(keys_size, Base::getActualColumns(), key_sizes, prepared_keys);
#endif
    }

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &) const
    {
        if constexpr (has_nullable_keys)
        {
            auto bitmap = Base::createBitmap(row);
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), bitmap);
        }
        else
        {
#if 0
            if (!prepared_keys.empty())
                return prepared_keys[row];
#endif
            return packFixed<Key>(row, keys_size, Base::getActualColumns());
        }
    }
#if 0
    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        if (!usePreparedKeys(key_sizes))
            return {};

        std::vector<IColumn *> new_columns;
        new_columns.reserve(key_columns.size());

        Sizes new_sizes;
        auto fill_size = [&](size_t size)
        {
            for (size_t i = 0; i < key_sizes.size(); ++i)
            {
                if (key_sizes[i] == size)
                {
                    new_columns.push_back(key_columns[i]);
                    new_sizes.push_back(size);
                }
            }
        };

        fill_size(16);
        fill_size(8);
        fill_size(4);
        fill_size(2);
        fill_size(1);

        key_columns.swap(new_columns);
        return new_sizes;
    }
#endif
};

/** Hash by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename Value, typename Mapped>
struct HashMethodSerialized
    : public columns_hashing_impl::HashMethodBase<HashMethodSerialized<Value, Mapped>, Value, Mapped, false>
{
    using Self = HashMethodSerialized<Value, Mapped>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ColumnRawPtrs key_columns;
    size_t keys_size;

    HashMethodSerialized(const ColumnRawPtrs & key_columns_, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
        : key_columns(key_columns_), keys_size(key_columns_.size()) {}

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ALWAYS_INLINE SerializedKeyHolder getKeyHolder(size_t row, Arena & pool) const
    {
        return SerializedKeyHolder{
            serializeKeysToPoolContiguous(row, keys_size, key_columns, pool),
            pool};
    }
};

/// For the case when there is one string key.
template <typename Value, typename Mapped, bool use_cache = true, bool need_offset = false>
struct HashMethodHashed
    : public columns_hashing_impl::HashMethodBase<HashMethodHashed<Value, Mapped, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Key = UInt128;
    using Self = HashMethodHashed<Value, Mapped, use_cache, need_offset>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

    ColumnRawPtrs key_columns;

    HashMethodHashed(ColumnRawPtrs key_columns_, const Sizes &, const HashMethodContextPtr &)
        : key_columns(std::move(key_columns_)) {}

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &) const
    {
        return hash128(row, key_columns.size(), key_columns);
    }
};

}
}
