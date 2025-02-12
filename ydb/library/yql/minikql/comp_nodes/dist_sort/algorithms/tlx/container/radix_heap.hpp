/*******************************************************************************
 * tlx/container/radix_heap.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Manuel Penschuck <tlx@manuel.jetzt>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CONTAINER_RADIX_HEAP_HEADER
#define TLX_CONTAINER_RADIX_HEAP_HEADER

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include <tlx/define/likely.hpp>
#include <tlx/math/clz.hpp>
#include <tlx/math/div_ceil.hpp>
#include <tlx/math/ffs.hpp>
#include <tlx/meta/log2.hpp>

namespace tlx {
namespace radix_heap_detail {

/*!
 * Compute the rank of an integer x (i.e. the number of elements smaller than x
 * that are representable using type Int) and vice versa.
 * If Int is an unsigned integral type, all computations yield identity.
 * If Int is a signed integrals, the smallest (negative) number is mapped to
 * rank zero, the next larger value to one and so on.
 *
 * The implementation assumes negative numbers are implemented as Two's
 * complement and contains static_asserts failing if this is not the case.
 */
template <typename Int>
class IntegerRank
{
    static_assert(std::is_integral<Int>::value,
                  "SignedInt has to be an integral type");

public:
    using int_type = Int;
    using rank_type = typename std::make_unsigned<int_type>::type;

    //! Maps value i to its rank in int_type. For any pair T x < y the invariant
    //! IntegerRank<T>::rank_of_int(x) < IntegerRank<T>::rank_of_int(y) holds.
    static constexpr rank_type rank_of_int(int_type i) {
        return use_identity_
               ? static_cast<rank_type>(i)
               : static_cast<rank_type>(i) ^ sign_bit_;
    }

    //! Returns the r-th smallest number of int_r. It is the inverse of
    //! rank_of_int, i.e. int_at_rank(rank_of_int(i)) == i for all i.
    static constexpr int_type int_at_rank(rank_type r) {
        return use_identity_
               ? static_cast<int_type>(r)
               : static_cast<int_type>(r ^ sign_bit_);
    }

private:
    constexpr static bool use_identity_ = !std::is_signed<int_type>::value;

    constexpr static rank_type sign_bit_
        = (rank_type(1) << (8 * sizeof(rank_type) - 1));

    // These test fail if a signed type does not use Two's complement
    static_assert(rank_of_int(std::numeric_limits<int_type>::min()) == 0,
                  "Rank of minimum is not zero");
    static_assert(rank_of_int(std::numeric_limits<int_type>::min() + 1) == 1,
                  "Rank of minimum+1 is not one");
    static_assert(rank_of_int(std::numeric_limits<int_type>::max())
                  == std::numeric_limits<rank_type>::max(),
                  "Rank of maximum is not maximum rank");
    static_assert(rank_of_int(std::numeric_limits<int_type>::max()) >
                  rank_of_int(int_type(0)),
                  "Rank of maximum is not larger than rank of zero");
};

//! Internal implementation of BitArray; do not invoke directly
//! \tparam Size  Number of bits the data structure is supposed to store
//! \tparam SizeIsAtmost64  Switch between inner node implementation (false)
//!                         and leaf implementation (true)
template <size_t Size, bool SizeIsAtmost64>
class BitArrayRecursive;

template <size_t Size>
class BitArrayRecursive<Size, false>
{
    static constexpr size_t leaf_width = 6;
    static constexpr size_t width = tlx::Log2<Size>::ceil;
    static_assert(width > leaf_width,
                  "Size has to be larger than 2**leaf_width");
    static constexpr size_t root_width = (width % leaf_width)
                                         ? (width % leaf_width)
                                         : leaf_width;
    static constexpr size_t child_width = width - root_width;
    using child_type = BitArrayRecursive<1llu << child_width, child_width <= 6>;

    static constexpr size_t root_size = div_ceil(Size, child_type::size);
    using root_type = BitArrayRecursive < root_size <= 32 ? 32 : 64, true >;

    using child_array_type = std::array<child_type, root_size>;

public:
    static constexpr size_t size = Size;

    explicit BitArrayRecursive() noexcept = default;
    BitArrayRecursive(const BitArrayRecursive&) noexcept = default;
    BitArrayRecursive(BitArrayRecursive&&) noexcept = default;
    BitArrayRecursive& operator = (const BitArrayRecursive&) noexcept = default;
    BitArrayRecursive& operator = (BitArrayRecursive&&) noexcept = default;

    void set_bit(const size_t i) {
        const auto idx = get_index_(i);
        root_.set_bit(idx.first);
        children_[idx.first].set_bit(idx.second);
    }

    void clear_bit(const size_t i) {
        const auto idx = get_index_(i);
        children_[idx.first].clear_bit(idx.second);
        if (children_[idx.first].empty())
            root_.clear_bit(idx.first);
    }

    bool is_set(const size_t i) const {
        const auto idx = get_index_(i);
        return children_[idx.first].is_set(idx.second);
    }

    void clear_all() {
        root_.clear_all();
        for (auto& child : children_)
            child.clear_all();
    }

    bool empty() const {
        return root_.empty();
    }

    size_t find_lsb() const {
        assert(!empty());

        const size_t child_idx = root_.find_lsb();
        const size_t child_val = children_[child_idx].find_lsb();

        return child_idx * child_type::size + child_val;
    }

private:
    child_array_type children_;
    root_type root_;

    std::pair<size_t, size_t> get_index_(size_t i) const {
        assert(i < size);
        return { i / child_type::size, i % child_type::size };
    }
};

template <size_t Size>
class BitArrayRecursive<Size, true>
{
    static_assert(Size <= 64, "Support at most 64 bits");
    using uint_type = typename std::conditional<
        Size <= 32, std::uint32_t, std::uint64_t>::type;

public:
    static constexpr size_t size = Size;

    explicit BitArrayRecursive() noexcept : flags_(0) { }
    BitArrayRecursive(const BitArrayRecursive&) noexcept = default;
    BitArrayRecursive(BitArrayRecursive&&) noexcept = default;
    BitArrayRecursive& operator = (const BitArrayRecursive&) noexcept = default;
    BitArrayRecursive& operator = (BitArrayRecursive&&) noexcept = default;

    void set_bit(const size_t i) {
        assert(i < size);
        flags_ |= uint_type(1) << i;
    }

    void clear_bit(const size_t i) {
        assert(i < size);
        flags_ &= ~(uint_type(1) << i);
    }

    bool is_set(const size_t i) const {
        assert(i < size);
        return (flags_ & (uint_type(1) << i)) != 0;
    }

    void clear_all() {
        flags_ = 0;
    }

    bool empty() const {
        return !flags_;
    }

    size_t find_lsb() const {
        assert(!empty());
        return tlx::ffs(flags_) - 1;
    }

private:
    uint_type flags_;
};

/*!
 * A BitArray of fixed size supporting reading, setting, and clearing
 * of individual bits. The data structure is optimized to find the bit with
 * smallest index that is set (find_lsb).
 *
 * The BitArray is implemented as a search tree with a fan-out of up to 64.
 * It is thus very flat, and all operations but with the exception of clear_all
 * have a complexity of O(log_64(Size)) which is << 10 for all practical
 * purposes.
 */
template <size_t Size>
class BitArray
{
    using impl_type = BitArrayRecursive<Size, Size <= 64>;

public:
    static constexpr size_t size = Size;

    explicit BitArray() noexcept = default;
    BitArray(const BitArray&) noexcept = default;
    BitArray(BitArray&&) noexcept = default;
    BitArray& operator = (const BitArray&) noexcept = default;
    BitArray& operator = (BitArray&&) noexcept = default;

    //! Set the i-th bit to true
    void set_bit(const size_t i) {
        impl_.set_bit(i);
    }

    //! Set the i-th bit to false
    void clear_bit(const size_t i) {
        impl_.clear_bit(i);
    }

    //! Returns value of the i-th
    bool is_set(const size_t i) const {
        return impl_.is_set(i);
    }

    //! Sets all bits to false
    void clear_all() {
        impl_.clear_all();
    }

    //! True if all bits are false
    bool empty() const {
        return impl_.empty();
    }

    //! Finds the bit with smallest index that is set
    //! \warning If empty() is true, the result is undefined
    size_t find_lsb() const {
        return impl_.find_lsb();
    }

private:
    impl_type impl_;
};

template <unsigned Radix, typename Int>
class BucketComputation
{
    static_assert(std::is_unsigned<Int>::value, "Require unsigned integer");
    static constexpr unsigned radix_bits = tlx::Log2<Radix>::floor;

public:
    //! Return bucket index key x belongs to given the current insertion limit
    size_t operator () (const Int x, const Int insertion_limit) const {
        constexpr Int mask = (1u << radix_bits) - 1;

        assert(x >= insertion_limit);

        const auto diff = x ^ insertion_limit;
        if (!diff) return 0;

        const auto diff_in_bit = (8 * sizeof(Int) - 1) - clz(diff);

        const auto row = diff_in_bit / radix_bits;
        const auto bucket_in_row = ((x >> (radix_bits * row)) & mask) - row;

        const auto result = row * Radix + bucket_in_row;

        return result;
    }

    //! Return smallest key possible in bucket idx assuming insertion_limit==0
    Int lower_bound(const size_t idx) const {
        assert(idx < num_buckets);

        if (idx < Radix)
            return static_cast<Int>(idx);

        const size_t row = (idx - 1) / (Radix - 1);
        const auto digit = static_cast<Int>(idx - row * (Radix - 1));

        return digit << radix_bits * row;
    }

    //! Return largest key possible in bucket idx assuming insertion_limit==0
    Int upper_bound(const size_t idx) const {
        assert(idx < num_buckets);

        if (idx == num_buckets - 1)
            return std::numeric_limits<Int>::max();

        return lower_bound(idx + 1) - 1;
    }

private:
    constexpr static size_t num_buckets_(size_t bits) {
        return (bits >= radix_bits)
               ? (Radix - 1) + num_buckets_(bits - radix_bits)
               : (1 << bits) - 1;
    }

public:
    //! Number of buckets required given Radix and the current data type Int
    static constexpr size_t num_buckets =
        num_buckets_(std::numeric_limits<Int>::digits) + 1;
};

//! Used as an adapter to implement RadixHeapPair on top of RadixHeap.
template <typename KeyType, typename DataType>
struct PairKeyExtract {
    using allow_emplace_pair = bool;

    KeyType operator () (const std::pair<KeyType, DataType>& p) const {
        return p.first;
    }
};

} // namespace radix_heap_detail

//! \addtogroup tlx_container
//! \{

/*!
 * This class implements a monotonic integer min priority queue, more specific
 * a multi-level radix heap.
 *
 * Here, monotonic refers to the fact that the heap maintains an insertion limit
 * and does not allow the insertion of keys smaller than this limit. The
 * frontier is increased to the current minimum when invoking the methods top(),
 * pop() and swap_top_bucket(). To query the currently smallest item without
 * updating the insertion limit use peak_top_key().
 *
 * We implement a two level radix heap. Let k=sizeof(KeyType)*8 be the number of
 * bits in a key. In contrast to an ordinary radix heap which contains k
 * buckets, we maintain ceil(k/log2(Radix)) rows each containing Radix-many
 * buckets.  This reduces the number of move operations when reorganizing the
 * data structure.
 *
 * The implementation loosly follows the description of "An Experimental Study
 * of Priority Queues in External Memory" [Bregel et al.] and is also inspired
 * by https://github.com/iwiwi/radix-heap
 *
 * \tparam KeyType   Has to be an unsigned integer type
 * \tparam DataType  Type of data payload
 * \tparam Radix     A power of two <= 64.
 */
template <typename ValueType, typename KeyExtract,
          typename KeyType, unsigned Radix = 8>
class RadixHeap
{
    static_assert(Log2<Radix>::floor == Log2<Radix>::ceil,
                  "Radix has to be power of two");

    static constexpr bool debug = false;

public:
    using key_type = KeyType;
    using value_type = ValueType;
    using bucket_index_type = size_t;

    static constexpr unsigned radix = Radix;

protected:
    using Encoder = radix_heap_detail::IntegerRank<key_type>;
    using ranked_key_type = typename Encoder::rank_type;
    using bucket_map_type =
        radix_heap_detail::BucketComputation<Radix, ranked_key_type>;

    static constexpr unsigned radix_bits = tlx::Log2<radix>::floor;
    static constexpr unsigned num_layers =
        div_ceil(8 * sizeof(ranked_key_type), radix_bits);
    static constexpr unsigned num_buckets = bucket_map_type::num_buckets;

public:
    using bucket_data_type = std::vector<value_type>;

    explicit RadixHeap(KeyExtract key_extract = KeyExtract { })
        : key_extract_(key_extract) {
        initialize_();
    }

    // Copy
    RadixHeap(const RadixHeap&) = default;
    RadixHeap& operator = (const RadixHeap&) = default;

    // Move
    RadixHeap(RadixHeap&&) = default;
    RadixHeap& operator = (RadixHeap&&) = default;

    bucket_index_type get_bucket(const value_type& value) const {
        return get_bucket_key(key_extract_(value));
    }

    bucket_index_type get_bucket_key(const key_type key) const {
        const auto enc = Encoder::rank_of_int(key);
        assert(enc >= insertion_limit_);

        return bucket_map_(enc, insertion_limit_);
    }

    //! Construct and insert element with priority key
    //! \warning In contrast to all other methods the key has to be provided
    //! explicitly as the first argument. All other arguments are passed to
    //! the constructor of the element.
    template <typename... Args>
    bucket_index_type emplace(const key_type key, Args&& ... args) {
        const auto enc = Encoder::rank_of_int(key);
        assert(enc >= insertion_limit_);
        const auto idx = bucket_map_(enc, insertion_limit_);

        emplace_in_bucket(idx, std::forward<Args>(args) ...);
        return idx;
    }

    //! In case the first parameter can be directly casted into key_type,
    //! using this method avoid repeating it.
    template <typename... Args>
    bucket_index_type emplace_keyfirst(const key_type key, Args&& ... args) {
        return emplace(key, key, std::forward<Args>(args) ...);
    }

    //! Construct and insert element into bucket idx (useful if an item
    //! was inserted into the same bucket directly before)
    //! \warning Calling any method which updates the current
    //! can invalidate this hint
    template <typename... Args>
    void emplace_in_bucket(const bucket_index_type idx, Args&& ... args) {
        if (buckets_data_[idx].empty()) filled_.set_bit(idx);
        buckets_data_[idx].emplace_back(std::forward<Args>(args) ...);

        const auto enc = Encoder::rank_of_int(
            key_extract_(buckets_data_[idx].back()));
        if (mins_[idx] > enc) mins_[idx] = enc;
        assert(idx == bucket_map_(enc, insertion_limit_));

        size_++;
    }

    //! Insert element with priority key
    bucket_index_type push(const value_type& value) {
        const auto enc = Encoder::rank_of_int(key_extract_(value));
        assert(enc >= insertion_limit_);

        const auto idx = bucket_map_(enc, insertion_limit_);

        push_to_bucket(idx, value);

        return idx;
    }

    //! Insert element into specific bucket (useful if an item
    //! was inserted into the same bucket directly before)
    //! \warning Calling any method which updates the current
    //! can invalidate this hint
    void push_to_bucket(const bucket_index_type idx, const value_type& value) {
        const auto enc = Encoder::rank_of_int(key_extract_(value));

        assert(enc >= insertion_limit_);
        assert(idx == get_bucket(value));

        if (buckets_data_[idx].empty()) filled_.set_bit(idx);
        buckets_data_[idx].push_back(value);

        if (mins_[idx] > enc) mins_[idx] = enc;

        size_++;
    }

    //! Indicates whether size() == 0
    bool empty() const {
        return size() == 0;
    }

    //! Returns number of elements currently stored
    size_t size() const {
        return size_;
    }

    //! Returns currently smallest key without updating the insertion limit
    key_type peak_top_key() const {
        assert(!empty());
        const auto first = filled_.find_lsb();
        return Encoder::int_at_rank(mins_[first]);
    }

    //! Returns currently smallest key and data
    //! \warning Updates insertion limit; no smaller keys can be inserted later
    const value_type& top() {
        reorganize_();
        return buckets_data_[current_bucket_].back();
    }

    //! Removes smallest element
    //! \warning Updates insertion limit; no smaller keys can be inserted later
    void pop() {
        reorganize_();
        buckets_data_[current_bucket_].pop_back();
        if (buckets_data_[current_bucket_].empty())
            filled_.clear_bit(current_bucket_);
        --size_;
    }

    //! Exchanges the top buckets with an *empty* user provided bucket.
    //! Can be used for bulk removals and may reduce allocation overhead
    //! \warning The exchange bucket has to be empty
    //! \warning Updates insertion limit; no smaller keys can be inserted later
    void swap_top_bucket(bucket_data_type& exchange_bucket) {
        reorganize_();

        assert(exchange_bucket.empty());
        size_ -= buckets_data_[current_bucket_].size();
        buckets_data_[current_bucket_].swap(exchange_bucket);

        filled_.clear_bit(current_bucket_);
    }

    //! Clears all internal queues and resets insertion limit
    void clear() {
        for (auto& x : buckets_data_) x.clear();
        initialize_();
    }

protected:
    KeyExtract key_extract_;
    size_t size_ { 0 };
    ranked_key_type insertion_limit_{ 0 };
    size_t current_bucket_{ 0 };

    bucket_map_type bucket_map_;

    std::array<bucket_data_type, num_buckets> buckets_data_;

    std::array<ranked_key_type, num_buckets> mins_;
    radix_heap_detail::BitArray<num_buckets> filled_;

    void initialize_() {
        size_ = 0;
        insertion_limit_ = std::numeric_limits<ranked_key_type>::min();
        current_bucket_ = 0;

        std::fill(mins_.begin(), mins_.end(),
                  std::numeric_limits<ranked_key_type>::max());

        filled_.clear_all();
    }

    void reorganize_() {
        assert(!empty());

        // nothing do to if we already know a suited bucket
        if (TLX_LIKELY(!buckets_data_[current_bucket_].empty())) {
            assert(current_bucket_ < Radix);
            return;
        }

        // mark current bucket as empty
        mins_[current_bucket_] = std::numeric_limits<ranked_key_type>::max();
        filled_.clear_bit(current_bucket_);

        // find a non-empty bucket
        const auto first_non_empty = filled_.find_lsb();
#ifndef NDEBUG
        {
            assert(first_non_empty < num_buckets);

            for (size_t i = 0; i < first_non_empty; i++) {
                assert(buckets_data_[i].empty());
                assert(mins_[i] == std::numeric_limits<ranked_key_type>::max());
            }

            assert(!buckets_data_[first_non_empty].empty());
        }
#endif

        if (TLX_LIKELY(first_non_empty < Radix)) {
            // the first_non_empty non-empty bucket belongs to the smallest row
            // it hence contains only one key and we do not need to reorganise
            current_bucket_ = first_non_empty;
            return;
        }

        // update insertion limit
        {
            const auto new_ins_limit = mins_[first_non_empty];
            assert(new_ins_limit > insertion_limit_);
            insertion_limit_ = new_ins_limit;
        }

        auto& data_source = buckets_data_[first_non_empty];

        for (auto& x : data_source) {
            const ranked_key_type key = Encoder::rank_of_int(key_extract_(x));
            assert(key >= mins_[first_non_empty]);
            assert(first_non_empty == mins_.size() - 1
                   || key < mins_[first_non_empty + 1]);
            const auto idx = bucket_map_(key, insertion_limit_);
            assert(idx < first_non_empty);

            // insert into bucket
            if (buckets_data_[idx].empty()) filled_.set_bit(idx);
            buckets_data_[idx].push_back(std::move(x));
            if (mins_[idx] > key) mins_[idx] = key;
        }

        data_source.clear();

        // mark consumed bucket as empty
        mins_[first_non_empty] = std::numeric_limits<ranked_key_type>::max();
        filled_.clear_bit(first_non_empty);

        // update global pointers and minima
        current_bucket_ = filled_.find_lsb();
        assert(current_bucket_ < Radix);
        assert(!buckets_data_[current_bucket_].empty());
        assert(mins_[current_bucket_] >= insertion_limit_);
    }
};

/*!
 * Helper to easily derive type of RadixHeap for a pre-C++17 compiler.
 * Refer to RadixHeap for description of parameters.
 */
template <typename DataType, unsigned Radix = 8, typename KeyExtract = void>
auto make_radix_heap(KeyExtract&& key_extract)->
RadixHeap<DataType, KeyExtract,
          decltype(key_extract(std::declval<DataType>())), Radix> {
    return (RadixHeap<DataType,
                      KeyExtract,
                      decltype(key_extract(DataType{ })), Radix> {
                key_extract
            });
}

/*!
 * This class is a variant of tlx::RadixHeap for data types which do not
 * include the key directly. Hence each entry is stored as an (Key,Value)-Pair
 * implemented with std::pair.
 */
template <typename KeyType, typename DataType, unsigned Radix = 8>
using RadixHeapPair = RadixHeap<
    std::pair<KeyType, DataType>,
    radix_heap_detail::PairKeyExtract<KeyType, DataType>,
    KeyType, Radix
    >;

//! \}

} // namespace tlx

#endif // !TLX_CONTAINER_RADIX_HEAP_HEADER

/******************************************************************************/
