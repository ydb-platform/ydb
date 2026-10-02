/**
 * A C++ header for ART-based 64-bit Roaring Bitmaps.
 *
 * Reference (format specification) :
 * https://github.com/RoaringBitmap/RoaringFormatSpec#extention-for-64-bit-implementations
 */
#ifndef INCLUDE_ROARING_ROARING64_HH_
#define INCLUDE_ROARING_ROARING64_HH_

#include <algorithm>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <utility>

#include <roaring/roaring64.h>

#include "roaring.hh"

namespace roaring {

/**
 * Const bidirectional iterator over a Roaring64.
 */
class Roaring64ConstIterator {
   public:
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef uint64_t value_type;
    typedef std::ptrdiff_t difference_type;
    typedef void pointer;
    typedef uint64_t reference;

    /**
     * Create an iterator over a bitmap.
     * Begin if `first` is true, otherwise end.
     */

    explicit Roaring64ConstIterator(const api::roaring64_bitmap_t* r,
                                    bool first = true)
        : parent(r), it(first ? api::roaring64_iterator_create(r) : nullptr) {}

    Roaring64ConstIterator() : parent(nullptr), it(nullptr) {}

    Roaring64ConstIterator(const Roaring64ConstIterator& o)
        : parent(o.parent),
          it(o.it == nullptr ? nullptr : api::roaring64_iterator_copy(o.it)) {}

    Roaring64ConstIterator& operator=(const Roaring64ConstIterator& o) {
        if (this != &o) {
            if (it != nullptr) {
                api::roaring64_iterator_free(it);
            }
            parent = o.parent;
            it = o.it == nullptr ? nullptr : api::roaring64_iterator_copy(o.it);
        }
        return *this;
    }

    Roaring64ConstIterator(Roaring64ConstIterator&& o) noexcept
        : parent(o.parent), it(o.it) {
        o.it = nullptr;
    }

    Roaring64ConstIterator& operator=(Roaring64ConstIterator&& o) noexcept {
        if (this != &o) {
            if (it != nullptr) {
                api::roaring64_iterator_free(it);
            }
            parent = o.parent;
            it = o.it;
            o.it = nullptr;
        }
        return *this;
    }

    ~Roaring64ConstIterator() {
        if (it != nullptr) {
            api::roaring64_iterator_free(it);
        }
    }

    /** Returns the current value. Precondition: the iterator is not at end. */
    uint64_t operator*() const { return api::roaring64_iterator_value(it); }

    Roaring64ConstIterator& operator++() {
        if (it != nullptr && api::roaring64_iterator_has_value(it)) {
            api::roaring64_iterator_advance(it);
        }
        return *this;
    }

    /** Postfix increment. Returns the pre-increment position. */
    Roaring64ConstIterator operator++(int) {
        Roaring64ConstIterator orig(*this);
        ++(*this);
        return orig;
    }

    Roaring64ConstIterator& operator--() {
        if (it == nullptr) {
            // End holds no handle yet; create one at the parent's last value
            it = api::roaring64_iterator_create_last(parent);
        } else {
            api::roaring64_iterator_previous(it);
        }
        return *this;
    }

    /** Postfix decrement. Returns the pre-decrement position. */
    Roaring64ConstIterator operator--(int) {
        Roaring64ConstIterator orig(*this);
        --(*this);
        return orig;
    }

    bool operator==(const Roaring64ConstIterator& o) const {
        bool a = atEnd();
        bool b = o.atEnd();
        if (a || b) {
            return a == b;
        }
        return api::roaring64_iterator_value(it) ==
               api::roaring64_iterator_value(o.it);
    }

    bool operator!=(const Roaring64ConstIterator& o) const {
        return !(*this == o);
    }

   private:
    bool atEnd() const {
        return it == nullptr || !api::roaring64_iterator_has_value(it);
    }

    const api::roaring64_bitmap_t* parent;
    api::roaring64_iterator_t* it;
};

class Roaring64 {
    typedef api::roaring64_bitmap_t roaring64_bitmap_t;

   public:
    /**
     * Create an empty bitmap.
     */
    Roaring64() : roaring(api::roaring64_bitmap_create()) {
        if (roaring == nullptr) {
            ROARING_TERMINATE("failed memory alloc in roaring64_bitmap_create");
        }
    }

    /**
     * Construct a bitmap from a list of 64-bit integer values.
     */
    Roaring64(size_t n, const uint64_t* data) : Roaring64() {
        addMany(n, data);
    }

    /**
     * Construct a bitmap from an initializer list.
     */
    Roaring64(std::initializer_list<uint64_t> l) : Roaring64() {
        addMany(l.size(), l.begin());
    }

    /**
     * Construct a roaring object by taking control of a malloc()'d C struct.
     *
     * Passing a NULL pointer is unsafe.
     * The pointer to the C struct will be invalid after the call.
     */
    explicit Roaring64(roaring64_bitmap_t* s) noexcept : roaring(s) {}

    /**
     * Copy constructor.
     */
    Roaring64(const Roaring64& r)
        : roaring(api::roaring64_bitmap_copy(r.roaring)) {
        if (roaring == nullptr) {
            ROARING_TERMINATE("failed roaring64_bitmap_copy in constructor");
        }
    }

    /**
     * Move constructor. The moved-from bitmap is left in a null state and may
     * only be destroyed or assigned to.
     */
    Roaring64(Roaring64&& r) noexcept : roaring(r.roaring) {
        r.roaring = nullptr;
    }

    /**
     * Copy assignment operator.
     */
    Roaring64& operator=(const Roaring64& r) {
        if (this != &r) {
            roaring64_bitmap_t* copy = api::roaring64_bitmap_copy(r.roaring);
            if (copy == nullptr) {
                ROARING_TERMINATE("failed roaring64_bitmap_copy in assignment");
            }
            if (roaring != nullptr) {
                api::roaring64_bitmap_free(roaring);
            }
            roaring = copy;
        }
        return *this;
    }

    /**
     * Move assignment operator.
     */
    Roaring64& operator=(Roaring64&& r) noexcept {
        if (this != &r) {
            if (roaring != nullptr) {
                api::roaring64_bitmap_free(roaring);
            }
            roaring = r.roaring;
            r.roaring = nullptr;
        }
        return *this;
    }

    ~Roaring64() {
        if (roaring != nullptr) {
            api::roaring64_bitmap_free(roaring);
        }
    }

    /**
     * Construct a bitmap from a list of uint64_t values.
     */
    static Roaring64 bitmapOf(size_t n, ...) {
        Roaring64 ans;
        va_list vl;
        va_start(vl, n);
        for (size_t i = 0; i < n; i++) {
            ans.add(va_arg(vl, uint64_t));
        }
        va_end(vl);
        return ans;
    }

    /**
     * Construct a bitmap from a list of uint64_t values.
     * E.g., bitmapOfList({1,2,3}).
     */
    static Roaring64 bitmapOfList(std::initializer_list<uint64_t> l) {
        Roaring64 ans;
        ans.addMany(l.size(), l.begin());
        return ans;
    }

    /**
     * Adds value x.
     */
    void add(uint64_t x) noexcept { api::roaring64_bitmap_add(roaring, x); }

    /**
     * Adds 'n_args' values from the contiguous memory range starting at 'vals'.
     */
    void addMany(size_t n_args, const uint64_t* vals) noexcept {
        api::roaring64_bitmap_add_many(roaring, n_args, vals);
    }

    /**
     * Removes value x.
     */
    void remove(uint64_t x) noexcept {
        api::roaring64_bitmap_remove(roaring, x);
    }

    /**
     * Clears the bitmap.
     */
    void clear() noexcept { api::roaring64_bitmap_clear(roaring); }

    /**
     * Returns the largest value in the set, or 0 if empty.
     */
    uint64_t maximum() const noexcept {
        return api::roaring64_bitmap_maximum(roaring);
    }

    /**
     * Returns the smallest value in the set, or UINT64_MAX if the set is empty.
     */
    uint64_t minimum() const noexcept {
        return api::roaring64_bitmap_minimum(roaring);
    }

    /**
     * Check if value x is present
     */
    bool contains(uint64_t x) const noexcept {
        return api::roaring64_bitmap_contains(roaring, x);
    }

    /**
     * Compute the intersection of the current bitmap and the provided bitmap,
     * writing the result in the current bitmap. The provided bitmap is not
     * modified.
     */
    Roaring64& operator&=(const Roaring64& r) noexcept {
        api::roaring64_bitmap_and_inplace(roaring, r.roaring);
        return *this;
    }

    /**
     * Compute the difference between the current bitmap and the provided
     * bitmap, writing the result in the current bitmap. The provided bitmap is
     * not modified. Behavior is undefined if &r == this.
     */
    Roaring64& operator-=(const Roaring64& r) noexcept {
        api::roaring64_bitmap_andnot_inplace(roaring, r.roaring);
        return *this;
    }

    /**
     * Compute the union of the current bitmap and the provided bitmap, writing
     * the result in the current bitmap. The provided bitmap is not modified.
     */
    Roaring64& operator|=(const Roaring64& r) noexcept {
        api::roaring64_bitmap_or_inplace(roaring, r.roaring);
        return *this;
    }

    /**
     * Compute the XOR of the current bitmap and the provided bitmap, writing
     * the result in the current bitmap. The provided bitmap is not modified.
     * Behavior is undefined if &r == this.
     */
    Roaring64& operator^=(const Roaring64& r) noexcept {
        api::roaring64_bitmap_xor_inplace(roaring, r.roaring);
        return *this;
    }

    /**
     * Exchange the content of this bitmap with another.
     */
    void swap(Roaring64& r) noexcept { std::swap(roaring, r.roaring); }

    /**
     * Get the cardinality of the bitmap (number of elements).
     */
    uint64_t cardinality() const noexcept {
        return api::roaring64_bitmap_get_cardinality(roaring);
    }

    /**
     * Returns true if the bitmap is empty (cardinality is zero).
     */
    bool isEmpty() const noexcept {
        return api::roaring64_bitmap_is_empty(roaring);
    }

    /**
     * Return true if the two bitmaps contain the same elements.
     */
    bool operator==(const Roaring64& r) const noexcept {
        return api::roaring64_bitmap_equals(roaring, r.roaring);
    }

    /**
     * Computes the intersection between two bitmaps and returns new bitmap.
     * The current bitmap and the provided bitmap are unchanged.
     */
    Roaring64 operator&(const Roaring64& o) const {
        roaring64_bitmap_t* result =
            api::roaring64_bitmap_and(roaring, o.roaring);
        if (result == nullptr) {
            ROARING_TERMINATE("failed materialization in and");
        }
        return Roaring64(result);
    }

    /**
     * Computes the difference between two bitmaps and returns new bitmap.
     * The current bitmap and the provided bitmap are unchanged.
     */
    Roaring64 operator-(const Roaring64& o) const {
        roaring64_bitmap_t* result =
            api::roaring64_bitmap_andnot(roaring, o.roaring);
        if (result == nullptr) {
            ROARING_TERMINATE("failed materialization in andnot");
        }
        return Roaring64(result);
    }

    /**
     * Computes the union between two bitmaps and returns new bitmap.
     * The current bitmap and the provided bitmap are unchanged.
     */
    Roaring64 operator|(const Roaring64& o) const {
        roaring64_bitmap_t* result =
            api::roaring64_bitmap_or(roaring, o.roaring);
        if (result == nullptr) {
            ROARING_TERMINATE("failed materialization in or");
        }
        return Roaring64(result);
    }

    /**
     * Computes the symmetric union between two bitmaps and returns new bitmap.
     * The current bitmap and the provided bitmap are unchanged.
     */
    Roaring64 operator^(const Roaring64& o) const {
        roaring64_bitmap_t* result =
            api::roaring64_bitmap_xor(roaring, o.roaring);
        if (result == nullptr) {
            ROARING_TERMINATE("failed materialization in xor");
        }
        return Roaring64(result);
    }

    /**
     * Remove run-length encoding even when it is more space efficient.
     * Return whether a change was applied.
     */
    bool removeRunCompression() noexcept {
        return api::roaring64_bitmap_remove_run_compression(roaring);
    }

    /**
     * Convert array and bitmap containers to run containers when it is more
     * efficient; also convert from run containers when more space efficient.
     * Returns true if the result has at least one run container.
     * Additional savings might be possible by calling shrinkToFit().
     */
    bool runOptimize() noexcept {
        return api::roaring64_bitmap_run_optimize(roaring);
    }

    /**
     * If needed, reallocate memory to shrink the memory usage.
     * Returns the number of bytes saved.
     */
    size_t shrinkToFit() noexcept {
        return api::roaring64_bitmap_shrink_to_fit(roaring);
    }

    /**
     * How many bytes are required to serialize this bitmap.
     */
    size_t getSizeInBytes() const noexcept {
        return api::roaring64_bitmap_portable_size_in_bytes(roaring);
    }

    /**
     * Write a bitmap to a char buffer, which should refer to at least
     * getSizeInBytes() allocated bytes. Returns how many bytes were written.
     * Only the portable format is supported.
     */
    size_t write(char* buf) const noexcept {
        return api::roaring64_bitmap_portable_serialize(roaring, buf);
    }

    /**
     * Read a bitmap from a serialized version, placing no limit on how many
     * bytes are read. See also readSafe. May throw std::runtime_error.
     */
    static Roaring64 read(const char* buf) { return readSafe(buf, SIZE_MAX); }

    /**
     * Read a bitmap from a serialized version, reading no more than maxbytes
     * bytes. The result is usable only if the input follows the format
     * specification. May throw std::runtime_error.
     */
    static Roaring64 readSafe(const char* buf, size_t maxbytes) {
        roaring64_bitmap_t* result =
            api::roaring64_bitmap_portable_deserialize_safe(buf, maxbytes);
        if (result == nullptr) {
            ROARING_TERMINATE("failed alloc while reading");
        }
        return Roaring64(result);
    }

    /**
     * Compute how many bytes would be read by readSafe. Returns 0 if the
     * serialized data is invalid.
     */
    static size_t serializedSizeInBytesSafe(const char* buf, size_t maxbytes) {
        return api::roaring64_bitmap_portable_deserialize_size(buf, maxbytes);
    }

    /**
     * Read a bitmap from a portable serialized buffer as a read-only view of
     * the container payloads. See
     * roaring64_bitmap_portable_deserialize_frozen(): the buffer must outlive
     * the returned bitmap, which must not be modified, and untrusted input
     * should be checked with internal_validate() before use.
     *
     * Terminates (ROARING_TERMINATE) if the buffer cannot be read, which
     * covers both a malformed buffer and an allocation failure.
     */
    static Roaring64 readSafeFrozen(const char* buf, size_t maxbytes) {
        roaring64_bitmap_t* result =
            api::roaring64_bitmap_portable_deserialize_frozen(buf, maxbytes);
        if (result == nullptr) {
            ROARING_TERMINATE("failed to read frozen portable bitmap");
        }
        return Roaring64(result);
    }

    typedef Roaring64ConstIterator const_iterator;

    /**
     * Return an iterator over the bitmap values, ordered from smallest to
     * largest.
     */
    const_iterator begin() const { return const_iterator(roaring, true); }

    /**
     * Return a past-the-end iterator. Decrementing it yields the last value.
     */
    const_iterator end() const { return const_iterator(roaring, false); }

    /**
     * Write the values of the bitmap, in sorted order, to `ans`. The caller is
     * responsible for allocating space for cardinality() values.
     */
    void toArray(uint64_t* ans) const noexcept {
        api::roaring64_bitmap_to_uint64_array(roaring, ans);
    }

   private:
    roaring64_bitmap_t* roaring;
};

}  // namespace roaring

#endif  // INCLUDE_ROARING_ROARING64_HH_
