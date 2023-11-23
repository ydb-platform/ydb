/*
 * An implementation of Roaring Bitmaps in C.
 */

#ifndef ROARING_H
#define ROARING_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>  // for `size_t`

#include <roaring/memory.h>
#include <roaring/roaring_types.h>
#include <roaring/roaring_version.h>
#include <roaring/bitset/bitset.h>

#ifdef __cplusplus
extern "C" { namespace roaring { namespace api {
#endif

typedef struct roaring_bitmap_s {
    roaring_array_t high_low_container;
} roaring_bitmap_t;

/**
 * Dynamically allocates a new bitmap (initially empty).
 * Returns NULL if the allocation fails.
 * Capacity is a performance hint for how many "containers" the data will need.
 * Client is responsible for calling `roaring_bitmap_free()`.
 */
roaring_bitmap_t *roaring_bitmap_create_with_capacity(uint32_t cap);

/**
 * Dynamically allocates a new bitmap (initially empty).
 * Returns NULL if the allocation fails.
 * Client is responsible for calling `roaring_bitmap_free()`.
 */
inline roaring_bitmap_t *roaring_bitmap_create(void)
  { return roaring_bitmap_create_with_capacity(0); }

/**
 * Initialize a roaring bitmap structure in memory controlled by client.
 * Capacity is a performance hint for how many "containers" the data will need.
 * Can return false if auxiliary allocations fail when capacity greater than 0.
 */
bool roaring_bitmap_init_with_capacity(roaring_bitmap_t *r, uint32_t cap);

/**
 * Initialize a roaring bitmap structure in memory controlled by client.
 * The bitmap will be in a "clear" state, with no auxiliary allocations.
 * Since this performs no allocations, the function will not fail.
 */
inline void roaring_bitmap_init_cleared(roaring_bitmap_t *r)
  { roaring_bitmap_init_with_capacity(r, 0); }

/**
 * Add all the values between min (included) and max (excluded) that are at a
 * distance k*step from min.
*/
roaring_bitmap_t *roaring_bitmap_from_range(uint64_t min, uint64_t max,
                                            uint32_t step);

/**
 * Creates a new bitmap from a pointer of uint32_t integers
 */
roaring_bitmap_t *roaring_bitmap_of_ptr(size_t n_args, const uint32_t *vals);

/*
 * Whether you want to use copy-on-write.
 * Saves memory and avoids copies, but needs more care in a threaded context.
 * Most users should ignore this flag.
 *
 * Note: If you do turn this flag to 'true', enabling COW, then ensure that you
 * do so for all of your bitmaps, since interactions between bitmaps with and
 * without COW is unsafe.
 */
inline bool roaring_bitmap_get_copy_on_write(const roaring_bitmap_t* r) {
    return r->high_low_container.flags & ROARING_FLAG_COW;
}
inline void roaring_bitmap_set_copy_on_write(roaring_bitmap_t* r, bool cow) {
    if (cow) {
        r->high_low_container.flags |= ROARING_FLAG_COW;
    } else {
        r->high_low_container.flags &= ~ROARING_FLAG_COW;
    }
}

roaring_bitmap_t *roaring_bitmap_add_offset(const roaring_bitmap_t *bm,
                                            int64_t offset);
/**
 * Describe the inner structure of the bitmap.
 */
void roaring_bitmap_printf_describe(const roaring_bitmap_t *r);

/**
 * Creates a new bitmap from a list of uint32_t integers
 */
roaring_bitmap_t *roaring_bitmap_of(size_t n, ...);

/**
 * Copies a bitmap (this does memory allocation).
 * The caller is responsible for memory management.
 */
roaring_bitmap_t *roaring_bitmap_copy(const roaring_bitmap_t *r);

/**
 * Copies a bitmap from src to dest. It is assumed that the pointer dest
 * is to an already allocated bitmap. The content of the dest bitmap is
 * freed/deleted.
 *
 * It might be preferable and simpler to call roaring_bitmap_copy except
 * that roaring_bitmap_overwrite can save on memory allocations.
 *
 * Returns true if successful, or false if there was an error. On failure,
 * the dest bitmap is left in a valid, empty state (even if it was not empty before).
 */
bool roaring_bitmap_overwrite(roaring_bitmap_t *dest,
                              const roaring_bitmap_t *src);

/**
 * Print the content of the bitmap.
 */
void roaring_bitmap_printf(const roaring_bitmap_t *r);

/**
 * Computes the intersection between two bitmaps and returns new bitmap. The
 * caller is responsible for memory management.
 *
 * Performance hint: if you are computing the intersection between several
 * bitmaps, two-by-two, it is best to start with the smallest bitmap.
 * You may also rely on roaring_bitmap_and_inplace to avoid creating
 * many temporary bitmaps.
 */
roaring_bitmap_t *roaring_bitmap_and(const roaring_bitmap_t *r1,
                                     const roaring_bitmap_t *r2);

/**
 * Computes the size of the intersection between two bitmaps.
 */
uint64_t roaring_bitmap_and_cardinality(const roaring_bitmap_t *r1,
                                        const roaring_bitmap_t *r2);

/**
 * Check whether two bitmaps intersect.
 */
bool roaring_bitmap_intersect(const roaring_bitmap_t *r1,
                              const roaring_bitmap_t *r2);

/**
 * Check whether a bitmap and a closed range intersect.
 */
bool roaring_bitmap_intersect_with_range(const roaring_bitmap_t *bm,
                                         uint64_t x, uint64_t y);

/**
 * Computes the Jaccard index between two bitmaps. (Also known as the Tanimoto
 * distance, or the Jaccard similarity coefficient)
 *
 * The Jaccard index is undefined if both bitmaps are empty.
 */
double roaring_bitmap_jaccard_index(const roaring_bitmap_t *r1,
                                    const roaring_bitmap_t *r2);

/**
 * Computes the size of the union between two bitmaps.
 */
uint64_t roaring_bitmap_or_cardinality(const roaring_bitmap_t *r1,
                                       const roaring_bitmap_t *r2);

/**
 * Computes the size of the difference (andnot) between two bitmaps.
 */
uint64_t roaring_bitmap_andnot_cardinality(const roaring_bitmap_t *r1,
                                           const roaring_bitmap_t *r2);

/**
 * Computes the size of the symmetric difference (xor) between two bitmaps.
 */
uint64_t roaring_bitmap_xor_cardinality(const roaring_bitmap_t *r1,
                                        const roaring_bitmap_t *r2);

/**
 * Inplace version of `roaring_bitmap_and()`, modifies r1
 * r1 == r2 is allowed.
 *
 * Performance hint: if you are computing the intersection between several
 * bitmaps, two-by-two, it is best to start with the smallest bitmap.
 */
void roaring_bitmap_and_inplace(roaring_bitmap_t *r1,
                                const roaring_bitmap_t *r2);

/**
 * Computes the union between two bitmaps and returns new bitmap. The caller is
 * responsible for memory management.
 */
roaring_bitmap_t *roaring_bitmap_or(const roaring_bitmap_t *r1,
                                    const roaring_bitmap_t *r2);

/**
 * Inplace version of `roaring_bitmap_or(), modifies r1.
 * TODO: decide whether r1 == r2 ok
 */
void roaring_bitmap_or_inplace(roaring_bitmap_t *r1,
                               const roaring_bitmap_t *r2);

/**
 * Compute the union of 'number' bitmaps.
 * Caller is responsible for freeing the result.
 * See also `roaring_bitmap_or_many_heap()`
 */
roaring_bitmap_t *roaring_bitmap_or_many(size_t number,
                                         const roaring_bitmap_t **rs);

/**
 * Compute the union of 'number' bitmaps using a heap. This can sometimes be
 * faster than `roaring_bitmap_or_many() which uses a naive algorithm.
 * Caller is responsible for freeing the result.
 */
roaring_bitmap_t *roaring_bitmap_or_many_heap(uint32_t number,
                                              const roaring_bitmap_t **rs);

/**
 * Computes the symmetric difference (xor) between two bitmaps
 * and returns new bitmap. The caller is responsible for memory management.
 */
roaring_bitmap_t *roaring_bitmap_xor(const roaring_bitmap_t *r1,
                                     const roaring_bitmap_t *r2);

/**
 * Inplace version of roaring_bitmap_xor, modifies r1, r1 != r2.
 */
void roaring_bitmap_xor_inplace(roaring_bitmap_t *r1,
                                const roaring_bitmap_t *r2);

/**
 * Compute the xor of 'number' bitmaps.
 * Caller is responsible for freeing the result.
 */
roaring_bitmap_t *roaring_bitmap_xor_many(size_t number,
                                          const roaring_bitmap_t **rs);

/**
 * Computes the difference (andnot) between two bitmaps and returns new bitmap.
 * Caller is responsible for freeing the result.
 */
roaring_bitmap_t *roaring_bitmap_andnot(const roaring_bitmap_t *r1,
                                        const roaring_bitmap_t *r2);

/**
 * Inplace version of roaring_bitmap_andnot, modifies r1, r1 != r2.
 */
void roaring_bitmap_andnot_inplace(roaring_bitmap_t *r1,
                                   const roaring_bitmap_t *r2);

/**
 * TODO: consider implementing:
 *
 * "Compute the xor of 'number' bitmaps using a heap. This can sometimes be
 *  faster than roaring_bitmap_xor_many which uses a naive algorithm. Caller is
 *  responsible for freeing the result.""
 *
 * roaring_bitmap_t *roaring_bitmap_xor_many_heap(uint32_t number,
 *                                                const roaring_bitmap_t **rs);
 */

/**
 * Frees the memory.
 */
void roaring_bitmap_free(const roaring_bitmap_t *r);

/**
 * A bit of context usable with `roaring_bitmap_*_bulk()` functions
 *
 * Should be initialized with `{0}` (or `memset()` to all zeros).
 * Callers should treat it as an opaque type.
 *
 * A context may only be used with a single bitmap
 * (unless re-initialized to zero), and any modification to a bitmap
 * (other than modifications performed with `_bulk()` functions with the context
 * passed) will invalidate any contexts associated with that bitmap.
 */
typedef struct roaring_bulk_context_s {
    ROARING_CONTAINER_T *container;
    int idx;
    uint16_t key;
    uint8_t typecode;
} roaring_bulk_context_t;

/**
 * Add an item, using context from a previous insert for speed optimization.
 *
 * `context` will be used to store information between calls to make bulk
 * operations faster. `*context` should be zero-initialized before the first
 * call to this function.
 *
 * Modifying the bitmap in any way (other than `-bulk` suffixed functions)
 * will invalidate the stored context, calling this function with a non-zero
 * context after doing any modification invokes undefined behavior.
 *
 * In order to exploit this optimization, the caller should call this function
 * with values with the same "key" (high 16 bits of the value) consecutively.
 */
void roaring_bitmap_add_bulk(roaring_bitmap_t *r,
                             roaring_bulk_context_t *context, uint32_t val);

/**
 * Add value n_args from pointer vals, faster than repeatedly calling
 * `roaring_bitmap_add()`
 *
 * In order to exploit this optimization, the caller should attempt to keep
 * values with the same "key" (high 16 bits of the value) as consecutive
 * elements in `vals`
 */
void roaring_bitmap_add_many(roaring_bitmap_t *r, size_t n_args,
                             const uint32_t *vals);

/**
 * Add value x
 */
void roaring_bitmap_add(roaring_bitmap_t *r, uint32_t x);

/**
 * Add value x
 * Returns true if a new value was added, false if the value already existed.
 */
bool roaring_bitmap_add_checked(roaring_bitmap_t *r, uint32_t x);

/**
 * Add all values in range [min, max]
 */
void roaring_bitmap_add_range_closed(roaring_bitmap_t *r,
                                     uint32_t min, uint32_t max);

/**
 * Add all values in range [min, max)
 */
inline void roaring_bitmap_add_range(roaring_bitmap_t *r,
                                     uint64_t min, uint64_t max) {
    if(max <= min) return;
    roaring_bitmap_add_range_closed(r, (uint32_t)min, (uint32_t)(max - 1));
}

/**
 * Remove value x
 */
void roaring_bitmap_remove(roaring_bitmap_t *r, uint32_t x);

/**
 * Remove all values in range [min, max]
 */
void roaring_bitmap_remove_range_closed(roaring_bitmap_t *r,
                                        uint32_t min, uint32_t max);

/**
 * Remove all values in range [min, max)
 */
inline void roaring_bitmap_remove_range(roaring_bitmap_t *r,
                                        uint64_t min, uint64_t max) {
    if(max <= min) return;
    roaring_bitmap_remove_range_closed(r, (uint32_t)min, (uint32_t)(max - 1));
}

/**
 * Remove multiple values
 */
void roaring_bitmap_remove_many(roaring_bitmap_t *r, size_t n_args,
                                const uint32_t *vals);

/**
 * Remove value x
 * Returns true if a new value was removed, false if the value was not existing.
 */
bool roaring_bitmap_remove_checked(roaring_bitmap_t *r, uint32_t x);

/**
 * Check if value is present
 */
bool roaring_bitmap_contains(const roaring_bitmap_t *r, uint32_t val);

/**
 * Check whether a range of values from range_start (included)
 * to range_end (excluded) is present
 */
bool roaring_bitmap_contains_range(const roaring_bitmap_t *r,
                                   uint64_t range_start,
                                   uint64_t range_end);

/**
 * Check if an items is present, using context from a previous insert or search
 * for speed optimization.
 *
 * `context` will be used to store information between calls to make bulk
 * operations faster. `*context` should be zero-initialized before the first
 * call to this function.
 *
 * Modifying the bitmap in any way (other than `-bulk` suffixed functions)
 * will invalidate the stored context, calling this function with a non-zero
 * context after doing any modification invokes undefined behavior.
 *
 * In order to exploit this optimization, the caller should call this function
 * with values with the same "key" (high 16 bits of the value) consecutively.
 */
bool roaring_bitmap_contains_bulk(const roaring_bitmap_t *r,
                                  roaring_bulk_context_t *context,
                                  uint32_t val);

/**
 * Get the cardinality of the bitmap (number of elements).
 */
uint64_t roaring_bitmap_get_cardinality(const roaring_bitmap_t *r);

/**
 * Returns the number of elements in the range [range_start, range_end).
 */
uint64_t roaring_bitmap_range_cardinality(const roaring_bitmap_t *r,
                                          uint64_t range_start,
                                          uint64_t range_end);

/**
* Returns true if the bitmap is empty (cardinality is zero).
*/
bool roaring_bitmap_is_empty(const roaring_bitmap_t *r);


/**
 * Empties the bitmap.  It will have no auxiliary allocations (so if the bitmap
 * was initialized in client memory via roaring_bitmap_init(), then a call to
 * roaring_bitmap_clear() would be enough to "free" it)
 */
void roaring_bitmap_clear(roaring_bitmap_t *r);

/**
 * Convert the bitmap to a sorted array, output in `ans`.
 *
 * Caller is responsible to ensure that there is enough memory allocated, e.g.
 *
 *     ans = malloc(roaring_bitmap_get_cardinality(bitmap) * sizeof(uint32_t));
 */
void roaring_bitmap_to_uint32_array(const roaring_bitmap_t *r, uint32_t *ans);

/**
 * Store the bitmap to a bitset. This can be useful for people
 * who need the performance and simplicity of a standard bitset.
 * We assume that the input bitset is originally empty (does not
 * have any set bit).
 *
 *   bitset_t * out = bitset_create();
 *   // if the bitset has content in it, call "bitset_clear(out)"
 *   bool success = roaring_bitmap_to_bitset(mybitmap, out); 
 *   // on failure, success will be false.
 *   // You can then query the bitset:
 *   bool is_present = bitset_get(out,  10011 );
 *   // you must free the memory:
 *   bitset_free(out);
 *
 */
bool roaring_bitmap_to_bitset(const roaring_bitmap_t *r, bitset_t * bitset);

/**
 * Convert the bitmap to a sorted array from `offset` by `limit`, output in `ans`.
 *
 * Caller is responsible to ensure that there is enough memory allocated, e.g.
 *
 *     ans = malloc(roaring_bitmap_get_cardinality(limit) * sizeof(uint32_t));
 *
 * Return false in case of failure (e.g., insufficient memory)
 */
bool roaring_bitmap_range_uint32_array(const roaring_bitmap_t *r,
                                       size_t offset, size_t limit,
                                       uint32_t *ans);

/**
 * Remove run-length encoding even when it is more space efficient.
 * Return whether a change was applied.
 */
bool roaring_bitmap_remove_run_compression(roaring_bitmap_t *r);

/**
 * Convert array and bitmap containers to run containers when it is more
 * efficient; also convert from run containers when more space efficient.
 *
 * Returns true if the result has at least one run container.
 * Additional savings might be possible by calling `shrinkToFit()`.
 */
bool roaring_bitmap_run_optimize(roaring_bitmap_t *r);

/**
 * If needed, reallocate memory to shrink the memory usage.
 * Returns the number of bytes saved.
 */
size_t roaring_bitmap_shrink_to_fit(roaring_bitmap_t *r);

/**
 * Write the bitmap to an output pointer, this output buffer should refer to
 * at least `roaring_bitmap_size_in_bytes(r)` allocated bytes.
 *
 * See `roaring_bitmap_portable_serialize()` if you want a format that's
 * compatible with Java and Go implementations.  This format can sometimes be
 * more space efficient than the portable form, e.g. when the data is sparse.
 *
 * Returns how many bytes written, should be `roaring_bitmap_size_in_bytes(r)`.
 *
 * This function is endian-sensitive. If you have a big-endian system (e.g., a mainframe IBM s390x),
 * the data format is going to be big-endian and not compatible with little-endian systems.
 */
size_t roaring_bitmap_serialize(const roaring_bitmap_t *r, char *buf);

/**
 * Use with `roaring_bitmap_serialize()`.
 *
 * (See `roaring_bitmap_portable_deserialize()` if you want a format that's
 * compatible with Java and Go implementations).
 *
 * This function is endian-sensitive. If you have a big-endian system (e.g., a mainframe IBM s390x),
 * the data format is going to be big-endian and not compatible with little-endian systems.
 */
roaring_bitmap_t *roaring_bitmap_deserialize(const void *buf);

/**
 * Use with `roaring_bitmap_serialize()`.
 *
 * (See `roaring_bitmap_portable_deserialize_safe()` if you want a format that's
 * compatible with Java and Go implementations).
 *
 * This function is endian-sensitive. If you have a big-endian system (e.g., a mainframe IBM s390x),
 * the data format is going to be big-endian and not compatible with little-endian systems.
 * 
 * The difference with `roaring_bitmap_deserialize()` is that this function checks that the input buffer
 * is a valid bitmap.  If the buffer is too small, NULL is returned.
 */
roaring_bitmap_t *roaring_bitmap_deserialize_safe(const void *buf, size_t maxbytes);

/**
 * How many bytes are required to serialize this bitmap (NOT compatible
 * with Java and Go versions)
 */
size_t roaring_bitmap_size_in_bytes(const roaring_bitmap_t *r);

/**
 * Read bitmap from a serialized buffer.
 * In case of failure, NULL is returned.
 *
 * This function is unsafe in the sense that if there is no valid serialized
 * bitmap at the pointer, then many bytes could be read, possibly causing a
 * buffer overflow.  See also roaring_bitmap_portable_deserialize_safe().
 *
 * This is meant to be compatible with the Java and Go versions:
 * https://github.com/RoaringBitmap/RoaringFormatSpec
*
 * This function is endian-sensitive. If you have a big-endian system (e.g., a mainframe IBM s390x),
 * the data format is going to be big-endian and not compatible with little-endian systems.
 */
roaring_bitmap_t *roaring_bitmap_portable_deserialize(const char *buf);

/**
 * Read bitmap from a serialized buffer safely (reading up to maxbytes).
 * In case of failure, NULL is returned.
 *
 * This is meant to be compatible with the Java and Go versions:
 * https://github.com/RoaringBitmap/RoaringFormatSpec
 *
 * The function itself is safe in the sense that it will not cause buffer overflows.
 * However, for correct operations, it is assumed that the bitmap read was once
 * serialized from a valid bitmap (i.e., it follows the format specification).
 * If you provided an incorrect input (garbage), then the bitmap read may not be in
 * a valid state and following operations may not lead to sensible results.
 * In particular, the serialized array containers need to be in sorted order, and the
 * run containers should be in sorted non-overlapping order. This is is guaranteed to
 * happen when serializing an existing bitmap, but not for random inputs.
 *
 * This function is endian-sensitive. If you have a big-endian system (e.g., a mainframe IBM s390x),
 * the data format is going to be big-endian and not compatible with little-endian systems.
 */
roaring_bitmap_t *roaring_bitmap_portable_deserialize_safe(const char *buf,
                                                           size_t maxbytes);

/**
 * Read bitmap from a serialized buffer.
 * In case of failure, NULL is returned.
 *
 * Bitmap returned by this function can be used in all readonly contexts.
 * Bitmap must be freed as usual, by calling roaring_bitmap_free().
 * Underlying buffer must not be freed or modified while it backs any bitmaps.
 *
 * The function is unsafe in the following ways:
 * 1) It may execute unaligned memory accesses.
 * 2) A buffer overflow may occur if buf does not point to a valid serialized
 *    bitmap.
 *
 * This is meant to be compatible with the Java and Go versions:
 * https://github.com/RoaringBitmap/RoaringFormatSpec
 *
 * This function is endian-sensitive. If you have a big-endian system (e.g., a mainframe IBM s390x),
 * the data format is going to be big-endian and not compatible with little-endian systems.
 */
roaring_bitmap_t *roaring_bitmap_portable_deserialize_frozen(const char *buf);

/**
 * Check how many bytes would be read (up to maxbytes) at this pointer if there
 * is a bitmap, returns zero if there is no valid bitmap.
 *
 * This is meant to be compatible with the Java and Go versions:
 * https://github.com/RoaringBitmap/RoaringFormatSpec
 */
size_t roaring_bitmap_portable_deserialize_size(const char *buf,
                                                size_t maxbytes);

/**
 * How many bytes are required to serialize this bitmap.
 *
 * This is meant to be compatible with the Java and Go versions:
 * https://github.com/RoaringBitmap/RoaringFormatSpec
 */
size_t roaring_bitmap_portable_size_in_bytes(const roaring_bitmap_t *r);

/**
 * Write a bitmap to a char buffer.  The output buffer should refer to at least
 * `roaring_bitmap_portable_size_in_bytes(r)` bytes of allocated memory.
 *
 * Returns how many bytes were written which should match
 * `roaring_bitmap_portable_size_in_bytes(r)`.
 *
 * This is meant to be compatible with the Java and Go versions:
 * https://github.com/RoaringBitmap/RoaringFormatSpec
 *
 * This function is endian-sensitive. If you have a big-endian system (e.g., a mainframe IBM s390x),
 * the data format is going to be big-endian and not compatible with little-endian systems.
 */
size_t roaring_bitmap_portable_serialize(const roaring_bitmap_t *r, char *buf);

/*
 * "Frozen" serialization format imitates memory layout of roaring_bitmap_t.
 * Deserialized bitmap is a constant view of the underlying buffer.
 * This significantly reduces amount of allocations and copying required during
 * deserialization.
 * It can be used with memory mapped files.
 * Example can be found in benchmarks/frozen_benchmark.c
 *
 *         [#####] const roaring_bitmap_t *
 *          | | |
 *     +----+ | +-+
 *     |      |   |
 * [#####################################] underlying buffer
 *
 * Note that because frozen serialization format imitates C memory layout
 * of roaring_bitmap_t, it is not fixed. It is different on big/little endian
 * platforms and can be changed in future.
 */

/**
 * Returns number of bytes required to serialize bitmap using frozen format.
 */
size_t roaring_bitmap_frozen_size_in_bytes(const roaring_bitmap_t *r);

/**
 * Serializes bitmap using frozen format.
 * Buffer size must be at least roaring_bitmap_frozen_size_in_bytes().
 *
 * This function is endian-sensitive. If you have a big-endian system (e.g., a mainframe IBM s390x),
 * the data format is going to be big-endian and not compatible with little-endian systems.
 */
void roaring_bitmap_frozen_serialize(const roaring_bitmap_t *r, char *buf);

/**
 * Creates constant bitmap that is a view of a given buffer.
 * Buffer data should have been written by `roaring_bitmap_frozen_serialize()`
 * Its beginning must also be aligned by 32 bytes.
 * Length must be equal exactly to `roaring_bitmap_frozen_size_in_bytes()`.
 * In case of failure, NULL is returned.
 *
 * Bitmap returned by this function can be used in all readonly contexts.
 * Bitmap must be freed as usual, by calling roaring_bitmap_free().
 * Underlying buffer must not be freed or modified while it backs any bitmaps.
 *
 * This function is endian-sensitive. If you have a big-endian system (e.g., a mainframe IBM s390x),
 * the data format is going to be big-endian and not compatible with little-endian systems.
 */
const roaring_bitmap_t *roaring_bitmap_frozen_view(const char *buf,
                                                   size_t length);

/**
 * Iterate over the bitmap elements. The function iterator is called once for
 * all the values with ptr (can be NULL) as the second parameter of each call.
 *
 * `roaring_iterator` is simply a pointer to a function that returns bool
 * (true means that the iteration should continue while false means that it
 * should stop), and takes (uint32_t,void*) as inputs.
 *
 * Returns true if the roaring_iterator returned true throughout (so that all
 * data points were necessarily visited).
 *
 * Iteration is ordered: from the smallest to the largest elements.
 */
bool roaring_iterate(const roaring_bitmap_t *r, roaring_iterator iterator,
                     void *ptr);

bool roaring_iterate64(const roaring_bitmap_t *r, roaring_iterator64 iterator,
                       uint64_t high_bits, void *ptr);

/**
 * Return true if the two bitmaps contain the same elements.
 */
bool roaring_bitmap_equals(const roaring_bitmap_t *r1,
                           const roaring_bitmap_t *r2);

/**
 * Return true if all the elements of r1 are also in r2.
 */
bool roaring_bitmap_is_subset(const roaring_bitmap_t *r1,
                              const roaring_bitmap_t *r2);

/**
 * Return true if all the elements of r1 are also in r2, and r2 is strictly
 * greater than r1.
 */
bool roaring_bitmap_is_strict_subset(const roaring_bitmap_t *r1,
                                     const roaring_bitmap_t *r2);

/**
 * (For expert users who seek high performance.)
 *
 * Computes the union between two bitmaps and returns new bitmap. The caller is
 * responsible for memory management.
 *
 * The lazy version defers some computations such as the maintenance of the
 * cardinality counts. Thus you must call `roaring_bitmap_repair_after_lazy()`
 * after executing "lazy" computations.
 *
 * It is safe to repeatedly call roaring_bitmap_lazy_or_inplace on the result.
 *
 * `bitsetconversion` is a flag which determines whether container-container
 * operations force a bitset conversion.
 */
roaring_bitmap_t *roaring_bitmap_lazy_or(const roaring_bitmap_t *r1,
                                         const roaring_bitmap_t *r2,
                                         const bool bitsetconversion);

/**
 * (For expert users who seek high performance.)
 *
 * Inplace version of roaring_bitmap_lazy_or, modifies r1.
 *
 * `bitsetconversion` is a flag which determines whether container-container
 * operations force a bitset conversion.
 */
void roaring_bitmap_lazy_or_inplace(roaring_bitmap_t *r1,
                                    const roaring_bitmap_t *r2,
                                    const bool bitsetconversion);

/**
 * (For expert users who seek high performance.)
 *
 * Execute maintenance on a bitmap created from `roaring_bitmap_lazy_or()`
 * or modified with `roaring_bitmap_lazy_or_inplace()`.
 */
void roaring_bitmap_repair_after_lazy(roaring_bitmap_t *r1);

/**
 * Computes the symmetric difference between two bitmaps and returns new bitmap.
 * The caller is responsible for memory management.
 *
 * The lazy version defers some computations such as the maintenance of the
 * cardinality counts. Thus you must call `roaring_bitmap_repair_after_lazy()`
 * after executing "lazy" computations.
 *
 * It is safe to repeatedly call `roaring_bitmap_lazy_xor_inplace()` on
 * the result.
 */
roaring_bitmap_t *roaring_bitmap_lazy_xor(const roaring_bitmap_t *r1,
                                          const roaring_bitmap_t *r2);

/**
 * (For expert users who seek high performance.)
 *
 * Inplace version of roaring_bitmap_lazy_xor, modifies r1. r1 != r2
 */
void roaring_bitmap_lazy_xor_inplace(roaring_bitmap_t *r1,
                                     const roaring_bitmap_t *r2);

/**
 * Compute the negation of the bitmap in the interval [range_start, range_end).
 * The number of negated values is range_end - range_start.
 * Areas outside the range are passed through unchanged.
 */
roaring_bitmap_t *roaring_bitmap_flip(const roaring_bitmap_t *r1,
                                      uint64_t range_start, uint64_t range_end);

/**
 * compute (in place) the negation of the roaring bitmap within a specified
 * interval: [range_start, range_end). The number of negated values is
 * range_end - range_start.
 * Areas outside the range are passed through unchanged.
 */
void roaring_bitmap_flip_inplace(roaring_bitmap_t *r1, uint64_t range_start,
                                 uint64_t range_end);

/**
 * Selects the element at index 'rank' where the smallest element is at index 0.
 * If the size of the roaring bitmap is strictly greater than rank, then this
 * function returns true and sets element to the element of given rank.
 * Otherwise, it returns false.
 */
bool roaring_bitmap_select(const roaring_bitmap_t *r, uint32_t rank,
                           uint32_t *element);

/**
 * roaring_bitmap_rank returns the number of integers that are smaller or equal
 * to x. Thus if x is the first element, this function will return 1. If
 * x is smaller than the smallest element, this function will return 0.
 *
 * The indexing convention differs between roaring_bitmap_select and
 * roaring_bitmap_rank: roaring_bitmap_select refers to the smallest value
 * as having index 0, whereas roaring_bitmap_rank returns 1 when ranking
 * the smallest value.
 */
uint64_t roaring_bitmap_rank(const roaring_bitmap_t *r, uint32_t x);

/**
 * Returns the index of x in the given roaring bitmap.
 * If the roaring bitmap doesn't contain x , this function will return -1.
 * The difference with rank function is that this function will return -1 when x
 * is not the element of roaring bitmap, but the rank function will return a
 * non-negative number.
 */
int64_t roaring_bitmap_get_index(const roaring_bitmap_t *r, uint32_t x);

/**
 * Returns the smallest value in the set, or UINT32_MAX if the set is empty.
 */
uint32_t roaring_bitmap_minimum(const roaring_bitmap_t *r);

/**
 * Returns the greatest value in the set, or 0 if the set is empty.
 */
uint32_t roaring_bitmap_maximum(const roaring_bitmap_t *r);

/**
 * (For advanced users.)
 *
 * Collect statistics about the bitmap, see roaring_types.h for
 * a description of roaring_statistics_t
 */
void roaring_bitmap_statistics(const roaring_bitmap_t *r,
                               roaring_statistics_t *stat);

/**
 * Perform internal consistency checks. Returns true if the bitmap is consistent.
 *
 * Note that some operations intentionally leave bitmaps in an inconsistent state temporarily,
 * for example, `roaring_bitmap_lazy_*` functions, until `roaring_bitmap_repair_after_lazy` is called.
 *
 * If reason is non-null, it will be set to a string describing the first inconsistency found if any.
 */
bool roaring_bitmap_internal_validate(const roaring_bitmap_t *r, const char **reason);

/*********************
* What follows is code use to iterate through values in a roaring bitmap

roaring_bitmap_t *r =...
roaring_uint32_iterator_t i;
roaring_create_iterator(r, &i);
while(i.has_value) {
  printf("value = %d\n", i.current_value);
  roaring_advance_uint32_iterator(&i);
}

Obviously, if you modify the underlying bitmap, the iterator
becomes invalid. So don't.
*/

typedef struct roaring_uint32_iterator_s {
    const roaring_bitmap_t *parent;  // owner
    int32_t container_index;         // point to the current container index
    int32_t in_container_index;  // for bitset and array container, this is out
                                 // index
    int32_t run_index;           // for run container, this points  at the run

    uint32_t current_value;
    bool has_value;

    const ROARING_CONTAINER_T
        *container;  // should be:
                     // parent->high_low_container.containers[container_index];
    uint8_t typecode;  // should be:
                       // parent->high_low_container.typecodes[container_index];
    uint32_t highbits;  // should be:
                        // parent->high_low_container.keys[container_index]) <<
                        // 16;

} roaring_uint32_iterator_t;

/**
 * Initialize an iterator object that can be used to iterate through the
 * values. If there is a  value, then this iterator points to the first value
 * and `it->has_value` is true. The value is in `it->current_value`.
 */
void roaring_init_iterator(const roaring_bitmap_t *r,
                           roaring_uint32_iterator_t *newit);

/**
 * Initialize an iterator object that can be used to iterate through the
 * values. If there is a value, then this iterator points to the last value
 * and `it->has_value` is true. The value is in `it->current_value`.
 */
void roaring_init_iterator_last(const roaring_bitmap_t *r,
                                roaring_uint32_iterator_t *newit);

/**
 * Create an iterator object that can be used to iterate through the values.
 * Caller is responsible for calling `roaring_free_iterator()`.
 *
 * The iterator is initialized (this function calls `roaring_init_iterator()`)
 * If there is a value, then this iterator points to the first value and
 * `it->has_value` is true.  The value is in `it->current_value`.
 */
roaring_uint32_iterator_t *roaring_create_iterator(const roaring_bitmap_t *r);

/**
* Advance the iterator. If there is a new value, then `it->has_value` is true.
* The new value is in `it->current_value`. Values are traversed in increasing
* orders. For convenience, returns `it->has_value`.
*/
bool roaring_advance_uint32_iterator(roaring_uint32_iterator_t *it);

/**
* Decrement the iterator. If there's a new value, then `it->has_value` is true.
* The new value is in `it->current_value`. Values are traversed in decreasing
* order. For convenience, returns `it->has_value`.
*/
bool roaring_previous_uint32_iterator(roaring_uint32_iterator_t *it);

/**
 * Move the iterator to the first value >= `val`. If there is a such a value,
 * then `it->has_value` is true. The new value is in `it->current_value`.
 * For convenience, returns `it->has_value`.
 */
bool roaring_move_uint32_iterator_equalorlarger(roaring_uint32_iterator_t *it,
                                                uint32_t val);

/**
 * Creates a copy of an iterator.
 * Caller must free it.
 */
roaring_uint32_iterator_t *roaring_copy_uint32_iterator(
    const roaring_uint32_iterator_t *it);

/**
 * Free memory following `roaring_create_iterator()`
 */
void roaring_free_uint32_iterator(roaring_uint32_iterator_t *it);

/*
 * Reads next ${count} values from iterator into user-supplied ${buf}.
 * Returns the number of read elements.
 * This number can be smaller than ${count}, which means that iterator is drained.
 *
 * This function satisfies semantics of iteration and can be used together with
 * other iterator functions.
 *  - first value is copied from ${it}->current_value
 *  - after function returns, iterator is positioned at the next element
 */
uint32_t roaring_read_uint32_iterator(roaring_uint32_iterator_t *it,
                                      uint32_t* buf, uint32_t count);

#ifdef __cplusplus
} } }  // extern "C" { namespace roaring { namespace api {
#endif

#endif  /* ROARING_H */

#ifdef __cplusplus
    /**
     * Best practices for C++ headers is to avoid polluting global scope.
     * But for C compatibility when just `roaring.h` is included building as
     * C++, default to global access for the C public API.
     *
     * BUT when `roaring.hh` is included instead, it sets this flag.  That way
     * explicit namespacing must be used to get the C functions.
     *
     * This is outside the include guard so that if you include BOTH headers,
     * the order won't matter; you still get the global definitions.
     */
    #if !defined(ROARING_API_NOT_IN_GLOBAL_NAMESPACE)
        using namespace ::roaring::api;
    #endif
#endif
