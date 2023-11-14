#ifndef ARRAY_UTIL_H
#define ARRAY_UTIL_H

#include <stddef.h>  // for size_t
#include <stdint.h>

#include <roaring/portability.h>

#if CROARING_IS_X64
#ifndef CROARING_COMPILER_SUPPORTS_AVX512
#error "CROARING_COMPILER_SUPPORTS_AVX512 needs to be defined."
#endif // CROARING_COMPILER_SUPPORTS_AVX512
#endif

#ifdef __cplusplus
extern "C" { namespace roaring { namespace internal {
#endif

/*
 *  Good old binary search.
 *  Assumes that array is sorted, has logarithmic complexity.
 *  if the result is x, then:
 *     if ( x>0 )  you have array[x] = ikey
 *     if ( x<0 ) then inserting ikey at position -x-1 in array (insuring that array[-x-1]=ikey)
 *                   keys the array sorted.
 */
inline int32_t binarySearch(const uint16_t *array, int32_t lenarray,
                            uint16_t ikey) {
    int32_t low = 0;
    int32_t high = lenarray - 1;
    while (low <= high) {
        int32_t middleIndex = (low + high) >> 1;
        uint16_t middleValue = array[middleIndex];
        if (middleValue < ikey) {
            low = middleIndex + 1;
        } else if (middleValue > ikey) {
            high = middleIndex - 1;
        } else {
            return middleIndex;
        }
    }
    return -(low + 1);
}

/**
 * Galloping search
 * Assumes that array is sorted, has logarithmic complexity.
 * if the result is x, then if x = length, you have that all values in array between pos and length
 *    are smaller than min.
 * otherwise returns the first index x such that array[x] >= min.
 */
static inline int32_t advanceUntil(const uint16_t *array, int32_t pos,
                                   int32_t length, uint16_t min) {
    int32_t lower = pos + 1;

    if ((lower >= length) || (array[lower] >= min)) {
        return lower;
    }

    int32_t spansize = 1;

    while ((lower + spansize < length) && (array[lower + spansize] < min)) {
        spansize <<= 1;
    }
    int32_t upper = (lower + spansize < length) ? lower + spansize : length - 1;

    if (array[upper] == min) {
        return upper;
    }
    if (array[upper] < min) {
        // means
        // array
        // has no
        // item
        // >= min
        // pos = array.length;
        return length;
    }

    // we know that the next-smallest span was too small
    lower += (spansize >> 1);

    int32_t mid = 0;
    while (lower + 1 != upper) {
        mid = (lower + upper) >> 1;
        if (array[mid] == min) {
            return mid;
        } else if (array[mid] < min) {
            lower = mid;
        } else {
            upper = mid;
        }
    }
    return upper;
}

/**
 * Returns number of elements which are less than ikey.
 * Array elements must be unique and sorted.
 */
static inline int32_t count_less(const uint16_t *array, int32_t lenarray,
                                 uint16_t ikey) {
    if (lenarray == 0) return 0;
    int32_t pos = binarySearch(array, lenarray, ikey);
    return pos >= 0 ? pos : -(pos+1);
}

/**
 * Returns number of elements which are greater than ikey.
 * Array elements must be unique and sorted.
 */
static inline int32_t count_greater(const uint16_t *array, int32_t lenarray,
                                    uint16_t ikey) {
    if (lenarray == 0) return 0;
    int32_t pos = binarySearch(array, lenarray, ikey);
    if (pos >= 0) {
        return lenarray - (pos+1);
    } else {
        return lenarray - (-pos-1);
    }
}

/**
 * From Schlegel et al., Fast Sorted-Set Intersection using SIMD Instructions
 * Optimized by D. Lemire on May 3rd 2013
 *
 * C should have capacity greater than the minimum of s_1 and s_b + 8
 * where 8 is sizeof(__m128i)/sizeof(uint16_t).
 */
int32_t intersect_vector16(const uint16_t *__restrict__ A, size_t s_a,
                           const uint16_t *__restrict__ B, size_t s_b,
                           uint16_t *C);

int32_t intersect_vector16_inplace(uint16_t *__restrict__ A, size_t s_a,
                           const uint16_t *__restrict__ B, size_t s_b);

/**
 * Take an array container and write it out to a 32-bit array, using base
 * as the offset.
 */
int array_container_to_uint32_array_vector16(void *vout, const uint16_t* array, size_t cardinality,
                                    uint32_t base);
#if CROARING_COMPILER_SUPPORTS_AVX512
int avx512_array_container_to_uint32_array(void *vout, const uint16_t* array, size_t cardinality,
                                    uint32_t base);
#endif
/**
 * Compute the cardinality of the intersection using SSE4 instructions
 */
int32_t intersect_vector16_cardinality(const uint16_t *__restrict__ A,
                                       size_t s_a,
                                       const uint16_t *__restrict__ B,
                                       size_t s_b);

/* Computes the intersection between one small and one large set of uint16_t.
 * Stores the result into buffer and return the number of elements. */
int32_t intersect_skewed_uint16(const uint16_t *smallarray, size_t size_s,
                                const uint16_t *largearray, size_t size_l,
                                uint16_t *buffer);

/* Computes the size of the intersection between one small and one large set of
 * uint16_t. */
int32_t intersect_skewed_uint16_cardinality(const uint16_t *smallarray,
                                            size_t size_s,
                                            const uint16_t *largearray,
                                            size_t size_l);


/* Check whether the size of the intersection between one small and one large set of uint16_t is non-zero. */
bool intersect_skewed_uint16_nonempty(const uint16_t *smallarray, size_t size_s,
                                const uint16_t *largearray, size_t size_l);
/**
 * Generic intersection function.
 */
int32_t intersect_uint16(const uint16_t *A, const size_t lenA,
                         const uint16_t *B, const size_t lenB, uint16_t *out);
/**
 * Compute the size of the intersection (generic).
 */
int32_t intersect_uint16_cardinality(const uint16_t *A, const size_t lenA,
                                     const uint16_t *B, const size_t lenB);

/**
 * Checking whether the size of the intersection  is non-zero.
 */
bool intersect_uint16_nonempty(const uint16_t *A, const size_t lenA,
                         const uint16_t *B, const size_t lenB);
/**
 * Generic union function.
 */
size_t union_uint16(const uint16_t *set_1, size_t size_1, const uint16_t *set_2,
                    size_t size_2, uint16_t *buffer);

/**
 * Generic XOR function.
 */
int32_t xor_uint16(const uint16_t *array_1, int32_t card_1,
                   const uint16_t *array_2, int32_t card_2, uint16_t *out);

/**
 * Generic difference function (ANDNOT).
 */
int difference_uint16(const uint16_t *a1, int length1, const uint16_t *a2,
                      int length2, uint16_t *a_out);

/**
 * Generic intersection function.
 */
size_t intersection_uint32(const uint32_t *A, const size_t lenA,
                           const uint32_t *B, const size_t lenB, uint32_t *out);

/**
 * Generic intersection function, returns just the cardinality.
 */
size_t intersection_uint32_card(const uint32_t *A, const size_t lenA,
                                const uint32_t *B, const size_t lenB);

/**
 * Generic union function.
 */
size_t union_uint32(const uint32_t *set_1, size_t size_1, const uint32_t *set_2,
                    size_t size_2, uint32_t *buffer);

/**
 * A fast SSE-based union function.
 */
uint32_t union_vector16(const uint16_t *__restrict__ set_1, uint32_t size_1,
                        const uint16_t *__restrict__ set_2, uint32_t size_2,
                        uint16_t *__restrict__ buffer);
/**
 * A fast SSE-based XOR function.
 */
uint32_t xor_vector16(const uint16_t *__restrict__ array1, uint32_t length1,
                      const uint16_t *__restrict__ array2, uint32_t length2,
                      uint16_t *__restrict__ output);

/**
 * A fast SSE-based difference function.
 */
int32_t difference_vector16(const uint16_t *__restrict__ A, size_t s_a,
                            const uint16_t *__restrict__ B, size_t s_b,
                            uint16_t *C);

/**
 * Generic union function, returns just the cardinality.
 */
size_t union_uint32_card(const uint32_t *set_1, size_t size_1,
                         const uint32_t *set_2, size_t size_2);

/**
* combines union_uint16 and  union_vector16 optimally
*/
size_t fast_union_uint16(const uint16_t *set_1, size_t size_1, const uint16_t *set_2,
                    size_t size_2, uint16_t *buffer);


bool memequals(const void *s1, const void *s2, size_t n);

#ifdef __cplusplus
} } }  // extern "C" { namespace roaring { namespace internal {
#endif

#endif
