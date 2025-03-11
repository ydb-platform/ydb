#include <assert.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>

#include <roaring/art/art.h>
#include <roaring/portability.h>
#include <roaring/roaring64.h>

// For serialization / deserialization
#include <roaring/roaring.h>
#include <roaring/roaring_array.h>
// containers.h last to avoid conflict with ROARING_CONTAINER_T.
#include <roaring/containers/containers.h>

#ifdef __cplusplus
using namespace ::roaring::internal;

extern "C" {
namespace roaring {
namespace api {
#endif

// TODO: Copy on write.
// TODO: Error on failed allocation.

typedef struct roaring64_bitmap_s {
    art_t art;
    uint8_t flags;
} roaring64_bitmap_t;

// Leaf type of the ART used to keep the high 48 bits of each entry.
typedef struct roaring64_leaf_s {
    art_val_t _pad;
    uint8_t typecode;
    container_t *container;
} roaring64_leaf_t;

// Alias to make it easier to work with, since it's an internal-only type
// anyway.
typedef struct roaring64_leaf_s leaf_t;

// Iterator struct to hold iteration state.
typedef struct roaring64_iterator_s {
    const roaring64_bitmap_t *parent;
    art_iterator_t art_it;
    roaring_container_iterator_t container_it;
    uint64_t high48;  // Key that art_it points to.

    uint64_t value;
    bool has_value;

    // If has_value is false, then the iterator is saturated. This field
    // indicates the direction of saturation. If true, there are no more values
    // in the forward direction. If false, there are no more values in the
    // backward direction.
    bool saturated_forward;
} roaring64_iterator_t;

// Splits the given uint64 key into high 48 bit and low 16 bit components.
// Expects high48_out to be of length ART_KEY_BYTES.
static inline uint16_t split_key(uint64_t key, uint8_t high48_out[]) {
    uint64_t tmp = croaring_htobe64(key);
    memcpy(high48_out, (uint8_t *)(&tmp), ART_KEY_BYTES);
    return (uint16_t)key;
}

// Recombines the high 48 bit and low 16 bit components into a uint64 key.
// Expects high48_out to be of length ART_KEY_BYTES.
static inline uint64_t combine_key(const uint8_t high48[], uint16_t low16) {
    uint64_t result = 0;
    memcpy((uint8_t *)(&result), high48, ART_KEY_BYTES);
    return croaring_be64toh(result) | low16;
}

static inline uint64_t minimum(uint64_t a, uint64_t b) {
    return (a < b) ? a : b;
}

static inline leaf_t *create_leaf(container_t *container, uint8_t typecode) {
    leaf_t *leaf = (leaf_t *)roaring_malloc(sizeof(leaf_t));
    leaf->container = container;
    leaf->typecode = typecode;
    return leaf;
}

static inline leaf_t *copy_leaf_container(const leaf_t *leaf) {
    leaf_t *result_leaf = (leaf_t *)roaring_malloc(sizeof(leaf_t));
    result_leaf->typecode = leaf->typecode;
    // get_copy_of_container modifies the typecode passed in.
    result_leaf->container = get_copy_of_container(
        leaf->container, &result_leaf->typecode, /*copy_on_write=*/false);
    return result_leaf;
}

static inline void free_leaf(leaf_t *leaf) { roaring_free(leaf); }

static inline int compare_high48(art_key_chunk_t key1[],
                                 art_key_chunk_t key2[]) {
    return art_compare_keys(key1, key2);
}

static inline bool roaring64_iterator_init_at_leaf_first(
    roaring64_iterator_t *it) {
    it->high48 = combine_key(it->art_it.key, 0);
    leaf_t *leaf = (leaf_t *)it->art_it.value;
    uint16_t low16 = 0;
    it->container_it =
        container_init_iterator(leaf->container, leaf->typecode, &low16);
    it->value = it->high48 | low16;
    return (it->has_value = true);
}

static inline bool roaring64_iterator_init_at_leaf_last(
    roaring64_iterator_t *it) {
    it->high48 = combine_key(it->art_it.key, 0);
    leaf_t *leaf = (leaf_t *)it->art_it.value;
    uint16_t low16 = 0;
    it->container_it =
        container_init_iterator_last(leaf->container, leaf->typecode, &low16);
    it->value = it->high48 | low16;
    return (it->has_value = true);
}

static inline roaring64_iterator_t *roaring64_iterator_init_at(
    const roaring64_bitmap_t *r, roaring64_iterator_t *it, bool first) {
    it->parent = r;
    it->art_it = art_init_iterator(&r->art, first);
    it->has_value = it->art_it.value != NULL;
    if (it->has_value) {
        if (first) {
            roaring64_iterator_init_at_leaf_first(it);
        } else {
            roaring64_iterator_init_at_leaf_last(it);
        }
    } else {
        it->saturated_forward = first;
    }
    return it;
}

roaring64_bitmap_t *roaring64_bitmap_create(void) {
    roaring64_bitmap_t *r =
        (roaring64_bitmap_t *)roaring_malloc(sizeof(roaring64_bitmap_t));
    r->art.root = NULL;
    r->flags = 0;
    return r;
}

void roaring64_bitmap_free(roaring64_bitmap_t *r) {
    if (!r) {
        return;
    }
    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    while (it.value != NULL) {
        leaf_t *leaf = (leaf_t *)it.value;
        container_free(leaf->container, leaf->typecode);
        free_leaf(leaf);
        art_iterator_next(&it);
    }
    art_free(&r->art);
    roaring_free(r);
}

roaring64_bitmap_t *roaring64_bitmap_copy(const roaring64_bitmap_t *r) {
    roaring64_bitmap_t *result = roaring64_bitmap_create();

    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    while (it.value != NULL) {
        leaf_t *leaf = (leaf_t *)it.value;
        uint8_t result_typecode = leaf->typecode;
        container_t *result_container = get_copy_of_container(
            leaf->container, &result_typecode, /*copy_on_write=*/false);
        leaf_t *result_leaf = create_leaf(result_container, result_typecode);
        art_insert(&result->art, it.key, (art_val_t *)result_leaf);
        art_iterator_next(&it);
    }
    return result;
}

/**
 * Steal the containers from a 32-bit bitmap and insert them into a 64-bit
 * bitmap (with an offset)
 *
 * After calling this function, the original bitmap will be empty, and the
 * returned bitmap will contain all the values from the original bitmap.
 */
static void move_from_roaring32_offset(roaring64_bitmap_t *dst,
                                       roaring_bitmap_t *src,
                                       uint32_t high_bits) {
    uint64_t key_base = ((uint64_t)high_bits) << 32;
    uint32_t r32_size = ra_get_size(&src->high_low_container);
    for (uint32_t i = 0; i < r32_size; ++i) {
        uint16_t key = ra_get_key_at_index(&src->high_low_container, i);
        uint8_t typecode;
        container_t *container = ra_get_container_at_index(
            &src->high_low_container, (uint16_t)i, &typecode);

        uint8_t high48[ART_KEY_BYTES];
        uint64_t high48_bits = key_base | ((uint64_t)key << 16);
        split_key(high48_bits, high48);
        leaf_t *leaf = create_leaf(container, typecode);
        art_insert(&dst->art, high48, (art_val_t *)leaf);
    }
    // We stole all the containers, so leave behind a size of zero
    src->high_low_container.size = 0;
}

roaring64_bitmap_t *roaring64_bitmap_move_from_roaring32(
    roaring_bitmap_t *bitmap32) {
    roaring64_bitmap_t *result = roaring64_bitmap_create();

    move_from_roaring32_offset(result, bitmap32, 0);

    return result;
}

roaring64_bitmap_t *roaring64_bitmap_from_range(uint64_t min, uint64_t max,
                                                uint64_t step) {
    if (step == 0 || max <= min) {
        return NULL;
    }
    roaring64_bitmap_t *r = roaring64_bitmap_create();
    if (step >= (1 << 16)) {
        // Only one value per container.
        for (uint64_t value = min; value < max; value += step) {
            roaring64_bitmap_add(r, value);
            if (value > UINT64_MAX - step) {
                break;
            }
        }
        return r;
    }
    do {
        uint64_t high_bits = min & 0xFFFFFFFFFFFF0000;
        uint16_t container_min = min & 0xFFFF;
        uint32_t container_max = (uint32_t)minimum(max - high_bits, 1 << 16);

        uint8_t typecode;
        container_t *container = container_from_range(
            &typecode, container_min, container_max, (uint16_t)step);

        uint8_t high48[ART_KEY_BYTES];
        split_key(min, high48);
        leaf_t *leaf = create_leaf(container, typecode);
        art_insert(&r->art, high48, (art_val_t *)leaf);

        uint64_t gap = container_max - container_min + step - 1;
        uint64_t increment = gap - (gap % step);
        if (min > UINT64_MAX - increment) {
            break;
        }
        min += increment;
    } while (min < max);
    return r;
}

roaring64_bitmap_t *roaring64_bitmap_of_ptr(size_t n_args,
                                            const uint64_t *vals) {
    roaring64_bitmap_t *r = roaring64_bitmap_create();
    roaring64_bitmap_add_many(r, n_args, vals);
    return r;
}

static inline leaf_t *containerptr_roaring64_bitmap_add(roaring64_bitmap_t *r,
                                                        uint8_t *high48,
                                                        uint16_t low16,
                                                        leaf_t *leaf) {
    if (leaf != NULL) {
        uint8_t typecode2;
        container_t *container2 =
            container_add(leaf->container, low16, leaf->typecode, &typecode2);
        if (container2 != leaf->container) {
            container_free(leaf->container, leaf->typecode);
            leaf->container = container2;
            leaf->typecode = typecode2;
        }
        return leaf;
    } else {
        array_container_t *ac = array_container_create();
        uint8_t typecode;
        container_t *container =
            container_add(ac, low16, ARRAY_CONTAINER_TYPE, &typecode);
        assert(ac == container);
        leaf = create_leaf(container, typecode);
        art_insert(&r->art, high48, (art_val_t *)leaf);
        return leaf;
    }
}

void roaring64_bitmap_add(roaring64_bitmap_t *r, uint64_t val) {
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);
    leaf_t *leaf = (leaf_t *)art_find(&r->art, high48);
    containerptr_roaring64_bitmap_add(r, high48, low16, leaf);
}

bool roaring64_bitmap_add_checked(roaring64_bitmap_t *r, uint64_t val) {
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);
    leaf_t *leaf = (leaf_t *)art_find(&r->art, high48);

    int old_cardinality = 0;
    if (leaf != NULL) {
        old_cardinality =
            container_get_cardinality(leaf->container, leaf->typecode);
    }
    leaf = containerptr_roaring64_bitmap_add(r, high48, low16, leaf);
    int new_cardinality =
        container_get_cardinality(leaf->container, leaf->typecode);
    return old_cardinality != new_cardinality;
}

void roaring64_bitmap_add_bulk(roaring64_bitmap_t *r,
                               roaring64_bulk_context_t *context,
                               uint64_t val) {
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);
    if (context->leaf != NULL &&
        compare_high48(context->high_bytes, high48) == 0) {
        // We're at a container with the correct high bits.
        uint8_t typecode2;
        container_t *container2 =
            container_add(context->leaf->container, low16,
                          context->leaf->typecode, &typecode2);
        if (container2 != context->leaf->container) {
            container_free(context->leaf->container, context->leaf->typecode);
            context->leaf->container = container2;
            context->leaf->typecode = typecode2;
        }
    } else {
        // We're not positioned anywhere yet or the high bits of the key
        // differ.
        leaf_t *leaf = (leaf_t *)art_find(&r->art, high48);
        context->leaf =
            containerptr_roaring64_bitmap_add(r, high48, low16, leaf);
        memcpy(context->high_bytes, high48, ART_KEY_BYTES);
    }
}

void roaring64_bitmap_add_many(roaring64_bitmap_t *r, size_t n_args,
                               const uint64_t *vals) {
    if (n_args == 0) {
        return;
    }
    const uint64_t *end = vals + n_args;
    roaring64_bulk_context_t context = CROARING_ZERO_INITIALIZER;
    for (const uint64_t *current_val = vals; current_val != end;
         current_val++) {
        roaring64_bitmap_add_bulk(r, &context, *current_val);
    }
}

static inline void add_range_closed_at(art_t *art, uint8_t *high48,
                                       uint16_t min, uint16_t max) {
    leaf_t *leaf = (leaf_t *)art_find(art, high48);
    if (leaf != NULL) {
        uint8_t typecode2;
        container_t *container2 = container_add_range(
            leaf->container, leaf->typecode, min, max, &typecode2);
        if (container2 != leaf->container) {
            container_free(leaf->container, leaf->typecode);
            leaf->container = container2;
            leaf->typecode = typecode2;
        }
        return;
    }
    uint8_t typecode;
    // container_add_range is inclusive, but `container_range_of_ones` is
    // exclusive.
    container_t *container = container_range_of_ones(min, max + 1, &typecode);
    leaf = create_leaf(container, typecode);
    art_insert(art, high48, (art_val_t *)leaf);
}

void roaring64_bitmap_add_range(roaring64_bitmap_t *r, uint64_t min,
                                uint64_t max) {
    if (min >= max) {
        return;
    }
    roaring64_bitmap_add_range_closed(r, min, max - 1);
}

void roaring64_bitmap_add_range_closed(roaring64_bitmap_t *r, uint64_t min,
                                       uint64_t max) {
    if (min > max) {
        return;
    }

    art_t *art = &r->art;
    uint8_t min_high48[ART_KEY_BYTES];
    uint16_t min_low16 = split_key(min, min_high48);
    uint8_t max_high48[ART_KEY_BYTES];
    uint16_t max_low16 = split_key(max, max_high48);
    if (compare_high48(min_high48, max_high48) == 0) {
        // Only populate range within one container.
        add_range_closed_at(art, min_high48, min_low16, max_low16);
        return;
    }

    // Populate a range across containers. Fill intermediate containers
    // entirely.
    add_range_closed_at(art, min_high48, min_low16, 0xffff);
    uint64_t min_high_bits = min >> 16;
    uint64_t max_high_bits = max >> 16;
    for (uint64_t current = min_high_bits + 1; current < max_high_bits;
         ++current) {
        uint8_t current_high48[ART_KEY_BYTES];
        split_key(current << 16, current_high48);
        add_range_closed_at(art, current_high48, 0, 0xffff);
    }
    add_range_closed_at(art, max_high48, 0, max_low16);
}

bool roaring64_bitmap_contains(const roaring64_bitmap_t *r, uint64_t val) {
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);
    leaf_t *leaf = (leaf_t *)art_find(&r->art, high48);
    if (leaf != NULL) {
        return container_contains(leaf->container, low16, leaf->typecode);
    }
    return false;
}

bool roaring64_bitmap_contains_range(const roaring64_bitmap_t *r, uint64_t min,
                                     uint64_t max) {
    if (min >= max) {
        return true;
    }

    uint8_t min_high48[ART_KEY_BYTES];
    uint16_t min_low16 = split_key(min, min_high48);
    uint8_t max_high48[ART_KEY_BYTES];
    uint16_t max_low16 = split_key(max, max_high48);
    uint64_t max_high48_bits = (max - 1) & 0xFFFFFFFFFFFF0000;  // Inclusive

    art_iterator_t it = art_lower_bound(&r->art, min_high48);
    if (it.value == NULL || combine_key(it.key, 0) > min) {
        return false;
    }
    uint64_t prev_high48_bits = min & 0xFFFFFFFFFFFF0000;
    while (it.value != NULL) {
        uint64_t current_high48_bits = combine_key(it.key, 0);
        if (current_high48_bits > max_high48_bits) {
            // We've passed the end of the range with all containers containing
            // the range.
            return true;
        }
        if (current_high48_bits - prev_high48_bits > 0x10000) {
            // There is a gap in the iterator that falls in the range.
            return false;
        }

        leaf_t *leaf = (leaf_t *)it.value;
        uint32_t container_min = 0;
        if (compare_high48(it.key, min_high48) == 0) {
            container_min = min_low16;
        }
        uint32_t container_max = 0xFFFF + 1;  // Exclusive
        if (compare_high48(it.key, max_high48) == 0) {
            container_max = max_low16;
        }

        // For the first and last containers we use container_contains_range,
        // for the intermediate containers we can use container_is_full.
        if (container_min == 0 && container_max == 0xFFFF + 1) {
            if (!container_is_full(leaf->container, leaf->typecode)) {
                return false;
            }
        } else if (!container_contains_range(leaf->container, container_min,
                                             container_max, leaf->typecode)) {
            return false;
        }
        prev_high48_bits = current_high48_bits;
        art_iterator_next(&it);
    }
    return prev_high48_bits == max_high48_bits;
}

bool roaring64_bitmap_contains_bulk(const roaring64_bitmap_t *r,
                                    roaring64_bulk_context_t *context,
                                    uint64_t val) {
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);

    if (context->leaf == NULL ||
        art_compare_keys(context->high_bytes, high48) != 0) {
        // We're not positioned anywhere yet or the high bits of the key
        // differ.
        leaf_t *leaf = (leaf_t *)art_find(&r->art, high48);
        if (leaf == NULL) {
            return false;
        }
        context->leaf = leaf;
        memcpy(context->high_bytes, high48, ART_KEY_BYTES);
    }
    return container_contains(context->leaf->container, low16,
                              context->leaf->typecode);
}

bool roaring64_bitmap_select(const roaring64_bitmap_t *r, uint64_t rank,
                             uint64_t *element) {
    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    uint64_t start_rank = 0;
    while (it.value != NULL) {
        leaf_t *leaf = (leaf_t *)it.value;
        uint64_t cardinality =
            container_get_cardinality(leaf->container, leaf->typecode);
        if (start_rank + cardinality > rank) {
            uint32_t uint32_start = 0;
            uint32_t uint32_rank = rank - start_rank;
            uint32_t uint32_element = 0;
            if (container_select(leaf->container, leaf->typecode, &uint32_start,
                                 uint32_rank, &uint32_element)) {
                *element = combine_key(it.key, (uint16_t)uint32_element);
                return true;
            }
            return false;
        }
        start_rank += cardinality;
        art_iterator_next(&it);
    }
    return false;
}

uint64_t roaring64_bitmap_rank(const roaring64_bitmap_t *r, uint64_t val) {
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);

    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    uint64_t rank = 0;
    while (it.value != NULL) {
        leaf_t *leaf = (leaf_t *)it.value;
        int compare_result = compare_high48(it.key, high48);
        if (compare_result < 0) {
            rank += container_get_cardinality(leaf->container, leaf->typecode);
        } else if (compare_result == 0) {
            return rank +
                   container_rank(leaf->container, leaf->typecode, low16);
        } else {
            return rank;
        }
        art_iterator_next(&it);
    }
    return rank;
}

bool roaring64_bitmap_get_index(const roaring64_bitmap_t *r, uint64_t val,
                                uint64_t *out_index) {
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);

    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    uint64_t index = 0;
    while (it.value != NULL) {
        leaf_t *leaf = (leaf_t *)it.value;
        int compare_result = compare_high48(it.key, high48);
        if (compare_result < 0) {
            index += container_get_cardinality(leaf->container, leaf->typecode);
        } else if (compare_result == 0) {
            int index16 =
                container_get_index(leaf->container, leaf->typecode, low16);
            if (index16 < 0) {
                return false;
            }
            *out_index = index + index16;
            return true;
        } else {
            return false;
        }
        art_iterator_next(&it);
    }
    return false;
}

static inline leaf_t *containerptr_roaring64_bitmap_remove(
    roaring64_bitmap_t *r, uint8_t *high48, uint16_t low16, leaf_t *leaf) {
    if (leaf == NULL) {
        return NULL;
    }

    container_t *container = leaf->container;
    uint8_t typecode = leaf->typecode;
    uint8_t typecode2;
    container_t *container2 =
        container_remove(container, low16, typecode, &typecode2);
    if (container2 != container) {
        container_free(container, typecode);
        leaf->container = container2;
        leaf->typecode = typecode2;
    }
    if (!container_nonzero_cardinality(container2, typecode2)) {
        container_free(container2, typecode2);
        leaf = (leaf_t *)art_erase(&r->art, high48);
        if (leaf != NULL) {
            free_leaf(leaf);
        }
        return NULL;
    }
    return leaf;
}

void roaring64_bitmap_remove(roaring64_bitmap_t *r, uint64_t val) {
    art_t *art = &r->art;
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);

    leaf_t *leaf = (leaf_t *)art_find(art, high48);
    containerptr_roaring64_bitmap_remove(r, high48, low16, leaf);
}

bool roaring64_bitmap_remove_checked(roaring64_bitmap_t *r, uint64_t val) {
    art_t *art = &r->art;
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);
    leaf_t *leaf = (leaf_t *)art_find(art, high48);

    if (leaf == NULL) {
        return false;
    }
    int old_cardinality =
        container_get_cardinality(leaf->container, leaf->typecode);
    leaf = containerptr_roaring64_bitmap_remove(r, high48, low16, leaf);
    if (leaf == NULL) {
        return true;
    }
    int new_cardinality =
        container_get_cardinality(leaf->container, leaf->typecode);
    return new_cardinality != old_cardinality;
}

void roaring64_bitmap_remove_bulk(roaring64_bitmap_t *r,
                                  roaring64_bulk_context_t *context,
                                  uint64_t val) {
    art_t *art = &r->art;
    uint8_t high48[ART_KEY_BYTES];
    uint16_t low16 = split_key(val, high48);
    if (context->leaf != NULL &&
        compare_high48(context->high_bytes, high48) == 0) {
        // We're at a container with the correct high bits.
        uint8_t typecode2;
        container_t *container2 =
            container_remove(context->leaf->container, low16,
                             context->leaf->typecode, &typecode2);
        if (container2 != context->leaf->container) {
            container_free(context->leaf->container, context->leaf->typecode);
            context->leaf->container = container2;
            context->leaf->typecode = typecode2;
        }
        if (!container_nonzero_cardinality(container2, typecode2)) {
            leaf_t *leaf = (leaf_t *)art_erase(art, high48);
            container_free(container2, typecode2);
            free_leaf(leaf);
        }
    } else {
        // We're not positioned anywhere yet or the high bits of the key
        // differ.
        leaf_t *leaf = (leaf_t *)art_find(art, high48);
        context->leaf =
            containerptr_roaring64_bitmap_remove(r, high48, low16, leaf);
        memcpy(context->high_bytes, high48, ART_KEY_BYTES);
    }
}

void roaring64_bitmap_remove_many(roaring64_bitmap_t *r, size_t n_args,
                                  const uint64_t *vals) {
    if (n_args == 0) {
        return;
    }
    const uint64_t *end = vals + n_args;
    roaring64_bulk_context_t context = CROARING_ZERO_INITIALIZER;
    for (const uint64_t *current_val = vals; current_val != end;
         current_val++) {
        roaring64_bitmap_remove_bulk(r, &context, *current_val);
    }
}

static inline void remove_range_closed_at(art_t *art, uint8_t *high48,
                                          uint16_t min, uint16_t max) {
    leaf_t *leaf = (leaf_t *)art_find(art, high48);
    if (leaf == NULL) {
        return;
    }
    uint8_t typecode2;
    container_t *container2 = container_remove_range(
        leaf->container, leaf->typecode, min, max, &typecode2);
    if (container2 != leaf->container) {
        container_free(leaf->container, leaf->typecode);
        if (container2 != NULL) {
            leaf->container = container2;
            leaf->typecode = typecode2;
        } else {
            art_erase(art, high48);
            free_leaf(leaf);
        }
    }
}

void roaring64_bitmap_remove_range(roaring64_bitmap_t *r, uint64_t min,
                                   uint64_t max) {
    if (min >= max) {
        return;
    }
    roaring64_bitmap_remove_range_closed(r, min, max - 1);
}

void roaring64_bitmap_remove_range_closed(roaring64_bitmap_t *r, uint64_t min,
                                          uint64_t max) {
    if (min > max) {
        return;
    }

    art_t *art = &r->art;
    uint8_t min_high48[ART_KEY_BYTES];
    uint16_t min_low16 = split_key(min, min_high48);
    uint8_t max_high48[ART_KEY_BYTES];
    uint16_t max_low16 = split_key(max, max_high48);
    if (compare_high48(min_high48, max_high48) == 0) {
        // Only remove a range within one container.
        remove_range_closed_at(art, min_high48, min_low16, max_low16);
        return;
    }

    // Remove a range across containers. Remove intermediate containers
    // entirely.
    remove_range_closed_at(art, min_high48, min_low16, 0xffff);

    art_iterator_t it = art_upper_bound(art, min_high48);
    while (it.value != NULL && art_compare_keys(it.key, max_high48) < 0) {
        leaf_t *leaf = (leaf_t *)art_iterator_erase(art, &it);
        container_free(leaf->container, leaf->typecode);
        free_leaf(leaf);
    }
    remove_range_closed_at(art, max_high48, 0, max_low16);
}

void roaring64_bitmap_clear(roaring64_bitmap_t *r) {
    roaring64_bitmap_remove_range_closed(r, 0, UINT64_MAX);
}

uint64_t roaring64_bitmap_get_cardinality(const roaring64_bitmap_t *r) {
    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    uint64_t cardinality = 0;
    while (it.value != NULL) {
        leaf_t *leaf = (leaf_t *)it.value;
        cardinality +=
            container_get_cardinality(leaf->container, leaf->typecode);
        art_iterator_next(&it);
    }
    return cardinality;
}

uint64_t roaring64_bitmap_range_cardinality(const roaring64_bitmap_t *r,
                                            uint64_t min, uint64_t max) {
    if (min >= max) {
        return 0;
    }
    // Convert to a closed range
    // No underflow here: passing the above condition implies min < max, so
    // there is a number less than max
    return roaring64_bitmap_range_closed_cardinality(r, min, max - 1);
}

uint64_t roaring64_bitmap_range_closed_cardinality(const roaring64_bitmap_t *r,
                                                   uint64_t min, uint64_t max) {
    if (min > max) {
        return 0;
    }

    uint64_t cardinality = 0;
    uint8_t min_high48[ART_KEY_BYTES];
    uint16_t min_low16 = split_key(min, min_high48);
    uint8_t max_high48[ART_KEY_BYTES];
    uint16_t max_low16 = split_key(max, max_high48);

    art_iterator_t it = art_lower_bound(&r->art, min_high48);
    while (it.value != NULL) {
        int max_compare_result = compare_high48(it.key, max_high48);
        if (max_compare_result > 0) {
            // We're outside the range.
            break;
        }

        leaf_t *leaf = (leaf_t *)it.value;
        if (max_compare_result == 0) {
            // We're at the max high key, add only the range up to the low
            // 16 bits of max.
            cardinality +=
                container_rank(leaf->container, leaf->typecode, max_low16);
        } else {
            // We're not yet at the max high key, add the full container
            // range.
            cardinality +=
                container_get_cardinality(leaf->container, leaf->typecode);
        }
        if (compare_high48(it.key, min_high48) == 0 && min_low16 > 0) {
            // We're at the min high key, remove the range up to the low 16
            // bits of min.
            cardinality -=
                container_rank(leaf->container, leaf->typecode, min_low16 - 1);
        }
        art_iterator_next(&it);
    }
    return cardinality;
}

bool roaring64_bitmap_is_empty(const roaring64_bitmap_t *r) {
    return art_is_empty(&r->art);
}

uint64_t roaring64_bitmap_minimum(const roaring64_bitmap_t *r) {
    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    if (it.value == NULL) {
        return UINT64_MAX;
    }
    leaf_t *leaf = (leaf_t *)it.value;
    return combine_key(it.key,
                       container_minimum(leaf->container, leaf->typecode));
}

uint64_t roaring64_bitmap_maximum(const roaring64_bitmap_t *r) {
    art_iterator_t it = art_init_iterator(&r->art, /*first=*/false);
    if (it.value == NULL) {
        return 0;
    }
    leaf_t *leaf = (leaf_t *)it.value;
    return combine_key(it.key,
                       container_maximum(leaf->container, leaf->typecode));
}

bool roaring64_bitmap_run_optimize(roaring64_bitmap_t *r) {
    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    bool has_run_container = false;
    while (it.value != NULL) {
        leaf_t *leaf = (leaf_t *)it.value;
        uint8_t new_typecode;
        // We don't need to free the existing container if a new one was
        // created, convert_run_optimize does that internally.
        leaf->container = convert_run_optimize(leaf->container, leaf->typecode,
                                               &new_typecode);
        leaf->typecode = new_typecode;
        has_run_container |= new_typecode == RUN_CONTAINER_TYPE;
        art_iterator_next(&it);
    }
    return has_run_container;
}

/**
 *  (For advanced users.)
 * Collect statistics about the bitmap
 */
void roaring64_bitmap_statistics(const roaring64_bitmap_t *r,
                                 roaring64_statistics_t *stat) {
    memset(stat, 0, sizeof(*stat));
    stat->min_value = roaring64_bitmap_minimum(r);
    stat->max_value = roaring64_bitmap_maximum(r);

    art_iterator_t it = art_init_iterator(&r->art, true);
    while (it.value != NULL) {
        leaf_t *leaf = (leaf_t *)it.value;
        stat->n_containers++;
        uint8_t truetype = get_container_type(leaf->container, leaf->typecode);
        uint32_t card =
            container_get_cardinality(leaf->container, leaf->typecode);
        uint32_t sbytes =
            container_size_in_bytes(leaf->container, leaf->typecode);
        stat->cardinality += card;
        switch (truetype) {
            case BITSET_CONTAINER_TYPE:
                stat->n_bitset_containers++;
                stat->n_values_bitset_containers += card;
                stat->n_bytes_bitset_containers += sbytes;
                break;
            case ARRAY_CONTAINER_TYPE:
                stat->n_array_containers++;
                stat->n_values_array_containers += card;
                stat->n_bytes_array_containers += sbytes;
                break;
            case RUN_CONTAINER_TYPE:
                stat->n_run_containers++;
                stat->n_values_run_containers += card;
                stat->n_bytes_run_containers += sbytes;
                break;
            default:
                assert(false);
                roaring_unreachable;
        }
        art_iterator_next(&it);
    }
}

static bool roaring64_leaf_internal_validate(const art_val_t *val,
                                             const char **reason) {
    leaf_t *leaf = (leaf_t *)val;
    return container_internal_validate(leaf->container, leaf->typecode, reason);
}

bool roaring64_bitmap_internal_validate(const roaring64_bitmap_t *r,
                                        const char **reason) {
    return art_internal_validate(&r->art, reason,
                                 roaring64_leaf_internal_validate);
}

bool roaring64_bitmap_equals(const roaring64_bitmap_t *r1,
                             const roaring64_bitmap_t *r2) {
    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL && it2.value != NULL) {
        if (compare_high48(it1.key, it2.key) != 0) {
            return false;
        }
        leaf_t *leaf1 = (leaf_t *)it1.value;
        leaf_t *leaf2 = (leaf_t *)it2.value;
        if (!container_equals(leaf1->container, leaf1->typecode,
                              leaf2->container, leaf2->typecode)) {
            return false;
        }
        art_iterator_next(&it1);
        art_iterator_next(&it2);
    }
    return it1.value == NULL && it2.value == NULL;
}

bool roaring64_bitmap_is_subset(const roaring64_bitmap_t *r1,
                                const roaring64_bitmap_t *r2) {
    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL) {
        bool it2_present = it2.value != NULL;

        int compare_result = 0;
        if (it2_present) {
            compare_result = compare_high48(it1.key, it2.key);
            if (compare_result == 0) {
                leaf_t *leaf1 = (leaf_t *)it1.value;
                leaf_t *leaf2 = (leaf_t *)it2.value;
                if (!container_is_subset(leaf1->container, leaf1->typecode,
                                         leaf2->container, leaf2->typecode)) {
                    return false;
                }
                art_iterator_next(&it1);
                art_iterator_next(&it2);
            }
        }
        if (!it2_present || compare_result < 0) {
            return false;
        } else if (compare_result > 0) {
            art_iterator_lower_bound(&it2, it1.key);
        }
    }
    return true;
}

bool roaring64_bitmap_is_strict_subset(const roaring64_bitmap_t *r1,
                                       const roaring64_bitmap_t *r2) {
    return roaring64_bitmap_get_cardinality(r1) <
               roaring64_bitmap_get_cardinality(r2) &&
           roaring64_bitmap_is_subset(r1, r2);
}

roaring64_bitmap_t *roaring64_bitmap_and(const roaring64_bitmap_t *r1,
                                         const roaring64_bitmap_t *r2) {
    roaring64_bitmap_t *result = roaring64_bitmap_create();

    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL && it2.value != NULL) {
        // Cases:
        // 1. it1 <  it2 -> it1++
        // 2. it1 == it1 -> output it1 & it2, it1++, it2++
        // 3. it1 >  it2 -> it2++
        int compare_result = compare_high48(it1.key, it2.key);
        if (compare_result == 0) {
            // Case 2: iterators at the same high key position.
            leaf_t *result_leaf = (leaf_t *)roaring_malloc(sizeof(leaf_t));
            leaf_t *leaf1 = (leaf_t *)it1.value;
            leaf_t *leaf2 = (leaf_t *)it2.value;
            result_leaf->container = container_and(
                leaf1->container, leaf1->typecode, leaf2->container,
                leaf2->typecode, &result_leaf->typecode);

            if (container_nonzero_cardinality(result_leaf->container,
                                              result_leaf->typecode)) {
                art_insert(&result->art, it1.key, (art_val_t *)result_leaf);
            } else {
                container_free(result_leaf->container, result_leaf->typecode);
                free_leaf(result_leaf);
            }
            art_iterator_next(&it1);
            art_iterator_next(&it2);
        } else if (compare_result < 0) {
            // Case 1: it1 is before it2.
            art_iterator_lower_bound(&it1, it2.key);
        } else {
            // Case 3: it2 is before it1.
            art_iterator_lower_bound(&it2, it1.key);
        }
    }
    return result;
}

uint64_t roaring64_bitmap_and_cardinality(const roaring64_bitmap_t *r1,
                                          const roaring64_bitmap_t *r2) {
    uint64_t result = 0;

    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL && it2.value != NULL) {
        // Cases:
        // 1. it1 <  it2 -> it1++
        // 2. it1 == it1 -> output cardinaltiy it1 & it2, it1++, it2++
        // 3. it1 >  it2 -> it2++
        int compare_result = compare_high48(it1.key, it2.key);
        if (compare_result == 0) {
            // Case 2: iterators at the same high key position.
            leaf_t *leaf1 = (leaf_t *)it1.value;
            leaf_t *leaf2 = (leaf_t *)it2.value;
            result +=
                container_and_cardinality(leaf1->container, leaf1->typecode,
                                          leaf2->container, leaf2->typecode);
            art_iterator_next(&it1);
            art_iterator_next(&it2);
        } else if (compare_result < 0) {
            // Case 1: it1 is before it2.
            art_iterator_lower_bound(&it1, it2.key);
        } else {
            // Case 3: it2 is before it1.
            art_iterator_lower_bound(&it2, it1.key);
        }
    }
    return result;
}

// Inplace and (modifies its first argument).
void roaring64_bitmap_and_inplace(roaring64_bitmap_t *r1,
                                  const roaring64_bitmap_t *r2) {
    if (r1 == r2) {
        return;
    }
    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL) {
        // Cases:
        // 1. !it2_present -> erase it1
        // 2. it2_present
        //    a. it1 <  it2 -> erase it1
        //    b. it1 == it2 -> output it1 & it2, it1++, it2++
        //    c. it1 >  it2 -> it2++
        bool it2_present = it2.value != NULL;
        int compare_result = 0;
        if (it2_present) {
            compare_result = compare_high48(it1.key, it2.key);
            if (compare_result == 0) {
                // Case 2a: iterators at the same high key position.
                leaf_t *leaf1 = (leaf_t *)it1.value;
                leaf_t *leaf2 = (leaf_t *)it2.value;

                // We do the computation "in place" only when c1 is not a
                // shared container. Rationale: using a shared container
                // safely with in place computation would require making a
                // copy and then doing the computation in place which is
                // likely less efficient than avoiding in place entirely and
                // always generating a new container.
                uint8_t typecode2;
                container_t *container2;
                if (leaf1->typecode == SHARED_CONTAINER_TYPE) {
                    container2 = container_and(
                        leaf1->container, leaf1->typecode, leaf2->container,
                        leaf2->typecode, &typecode2);
                } else {
                    container2 = container_iand(
                        leaf1->container, leaf1->typecode, leaf2->container,
                        leaf2->typecode, &typecode2);
                }

                if (container2 != leaf1->container) {
                    container_free(leaf1->container, leaf1->typecode);
                    leaf1->container = container2;
                    leaf1->typecode = typecode2;
                }
                if (!container_nonzero_cardinality(container2, typecode2)) {
                    container_free(container2, typecode2);
                    art_iterator_erase(&r1->art, &it1);
                    free_leaf(leaf1);
                } else {
                    // Only advance the iterator if we didn't delete the
                    // leaf, as erasing advances by itself.
                    art_iterator_next(&it1);
                }
                art_iterator_next(&it2);
            }
        }

        if (!it2_present || compare_result < 0) {
            // Cases 1 and 3a: it1 is the only iterator or is before it2.
            leaf_t *leaf = (leaf_t *)art_iterator_erase(&r1->art, &it1);
            assert(leaf != NULL);
            container_free(leaf->container, leaf->typecode);
            free_leaf(leaf);
        } else if (compare_result > 0) {
            // Case 2c: it1 is after it2.
            art_iterator_lower_bound(&it2, it1.key);
        }
    }
}

bool roaring64_bitmap_intersect(const roaring64_bitmap_t *r1,
                                const roaring64_bitmap_t *r2) {
    bool intersect = false;
    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL && it2.value != NULL) {
        // Cases:
        // 1. it1 <  it2 -> it1++
        // 2. it1 == it1 -> intersect |= it1 & it2, it1++, it2++
        // 3. it1 >  it2 -> it2++
        int compare_result = compare_high48(it1.key, it2.key);
        if (compare_result == 0) {
            // Case 2: iterators at the same high key position.
            leaf_t *leaf1 = (leaf_t *)it1.value;
            leaf_t *leaf2 = (leaf_t *)it2.value;
            intersect |= container_intersect(leaf1->container, leaf1->typecode,
                                             leaf2->container, leaf2->typecode);
            art_iterator_next(&it1);
            art_iterator_next(&it2);
        } else if (compare_result < 0) {
            // Case 1: it1 is before it2.
            art_iterator_lower_bound(&it1, it2.key);
        } else {
            // Case 3: it2 is before it1.
            art_iterator_lower_bound(&it2, it1.key);
        }
    }
    return intersect;
}

bool roaring64_bitmap_intersect_with_range(const roaring64_bitmap_t *r,
                                           uint64_t min, uint64_t max) {
    if (min >= max) {
        return false;
    }
    roaring64_iterator_t it;
    roaring64_iterator_init_at(r, &it, /*first=*/true);
    if (!roaring64_iterator_move_equalorlarger(&it, min)) {
        return false;
    }
    return roaring64_iterator_has_value(&it) &&
           roaring64_iterator_value(&it) < max;
}

double roaring64_bitmap_jaccard_index(const roaring64_bitmap_t *r1,
                                      const roaring64_bitmap_t *r2) {
    uint64_t c1 = roaring64_bitmap_get_cardinality(r1);
    uint64_t c2 = roaring64_bitmap_get_cardinality(r2);
    uint64_t inter = roaring64_bitmap_and_cardinality(r1, r2);
    return (double)inter / (double)(c1 + c2 - inter);
}

roaring64_bitmap_t *roaring64_bitmap_or(const roaring64_bitmap_t *r1,
                                        const roaring64_bitmap_t *r2) {
    roaring64_bitmap_t *result = roaring64_bitmap_create();

    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL || it2.value != NULL) {
        bool it1_present = it1.value != NULL;
        bool it2_present = it2.value != NULL;

        // Cases:
        // 1. it1_present  && !it2_present -> output it1, it1++
        // 2. !it1_present && it2_present  -> output it2, it2++
        // 3. it1_present  && it2_present
        //    a. it1 <  it2 -> output it1, it1++
        //    b. it1 == it2 -> output it1 | it2, it1++, it2++
        //    c. it1 >  it2 -> output it2, it2++
        int compare_result = 0;
        if (it1_present && it2_present) {
            compare_result = compare_high48(it1.key, it2.key);
            if (compare_result == 0) {
                // Case 3b: iterators at the same high key position.
                leaf_t *leaf1 = (leaf_t *)it1.value;
                leaf_t *leaf2 = (leaf_t *)it2.value;
                leaf_t *result_leaf = (leaf_t *)roaring_malloc(sizeof(leaf_t));
                result_leaf->container = container_or(
                    leaf1->container, leaf1->typecode, leaf2->container,
                    leaf2->typecode, &result_leaf->typecode);
                art_insert(&result->art, it1.key, (art_val_t *)result_leaf);
                art_iterator_next(&it1);
                art_iterator_next(&it2);
            }
        }
        if ((it1_present && !it2_present) || compare_result < 0) {
            // Cases 1 and 3a: it1 is the only iterator or is before it2.
            leaf_t *result_leaf = copy_leaf_container((leaf_t *)it1.value);
            art_insert(&result->art, it1.key, (art_val_t *)result_leaf);
            art_iterator_next(&it1);
        } else if ((!it1_present && it2_present) || compare_result > 0) {
            // Cases 2 and 3c: it2 is the only iterator or is before it1.
            leaf_t *result_leaf = copy_leaf_container((leaf_t *)it2.value);
            art_insert(&result->art, it2.key, (art_val_t *)result_leaf);
            art_iterator_next(&it2);
        }
    }
    return result;
}

uint64_t roaring64_bitmap_or_cardinality(const roaring64_bitmap_t *r1,
                                         const roaring64_bitmap_t *r2) {
    uint64_t c1 = roaring64_bitmap_get_cardinality(r1);
    uint64_t c2 = roaring64_bitmap_get_cardinality(r2);
    uint64_t inter = roaring64_bitmap_and_cardinality(r1, r2);
    return c1 + c2 - inter;
}

void roaring64_bitmap_or_inplace(roaring64_bitmap_t *r1,
                                 const roaring64_bitmap_t *r2) {
    if (r1 == r2) {
        return;
    }
    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL || it2.value != NULL) {
        bool it1_present = it1.value != NULL;
        bool it2_present = it2.value != NULL;

        // Cases:
        // 1. it1_present  && !it2_present -> it1++
        // 2. !it1_present && it2_present  -> add it2, it2++
        // 3. it1_present  && it2_present
        //    a. it1 <  it2 -> it1++
        //    b. it1 == it2 -> it1 | it2, it1++, it2++
        //    c. it1 >  it2 -> add it2, it2++
        int compare_result = 0;
        if (it1_present && it2_present) {
            compare_result = compare_high48(it1.key, it2.key);
            if (compare_result == 0) {
                // Case 3b: iterators at the same high key position.
                leaf_t *leaf1 = (leaf_t *)it1.value;
                leaf_t *leaf2 = (leaf_t *)it2.value;
                uint8_t typecode2;
                container_t *container2;
                if (leaf1->typecode == SHARED_CONTAINER_TYPE) {
                    container2 = container_or(leaf1->container, leaf1->typecode,
                                              leaf2->container, leaf2->typecode,
                                              &typecode2);
                } else {
                    container2 = container_ior(
                        leaf1->container, leaf1->typecode, leaf2->container,
                        leaf2->typecode, &typecode2);
                }
                if (container2 != leaf1->container) {
                    container_free(leaf1->container, leaf1->typecode);
                    leaf1->container = container2;
                    leaf1->typecode = typecode2;
                }
                art_iterator_next(&it1);
                art_iterator_next(&it2);
            }
        }
        if ((it1_present && !it2_present) || compare_result < 0) {
            // Cases 1 and 3a: it1 is the only iterator or is before it2.
            art_iterator_next(&it1);
        } else if ((!it1_present && it2_present) || compare_result > 0) {
            // Cases 2 and 3c: it2 is the only iterator or is before it1.
            leaf_t *result_leaf = copy_leaf_container((leaf_t *)it2.value);
            art_iterator_insert(&r1->art, &it1, it2.key,
                                (art_val_t *)result_leaf);
            art_iterator_next(&it2);
        }
    }
}

roaring64_bitmap_t *roaring64_bitmap_xor(const roaring64_bitmap_t *r1,
                                         const roaring64_bitmap_t *r2) {
    roaring64_bitmap_t *result = roaring64_bitmap_create();

    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL || it2.value != NULL) {
        bool it1_present = it1.value != NULL;
        bool it2_present = it2.value != NULL;

        // Cases:
        // 1. it1_present  && !it2_present -> output it1, it1++
        // 2. !it1_present && it2_present  -> output it2, it2++
        // 3. it1_present  && it2_present
        //    a. it1 <  it2 -> output it1, it1++
        //    b. it1 == it2 -> output it1 ^ it2, it1++, it2++
        //    c. it1 >  it2 -> output it2, it2++
        int compare_result = 0;
        if (it1_present && it2_present) {
            compare_result = compare_high48(it1.key, it2.key);
            if (compare_result == 0) {
                // Case 3b: iterators at the same high key position.
                leaf_t *leaf1 = (leaf_t *)it1.value;
                leaf_t *leaf2 = (leaf_t *)it2.value;
                leaf_t *result_leaf = (leaf_t *)roaring_malloc(sizeof(leaf_t));
                result_leaf->container = container_xor(
                    leaf1->container, leaf1->typecode, leaf2->container,
                    leaf2->typecode, &result_leaf->typecode);
                if (container_nonzero_cardinality(result_leaf->container,
                                                  result_leaf->typecode)) {
                    art_insert(&result->art, it1.key, (art_val_t *)result_leaf);
                } else {
                    container_free(result_leaf->container,
                                   result_leaf->typecode);
                    free_leaf(result_leaf);
                }
                art_iterator_next(&it1);
                art_iterator_next(&it2);
            }
        }
        if ((it1_present && !it2_present) || compare_result < 0) {
            // Cases 1 and 3a: it1 is the only iterator or is before it2.
            leaf_t *result_leaf = copy_leaf_container((leaf_t *)it1.value);
            art_insert(&result->art, it1.key, (art_val_t *)result_leaf);
            art_iterator_next(&it1);
        } else if ((!it1_present && it2_present) || compare_result > 0) {
            // Cases 2 and 3c: it2 is the only iterator or is before it1.
            leaf_t *result_leaf = copy_leaf_container((leaf_t *)it2.value);
            art_insert(&result->art, it2.key, (art_val_t *)result_leaf);
            art_iterator_next(&it2);
        }
    }
    return result;
}

uint64_t roaring64_bitmap_xor_cardinality(const roaring64_bitmap_t *r1,
                                          const roaring64_bitmap_t *r2) {
    uint64_t c1 = roaring64_bitmap_get_cardinality(r1);
    uint64_t c2 = roaring64_bitmap_get_cardinality(r2);
    uint64_t inter = roaring64_bitmap_and_cardinality(r1, r2);
    return c1 + c2 - 2 * inter;
}

void roaring64_bitmap_xor_inplace(roaring64_bitmap_t *r1,
                                  const roaring64_bitmap_t *r2) {
    assert(r1 != r2);
    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL || it2.value != NULL) {
        bool it1_present = it1.value != NULL;
        bool it2_present = it2.value != NULL;

        // Cases:
        // 1.  it1_present && !it2_present -> it1++
        // 2. !it1_present &&  it2_present -> add it2, it2++
        // 3.  it1_present &&  it2_present
        //    a. it1 <  it2 -> it1++
        //    b. it1 == it2 -> it1 ^ it2, it1++, it2++
        //    c. it1 >  it2 -> add it2, it2++
        int compare_result = 0;
        if (it1_present && it2_present) {
            compare_result = compare_high48(it1.key, it2.key);
            if (compare_result == 0) {
                // Case 3b: iterators at the same high key position.
                leaf_t *leaf1 = (leaf_t *)it1.value;
                leaf_t *leaf2 = (leaf_t *)it2.value;
                container_t *container1 = leaf1->container;
                uint8_t typecode1 = leaf1->typecode;
                uint8_t typecode2;
                container_t *container2;
                if (leaf1->typecode == SHARED_CONTAINER_TYPE) {
                    container2 = container_xor(
                        leaf1->container, leaf1->typecode, leaf2->container,
                        leaf2->typecode, &typecode2);
                    if (container2 != container1) {
                        // We only free when doing container_xor, not
                        // container_ixor, as ixor frees the original
                        // internally.
                        container_free(container1, typecode1);
                    }
                } else {
                    container2 = container_ixor(
                        leaf1->container, leaf1->typecode, leaf2->container,
                        leaf2->typecode, &typecode2);
                }
                leaf1->container = container2;
                leaf1->typecode = typecode2;

                if (!container_nonzero_cardinality(container2, typecode2)) {
                    container_free(container2, typecode2);
                    art_iterator_erase(&r1->art, &it1);
                    free_leaf(leaf1);
                } else {
                    // Only advance the iterator if we didn't delete the
                    // leaf, as erasing advances by itself.
                    art_iterator_next(&it1);
                }
                art_iterator_next(&it2);
            }
        }
        if ((it1_present && !it2_present) || compare_result < 0) {
            // Cases 1 and 3a: it1 is the only iterator or is before it2.
            art_iterator_next(&it1);
        } else if ((!it1_present && it2_present) || compare_result > 0) {
            // Cases 2 and 3c: it2 is the only iterator or is before it1.
            leaf_t *result_leaf = copy_leaf_container((leaf_t *)it2.value);
            if (it1_present) {
                art_iterator_insert(&r1->art, &it1, it2.key,
                                    (art_val_t *)result_leaf);
                art_iterator_next(&it1);
            } else {
                art_insert(&r1->art, it2.key, (art_val_t *)result_leaf);
            }
            art_iterator_next(&it2);
        }
    }
}

roaring64_bitmap_t *roaring64_bitmap_andnot(const roaring64_bitmap_t *r1,
                                            const roaring64_bitmap_t *r2) {
    roaring64_bitmap_t *result = roaring64_bitmap_create();

    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL) {
        // Cases:
        // 1. it1_present && !it2_present -> output it1, it1++
        // 2. it1_present && it2_present
        //    a. it1 <  it2 -> output it1, it1++
        //    b. it1 == it2 -> output it1 - it2, it1++, it2++
        //    c. it1 >  it2 -> it2++
        bool it2_present = it2.value != NULL;
        int compare_result = 0;
        if (it2_present) {
            compare_result = compare_high48(it1.key, it2.key);
            if (compare_result == 0) {
                // Case 2b: iterators at the same high key position.
                leaf_t *result_leaf = (leaf_t *)roaring_malloc(sizeof(leaf_t));
                leaf_t *leaf1 = (leaf_t *)it1.value;
                leaf_t *leaf2 = (leaf_t *)it2.value;
                result_leaf->container = container_andnot(
                    leaf1->container, leaf1->typecode, leaf2->container,
                    leaf2->typecode, &result_leaf->typecode);

                if (container_nonzero_cardinality(result_leaf->container,
                                                  result_leaf->typecode)) {
                    art_insert(&result->art, it1.key, (art_val_t *)result_leaf);
                } else {
                    container_free(result_leaf->container,
                                   result_leaf->typecode);
                    free_leaf(result_leaf);
                }
                art_iterator_next(&it1);
                art_iterator_next(&it2);
            }
        }
        if (!it2_present || compare_result < 0) {
            // Cases 1 and 2a: it1 is the only iterator or is before it2.
            leaf_t *result_leaf = copy_leaf_container((leaf_t *)it1.value);
            art_insert(&result->art, it1.key, (art_val_t *)result_leaf);
            art_iterator_next(&it1);
        } else if (compare_result > 0) {
            // Case 2c: it1 is after it2.
            art_iterator_next(&it2);
        }
    }
    return result;
}

uint64_t roaring64_bitmap_andnot_cardinality(const roaring64_bitmap_t *r1,
                                             const roaring64_bitmap_t *r2) {
    uint64_t c1 = roaring64_bitmap_get_cardinality(r1);
    uint64_t inter = roaring64_bitmap_and_cardinality(r1, r2);
    return c1 - inter;
}

void roaring64_bitmap_andnot_inplace(roaring64_bitmap_t *r1,
                                     const roaring64_bitmap_t *r2) {
    art_iterator_t it1 = art_init_iterator(&r1->art, /*first=*/true);
    art_iterator_t it2 = art_init_iterator(&r2->art, /*first=*/true);

    while (it1.value != NULL) {
        // Cases:
        // 1. it1_present && !it2_present -> it1++
        // 2. it1_present &&  it2_present
        //    a. it1 <  it2 -> it1++
        //    b. it1 == it2 -> it1 - it2, it1++, it2++
        //    c. it1 >  it2 -> it2++
        bool it2_present = it2.value != NULL;
        int compare_result = 0;
        if (it2_present) {
            compare_result = compare_high48(it1.key, it2.key);
            if (compare_result == 0) {
                // Case 2b: iterators at the same high key position.
                leaf_t *leaf1 = (leaf_t *)it1.value;
                leaf_t *leaf2 = (leaf_t *)it2.value;
                container_t *container1 = leaf1->container;
                uint8_t typecode1 = leaf1->typecode;
                uint8_t typecode2;
                container_t *container2;
                if (leaf1->typecode == SHARED_CONTAINER_TYPE) {
                    container2 = container_andnot(
                        leaf1->container, leaf1->typecode, leaf2->container,
                        leaf2->typecode, &typecode2);
                    if (container2 != container1) {
                        // We only free when doing container_andnot, not
                        // container_iandnot, as iandnot frees the original
                        // internally.
                        container_free(container1, typecode1);
                    }
                } else {
                    container2 = container_iandnot(
                        leaf1->container, leaf1->typecode, leaf2->container,
                        leaf2->typecode, &typecode2);
                }
                if (container2 != container1) {
                    leaf1->container = container2;
                    leaf1->typecode = typecode2;
                }

                if (!container_nonzero_cardinality(container2, typecode2)) {
                    container_free(container2, typecode2);
                    art_iterator_erase(&r1->art, &it1);
                    free_leaf(leaf1);
                } else {
                    // Only advance the iterator if we didn't delete the
                    // leaf, as erasing advances by itself.
                    art_iterator_next(&it1);
                }
                art_iterator_next(&it2);
            }
        }
        if (!it2_present || compare_result < 0) {
            // Cases 1 and 2a: it1 is the only iterator or is before it2.
            art_iterator_next(&it1);
        } else if (compare_result > 0) {
            // Case 2c: it1 is after it2.
            art_iterator_next(&it2);
        }
    }
}

/**
 * Flips the leaf at high48 in the range [min, max), returning a new leaf with a
 * new container. If the high48 key is not found in the existing bitmap, a new
 * container is created. Returns null if the negation results in an empty range.
 */
static leaf_t *roaring64_flip_leaf(const roaring64_bitmap_t *r,
                                   uint8_t high48[], uint32_t min,
                                   uint32_t max) {
    leaf_t *leaf1 = (leaf_t *)art_find(&r->art, high48);
    container_t *container2;
    uint8_t typecode2;
    if (leaf1 == NULL) {
        // No container at this key, create a full container.
        container2 = container_range_of_ones(min, max, &typecode2);
    } else if (min == 0 && max > 0xFFFF) {
        // Flip whole container.
        container2 =
            container_not(leaf1->container, leaf1->typecode, &typecode2);
    } else {
        // Partially flip a container.
        container2 = container_not_range(leaf1->container, leaf1->typecode, min,
                                         max, &typecode2);
    }
    if (container_nonzero_cardinality(container2, typecode2)) {
        return create_leaf(container2, typecode2);
    }
    container_free(container2, typecode2);
    return NULL;
}

/**
 * Flips the leaf at high48 in the range [min, max). If the high48 key is not
 * found in the bitmap, a new container is created. Deletes the leaf and
 * associated container if the negation results in an empty range.
 */
static void roaring64_flip_leaf_inplace(roaring64_bitmap_t *r, uint8_t high48[],
                                        uint32_t min, uint32_t max) {
    leaf_t *leaf = (leaf_t *)art_find(&r->art, high48);
    container_t *container2;
    uint8_t typecode2;
    if (leaf == NULL) {
        // No container at this key, insert a full container.
        container2 = container_range_of_ones(min, max, &typecode2);
        art_insert(&r->art, high48,
                   (art_val_t *)create_leaf(container2, typecode2));
        return;
    }

    if (min == 0 && max > 0xFFFF) {
        // Flip whole container.
        container2 =
            container_inot(leaf->container, leaf->typecode, &typecode2);
    } else {
        // Partially flip a container.
        container2 = container_inot_range(leaf->container, leaf->typecode, min,
                                          max, &typecode2);
    }

    leaf->container = container2;
    leaf->typecode = typecode2;

    if (!container_nonzero_cardinality(leaf->container, leaf->typecode)) {
        art_erase(&r->art, high48);
        container_free(leaf->container, leaf->typecode);
        free_leaf(leaf);
    }
}

roaring64_bitmap_t *roaring64_bitmap_flip(const roaring64_bitmap_t *r,
                                          uint64_t min, uint64_t max) {
    if (min >= max) {
        return roaring64_bitmap_copy(r);
    }
    return roaring64_bitmap_flip_closed(r, min, max - 1);
}

roaring64_bitmap_t *roaring64_bitmap_flip_closed(const roaring64_bitmap_t *r1,
                                                 uint64_t min, uint64_t max) {
    if (min > max) {
        return roaring64_bitmap_copy(r1);
    }
    uint8_t min_high48_key[ART_KEY_BYTES];
    uint16_t min_low16 = split_key(min, min_high48_key);
    uint8_t max_high48_key[ART_KEY_BYTES];
    uint16_t max_low16 = split_key(max, max_high48_key);
    uint64_t min_high48_bits = (min & 0xFFFFFFFFFFFF0000ULL) >> 16;
    uint64_t max_high48_bits = (max & 0xFFFFFFFFFFFF0000ULL) >> 16;

    roaring64_bitmap_t *r2 = roaring64_bitmap_create();
    art_iterator_t it = art_init_iterator(&r1->art, /*first=*/true);

    // Copy the containers before min unchanged.
    while (it.value != NULL && compare_high48(it.key, min_high48_key) < 0) {
        leaf_t *leaf1 = (leaf_t *)it.value;
        uint8_t typecode2 = leaf1->typecode;
        container_t *container2 = get_copy_of_container(
            leaf1->container, &typecode2, /*copy_on_write=*/false);
        art_insert(&r2->art, it.key,
                   (art_val_t *)create_leaf(container2, typecode2));
        art_iterator_next(&it);
    }

    // Flip the range (including non-existent containers!) between min and max.
    for (uint64_t high48_bits = min_high48_bits; high48_bits <= max_high48_bits;
         high48_bits++) {
        uint8_t current_high48_key[ART_KEY_BYTES];
        split_key(high48_bits << 16, current_high48_key);

        uint32_t min_container = 0;
        if (high48_bits == min_high48_bits) {
            min_container = min_low16;
        }
        uint32_t max_container = 0xFFFF + 1;  // Exclusive range.
        if (high48_bits == max_high48_bits) {
            max_container = max_low16 + 1;  // Exclusive.
        }

        leaf_t *leaf = roaring64_flip_leaf(r1, current_high48_key,
                                           min_container, max_container);
        if (leaf != NULL) {
            art_insert(&r2->art, current_high48_key, (art_val_t *)leaf);
        }
    }

    // Copy the containers after max unchanged.
    it = art_upper_bound(&r1->art, max_high48_key);
    while (it.value != NULL) {
        leaf_t *leaf1 = (leaf_t *)it.value;
        uint8_t typecode2 = leaf1->typecode;
        container_t *container2 = get_copy_of_container(
            leaf1->container, &typecode2, /*copy_on_write=*/false);
        art_insert(&r2->art, it.key,
                   (art_val_t *)create_leaf(container2, typecode2));
        art_iterator_next(&it);
    }

    return r2;
}

void roaring64_bitmap_flip_inplace(roaring64_bitmap_t *r, uint64_t min,
                                   uint64_t max) {
    if (min >= max) {
        return;
    }
    roaring64_bitmap_flip_closed_inplace(r, min, max - 1);
}

void roaring64_bitmap_flip_closed_inplace(roaring64_bitmap_t *r, uint64_t min,
                                          uint64_t max) {
    if (min > max) {
        return;
    }
    uint16_t min_low16 = (uint16_t)min;
    uint16_t max_low16 = (uint16_t)max;
    uint64_t min_high48_bits = (min & 0xFFFFFFFFFFFF0000ULL) >> 16;
    uint64_t max_high48_bits = (max & 0xFFFFFFFFFFFF0000ULL) >> 16;

    // Flip the range (including non-existent containers!) between min and max.
    for (uint64_t high48_bits = min_high48_bits; high48_bits <= max_high48_bits;
         high48_bits++) {
        uint8_t current_high48_key[ART_KEY_BYTES];
        split_key(high48_bits << 16, current_high48_key);

        uint32_t min_container = 0;
        if (high48_bits == min_high48_bits) {
            min_container = min_low16;
        }
        uint32_t max_container = 0xFFFF + 1;  // Exclusive range.
        if (high48_bits == max_high48_bits) {
            max_container = max_low16 + 1;  // Exclusive.
        }

        roaring64_flip_leaf_inplace(r, current_high48_key, min_container,
                                    max_container);
    }
}

// Returns the number of distinct high 32-bit entries in the bitmap.
static inline uint64_t count_high32(const roaring64_bitmap_t *r) {
    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    uint64_t high32_count = 0;
    uint32_t prev_high32 = 0;
    while (it.value != NULL) {
        uint32_t current_high32 = (uint32_t)(combine_key(it.key, 0) >> 32);
        if (high32_count == 0 || prev_high32 != current_high32) {
            high32_count++;
            prev_high32 = current_high32;
        }
        art_iterator_next(&it);
    }
    return high32_count;
}

// Frees the (32-bit!) bitmap without freeing the containers.
static inline void roaring_bitmap_free_without_containers(roaring_bitmap_t *r) {
    ra_clear_without_containers(&r->high_low_container);
    roaring_free(r);
}

size_t roaring64_bitmap_portable_size_in_bytes(const roaring64_bitmap_t *r) {
    // https://github.com/RoaringBitmap/RoaringFormatSpec#extension-for-64-bit-implementations
    size_t size = 0;

    // Write as uint64 the distinct number of "buckets", where a bucket is
    // defined as the most significant 32 bits of an element.
    uint64_t high32_count;
    size += sizeof(high32_count);

    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    uint32_t prev_high32 = 0;
    roaring_bitmap_t *bitmap32 = NULL;

    // Iterate through buckets ordered by increasing keys.
    while (it.value != NULL) {
        uint32_t current_high32 = (uint32_t)(combine_key(it.key, 0) >> 32);
        if (bitmap32 == NULL || prev_high32 != current_high32) {
            if (bitmap32 != NULL) {
                // Write as uint32 the most significant 32 bits of the bucket.
                size += sizeof(prev_high32);

                // Write the 32-bit Roaring bitmaps representing the least
                // significant bits of a set of elements.
                size += roaring_bitmap_portable_size_in_bytes(bitmap32);
                roaring_bitmap_free_without_containers(bitmap32);
            }

            // Start a new 32-bit bitmap with the current high 32 bits.
            art_iterator_t it2 = it;
            uint32_t containers_with_high32 = 0;
            while (it2.value != NULL && (uint32_t)(combine_key(it2.key, 0) >>
                                                   32) == current_high32) {
                containers_with_high32++;
                art_iterator_next(&it2);
            }
            bitmap32 =
                roaring_bitmap_create_with_capacity(containers_with_high32);

            prev_high32 = current_high32;
        }
        leaf_t *leaf = (leaf_t *)it.value;
        ra_append(&bitmap32->high_low_container,
                  (uint16_t)(current_high32 >> 16), leaf->container,
                  leaf->typecode);
        art_iterator_next(&it);
    }

    if (bitmap32 != NULL) {
        // Write as uint32 the most significant 32 bits of the bucket.
        size += sizeof(prev_high32);

        // Write the 32-bit Roaring bitmaps representing the least
        // significant bits of a set of elements.
        size += roaring_bitmap_portable_size_in_bytes(bitmap32);
        roaring_bitmap_free_without_containers(bitmap32);
    }

    return size;
}

size_t roaring64_bitmap_portable_serialize(const roaring64_bitmap_t *r,
                                           char *buf) {
    // https://github.com/RoaringBitmap/RoaringFormatSpec#extension-for-64-bit-implementations
    if (buf == NULL) {
        return 0;
    }
    const char *initial_buf = buf;

    // Write as uint64 the distinct number of "buckets", where a bucket is
    // defined as the most significant 32 bits of an element.
    uint64_t high32_count = count_high32(r);
    memcpy(buf, &high32_count, sizeof(high32_count));
    buf += sizeof(high32_count);

    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    uint32_t prev_high32 = 0;
    roaring_bitmap_t *bitmap32 = NULL;

    // Iterate through buckets ordered by increasing keys.
    while (it.value != NULL) {
        uint64_t current_high48 = combine_key(it.key, 0);
        uint32_t current_high32 = (uint32_t)(current_high48 >> 32);
        if (bitmap32 == NULL || prev_high32 != current_high32) {
            if (bitmap32 != NULL) {
                // Write as uint32 the most significant 32 bits of the bucket.
                memcpy(buf, &prev_high32, sizeof(prev_high32));
                buf += sizeof(prev_high32);

                // Write the 32-bit Roaring bitmaps representing the least
                // significant bits of a set of elements.
                buf += roaring_bitmap_portable_serialize(bitmap32, buf);
                roaring_bitmap_free_without_containers(bitmap32);
            }

            // Start a new 32-bit bitmap with the current high 32 bits.
            art_iterator_t it2 = it;
            uint32_t containers_with_high32 = 0;
            while (it2.value != NULL &&
                   (uint32_t)combine_key(it2.key, 0) == current_high32) {
                containers_with_high32++;
                art_iterator_next(&it2);
            }
            bitmap32 =
                roaring_bitmap_create_with_capacity(containers_with_high32);

            prev_high32 = current_high32;
        }
        leaf_t *leaf = (leaf_t *)it.value;
        ra_append(&bitmap32->high_low_container,
                  (uint16_t)(current_high48 >> 16), leaf->container,
                  leaf->typecode);
        art_iterator_next(&it);
    }

    if (bitmap32 != NULL) {
        // Write as uint32 the most significant 32 bits of the bucket.
        memcpy(buf, &prev_high32, sizeof(prev_high32));
        buf += sizeof(prev_high32);

        // Write the 32-bit Roaring bitmaps representing the least
        // significant bits of a set of elements.
        buf += roaring_bitmap_portable_serialize(bitmap32, buf);
        roaring_bitmap_free_without_containers(bitmap32);
    }

    return buf - initial_buf;
}

size_t roaring64_bitmap_portable_deserialize_size(const char *buf,
                                                  size_t maxbytes) {
    // https://github.com/RoaringBitmap/RoaringFormatSpec#extension-for-64-bit-implementations
    if (buf == NULL) {
        return 0;
    }
    size_t read_bytes = 0;

    // Read as uint64 the distinct number of "buckets", where a bucket is
    // defined as the most significant 32 bits of an element.
    uint64_t buckets;
    if (read_bytes + sizeof(buckets) > maxbytes) {
        return 0;
    }
    memcpy(&buckets, buf, sizeof(buckets));
    buf += sizeof(buckets);
    read_bytes += sizeof(buckets);

    // Buckets should be 32 bits with 4 bits of zero padding.
    if (buckets > UINT32_MAX) {
        return 0;
    }

    // Iterate through buckets ordered by increasing keys.
    for (uint64_t bucket = 0; bucket < buckets; ++bucket) {
        // Read as uint32 the most significant 32 bits of the bucket.
        uint32_t high32;
        if (read_bytes + sizeof(high32) > maxbytes) {
            return 0;
        }
        buf += sizeof(high32);
        read_bytes += sizeof(high32);

        // Read the 32-bit Roaring bitmaps representing the least significant
        // bits of a set of elements.
        size_t bitmap32_size = roaring_bitmap_portable_deserialize_size(
            buf, maxbytes - read_bytes);
        if (bitmap32_size == 0) {
            return 0;
        }
        buf += bitmap32_size;
        read_bytes += bitmap32_size;
    }
    return read_bytes;
}

roaring64_bitmap_t *roaring64_bitmap_portable_deserialize_safe(
    const char *buf, size_t maxbytes) {
    // https://github.com/RoaringBitmap/RoaringFormatSpec#extension-for-64-bit-implementations
    if (buf == NULL) {
        return NULL;
    }
    size_t read_bytes = 0;

    // Read as uint64 the distinct number of "buckets", where a bucket is
    // defined as the most significant 32 bits of an element.
    uint64_t buckets;
    if (read_bytes + sizeof(buckets) > maxbytes) {
        return NULL;
    }
    memcpy(&buckets, buf, sizeof(buckets));
    buf += sizeof(buckets);
    read_bytes += sizeof(buckets);

    // Buckets should be 32 bits with 4 bits of zero padding.
    if (buckets > UINT32_MAX) {
        return NULL;
    }

    roaring64_bitmap_t *r = roaring64_bitmap_create();
    // Iterate through buckets ordered by increasing keys.
    int64_t previous_high32 = -1;
    for (uint64_t bucket = 0; bucket < buckets; ++bucket) {
        // Read as uint32 the most significant 32 bits of the bucket.
        uint32_t high32;
        if (read_bytes + sizeof(high32) > maxbytes) {
            roaring64_bitmap_free(r);
            return NULL;
        }
        memcpy(&high32, buf, sizeof(high32));
        buf += sizeof(high32);
        read_bytes += sizeof(high32);
        // High 32 bits must be strictly increasing.
        if (high32 <= previous_high32) {
            roaring64_bitmap_free(r);
            return NULL;
        }
        previous_high32 = high32;

        // Read the 32-bit Roaring bitmaps representing the least significant
        // bits of a set of elements.
        size_t bitmap32_size = roaring_bitmap_portable_deserialize_size(
            buf, maxbytes - read_bytes);
        if (bitmap32_size == 0) {
            roaring64_bitmap_free(r);
            return NULL;
        }

        roaring_bitmap_t *bitmap32 = roaring_bitmap_portable_deserialize_safe(
            buf, maxbytes - read_bytes);
        if (bitmap32 == NULL) {
            roaring64_bitmap_free(r);
            return NULL;
        }
        buf += bitmap32_size;
        read_bytes += bitmap32_size;

        // While we don't attempt to validate much, we must ensure that there
        // is no duplication in the high 48 bits - inserting into the ART
        // assumes (or UB) no duplicate keys. The top 32 bits must be unique
        // because we check for strict increasing values of  high32, but we
        // must also ensure the top 16 bits within each 32-bit bitmap are also
        // at least unique (we ensure they're strictly increasing as well,
        // which they must be for a _valid_ bitmap, since it's cheaper to check)
        int32_t last_bitmap_key = -1;
        for (int i = 0; i < bitmap32->high_low_container.size; i++) {
            uint16_t key = bitmap32->high_low_container.keys[i];
            if (key <= last_bitmap_key) {
                roaring_bitmap_free(bitmap32);
                roaring64_bitmap_free(r);
                return NULL;
            }
            last_bitmap_key = key;
        }

        // Insert all containers of the 32-bit bitmap into the 64-bit bitmap.
        move_from_roaring32_offset(r, bitmap32, high32);
        roaring_bitmap_free(bitmap32);
    }
    return r;
}

bool roaring64_bitmap_iterate(const roaring64_bitmap_t *r,
                              roaring_iterator64 iterator, void *ptr) {
    art_iterator_t it = art_init_iterator(&r->art, /*first=*/true);
    while (it.value != NULL) {
        uint64_t high48 = combine_key(it.key, 0);
        uint64_t high32 = high48 & 0xFFFFFFFF00000000ULL;
        uint32_t low32 = high48;
        leaf_t *leaf = (leaf_t *)it.value;
        if (!container_iterate64(leaf->container, leaf->typecode, low32,
                                 iterator, high32, ptr)) {
            return false;
        }
        art_iterator_next(&it);
    }
    return true;
}

void roaring64_bitmap_to_uint64_array(const roaring64_bitmap_t *r,
                                      uint64_t *out) {
    roaring64_iterator_t it;  // gets initialized in the next line
    roaring64_iterator_init_at(r, &it, /*first=*/true);
    roaring64_iterator_read(&it, out, UINT64_MAX);
}

roaring64_iterator_t *roaring64_iterator_create(const roaring64_bitmap_t *r) {
    roaring64_iterator_t *it =
        (roaring64_iterator_t *)roaring_malloc(sizeof(roaring64_iterator_t));
    return roaring64_iterator_init_at(r, it, /*first=*/true);
}

roaring64_iterator_t *roaring64_iterator_create_last(
    const roaring64_bitmap_t *r) {
    roaring64_iterator_t *it =
        (roaring64_iterator_t *)roaring_malloc(sizeof(roaring64_iterator_t));
    return roaring64_iterator_init_at(r, it, /*first=*/false);
}

void roaring64_iterator_reinit(const roaring64_bitmap_t *r,
                               roaring64_iterator_t *it) {
    roaring64_iterator_init_at(r, it, /*first=*/true);
}

void roaring64_iterator_reinit_last(const roaring64_bitmap_t *r,
                                    roaring64_iterator_t *it) {
    roaring64_iterator_init_at(r, it, /*first=*/false);
}

roaring64_iterator_t *roaring64_iterator_copy(const roaring64_iterator_t *it) {
    roaring64_iterator_t *new_it =
        (roaring64_iterator_t *)roaring_malloc(sizeof(roaring64_iterator_t));
    memcpy(new_it, it, sizeof(*it));
    return new_it;
}

void roaring64_iterator_free(roaring64_iterator_t *it) { roaring_free(it); }

bool roaring64_iterator_has_value(const roaring64_iterator_t *it) {
    return it->has_value;
}

uint64_t roaring64_iterator_value(const roaring64_iterator_t *it) {
    return it->value;
}

bool roaring64_iterator_advance(roaring64_iterator_t *it) {
    if (it->art_it.value == NULL) {
        if (it->saturated_forward) {
            return (it->has_value = false);
        }
        roaring64_iterator_init_at(it->parent, it, /*first=*/true);
        return it->has_value;
    }
    leaf_t *leaf = (leaf_t *)it->art_it.value;
    uint16_t low16 = (uint16_t)it->value;
    if (container_iterator_next(leaf->container, leaf->typecode,
                                &it->container_it, &low16)) {
        it->value = it->high48 | low16;
        return (it->has_value = true);
    }
    if (art_iterator_next(&it->art_it)) {
        return roaring64_iterator_init_at_leaf_first(it);
    }
    it->saturated_forward = true;
    return (it->has_value = false);
}

bool roaring64_iterator_previous(roaring64_iterator_t *it) {
    if (it->art_it.value == NULL) {
        if (!it->saturated_forward) {
            // Saturated backward.
            return (it->has_value = false);
        }
        roaring64_iterator_init_at(it->parent, it, /*first=*/false);
        return it->has_value;
    }
    leaf_t *leaf = (leaf_t *)it->art_it.value;
    uint16_t low16 = (uint16_t)it->value;
    if (container_iterator_prev(leaf->container, leaf->typecode,
                                &it->container_it, &low16)) {
        it->value = it->high48 | low16;
        return (it->has_value = true);
    }
    if (art_iterator_prev(&it->art_it)) {
        return roaring64_iterator_init_at_leaf_last(it);
    }
    it->saturated_forward = false;  // Saturated backward.
    return (it->has_value = false);
}

bool roaring64_iterator_move_equalorlarger(roaring64_iterator_t *it,
                                           uint64_t val) {
    uint8_t val_high48[ART_KEY_BYTES];
    uint16_t val_low16 = split_key(val, val_high48);
    if (!it->has_value || it->high48 != (val & 0xFFFFFFFFFFFF0000)) {
        // The ART iterator is before or after the high48 bits of `val` (or
        // beyond the ART altogether), so we need to move to a leaf with a key
        // equal or greater.
        if (!art_iterator_lower_bound(&it->art_it, val_high48)) {
            // Only smaller keys found.
            it->saturated_forward = true;
            return (it->has_value = false);
        }
        it->high48 = combine_key(it->art_it.key, 0);
        // Fall through to the next if statement.
    }

    if (it->high48 == (val & 0xFFFFFFFFFFFF0000)) {
        // We're at equal high bits, check if a suitable value can be found in
        // this container.
        leaf_t *leaf = (leaf_t *)it->art_it.value;
        uint16_t low16 = (uint16_t)it->value;
        if (container_iterator_lower_bound(leaf->container, leaf->typecode,
                                           &it->container_it, &low16,
                                           val_low16)) {
            it->value = it->high48 | low16;
            return (it->has_value = true);
        }
        // Only smaller entries in this container, move to the next.
        if (!art_iterator_next(&it->art_it)) {
            it->saturated_forward = true;
            return (it->has_value = false);
        }
    }

    // We're at a leaf with high bits greater than `val`, so the first entry in
    // this container is our result.
    return roaring64_iterator_init_at_leaf_first(it);
}

uint64_t roaring64_iterator_read(roaring64_iterator_t *it, uint64_t *buf,
                                 uint64_t count) {
    uint64_t consumed = 0;
    while (it->has_value && consumed < count) {
        uint32_t container_consumed;
        leaf_t *leaf = (leaf_t *)it->art_it.value;
        uint16_t low16 = (uint16_t)it->value;
        uint32_t container_count = UINT32_MAX;
        if (count - consumed < (uint64_t)UINT32_MAX) {
            container_count = count - consumed;
        }
        bool has_value = container_iterator_read_into_uint64(
            leaf->container, leaf->typecode, &it->container_it, it->high48, buf,
            container_count, &container_consumed, &low16);
        consumed += container_consumed;
        buf += container_consumed;
        if (has_value) {
            it->has_value = true;
            it->value = it->high48 | low16;
            assert(consumed == count);
            return consumed;
        }
        it->has_value = art_iterator_next(&it->art_it);
        if (it->has_value) {
            roaring64_iterator_init_at_leaf_first(it);
        }
    }
    return consumed;
}

#ifdef __cplusplus
}  // extern "C"
}  // namespace roaring
}  // namespace api
#endif
