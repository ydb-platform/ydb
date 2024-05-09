
#include <roaring/containers/containers.h>
#include <roaring/memory.h>

#ifdef __cplusplus
extern "C" {
// In Windows MSVC C++ compiler, (type){init} does not compile,
// it causes C4576: a parenthesized type followed by an initializer list is a
// non-standard explicit type conversion syntax The correct syntax is type{init}
#define ROARING_INIT_ROARING_CONTAINER_ITERATOR_T roaring_container_iterator_t
namespace roaring {
namespace internal {
#else
#define ROARING_INIT_ROARING_CONTAINER_ITERATOR_T (roaring_container_iterator_t)
#endif

static inline uint32_t minimum_uint32(uint32_t a, uint32_t b) {
    return (a < b) ? a : b;
}

extern inline const container_t *container_unwrap_shared(
    const container_t *candidate_shared_container, uint8_t *type);

extern inline container_t *container_mutable_unwrap_shared(
    container_t *candidate_shared_container, uint8_t *type);

extern inline int container_get_cardinality(const container_t *c,
                                            uint8_t typecode);

extern inline container_t *container_iand(container_t *c1, uint8_t type1,
                                          const container_t *c2, uint8_t type2,
                                          uint8_t *result_type);

extern inline container_t *container_ior(container_t *c1, uint8_t type1,
                                         const container_t *c2, uint8_t type2,
                                         uint8_t *result_type);

extern inline container_t *container_ixor(container_t *c1, uint8_t type1,
                                          const container_t *c2, uint8_t type2,
                                          uint8_t *result_type);

extern inline container_t *container_iandnot(container_t *c1, uint8_t type1,
                                             const container_t *c2,
                                             uint8_t type2,
                                             uint8_t *result_type);

void container_free(container_t *c, uint8_t type) {
    switch (type) {
        case BITSET_CONTAINER_TYPE:
            bitset_container_free(CAST_bitset(c));
            break;
        case ARRAY_CONTAINER_TYPE:
            array_container_free(CAST_array(c));
            break;
        case RUN_CONTAINER_TYPE:
            run_container_free(CAST_run(c));
            break;
        case SHARED_CONTAINER_TYPE:
            shared_container_free(CAST_shared(c));
            break;
        default:
            assert(false);
            roaring_unreachable;
    }
}

void container_printf(const container_t *c, uint8_t type) {
    c = container_unwrap_shared(c, &type);
    switch (type) {
        case BITSET_CONTAINER_TYPE:
            bitset_container_printf(const_CAST_bitset(c));
            return;
        case ARRAY_CONTAINER_TYPE:
            array_container_printf(const_CAST_array(c));
            return;
        case RUN_CONTAINER_TYPE:
            run_container_printf(const_CAST_run(c));
            return;
        default:
            roaring_unreachable;
    }
}

void container_printf_as_uint32_array(const container_t *c, uint8_t typecode,
                                      uint32_t base) {
    c = container_unwrap_shared(c, &typecode);
    switch (typecode) {
        case BITSET_CONTAINER_TYPE:
            bitset_container_printf_as_uint32_array(const_CAST_bitset(c), base);
            return;
        case ARRAY_CONTAINER_TYPE:
            array_container_printf_as_uint32_array(const_CAST_array(c), base);
            return;
        case RUN_CONTAINER_TYPE:
            run_container_printf_as_uint32_array(const_CAST_run(c), base);
            return;
        default:
            roaring_unreachable;
    }
}

bool container_internal_validate(const container_t *container, uint8_t typecode,
                                 const char **reason) {
    if (container == NULL) {
        *reason = "container is NULL";
        return false;
    }
    // Not using container_unwrap_shared because it asserts if shared containers
    // are nested
    if (typecode == SHARED_CONTAINER_TYPE) {
        const shared_container_t *shared_container =
            const_CAST_shared(container);
        if (croaring_refcount_get(&shared_container->counter) == 0) {
            *reason = "shared container has zero refcount";
            return false;
        }
        if (shared_container->typecode == SHARED_CONTAINER_TYPE) {
            *reason = "shared container is nested";
            return false;
        }
        if (shared_container->container == NULL) {
            *reason = "shared container has NULL container";
            return false;
        }
        container = shared_container->container;
        typecode = shared_container->typecode;
    }
    switch (typecode) {
        case BITSET_CONTAINER_TYPE:
            return bitset_container_validate(const_CAST_bitset(container),
                                             reason);
        case ARRAY_CONTAINER_TYPE:
            return array_container_validate(const_CAST_array(container),
                                            reason);
        case RUN_CONTAINER_TYPE:
            return run_container_validate(const_CAST_run(container), reason);
        default:
            *reason = "invalid typecode";
            return false;
    }
}

extern inline bool container_nonzero_cardinality(const container_t *c,
                                                 uint8_t typecode);

extern inline int container_to_uint32_array(uint32_t *output,
                                            const container_t *c,
                                            uint8_t typecode, uint32_t base);

extern inline container_t *container_add(container_t *c, uint16_t val,
                                         uint8_t typecode,  // !!! 2nd arg?
                                         uint8_t *new_typecode);

extern inline bool container_contains(const container_t *c, uint16_t val,
                                      uint8_t typecode);  // !!! 2nd arg?

extern inline container_t *container_and(const container_t *c1, uint8_t type1,
                                         const container_t *c2, uint8_t type2,
                                         uint8_t *result_type);

extern inline container_t *container_or(const container_t *c1, uint8_t type1,
                                        const container_t *c2, uint8_t type2,
                                        uint8_t *result_type);

extern inline container_t *container_xor(const container_t *c1, uint8_t type1,
                                         const container_t *c2, uint8_t type2,
                                         uint8_t *result_type);

container_t *get_copy_of_container(container_t *c, uint8_t *typecode,
                                   bool copy_on_write) {
    if (copy_on_write) {
        shared_container_t *shared_container;
        if (*typecode == SHARED_CONTAINER_TYPE) {
            shared_container = CAST_shared(c);
            croaring_refcount_inc(&shared_container->counter);
            return shared_container;
        }
        assert(*typecode != SHARED_CONTAINER_TYPE);

        if ((shared_container = (shared_container_t *)roaring_malloc(
                 sizeof(shared_container_t))) == NULL) {
            return NULL;
        }

        shared_container->container = c;
        shared_container->typecode = *typecode;
        // At this point, we are creating new shared container
        // so there should be no other references, and setting
        // the counter to 2 - even non-atomically - is safe as
        // long as the value is set before the return statement.
        shared_container->counter = 2;
        *typecode = SHARED_CONTAINER_TYPE;

        return shared_container;
    }  // copy_on_write
    // otherwise, no copy on write...
    const container_t *actual_container = container_unwrap_shared(c, typecode);
    assert(*typecode != SHARED_CONTAINER_TYPE);
    return container_clone(actual_container, *typecode);
}

/**
 * Copies a container, requires a typecode. This allocates new memory, caller
 * is responsible for deallocation.
 */
container_t *container_clone(const container_t *c, uint8_t typecode) {
    // We do not want to allow cloning of shared containers.
    // c = container_unwrap_shared(c, &typecode);
    switch (typecode) {
        case BITSET_CONTAINER_TYPE:
            return bitset_container_clone(const_CAST_bitset(c));
        case ARRAY_CONTAINER_TYPE:
            return array_container_clone(const_CAST_array(c));
        case RUN_CONTAINER_TYPE:
            return run_container_clone(const_CAST_run(c));
        case SHARED_CONTAINER_TYPE:
            // Shared containers are not cloneable. Are you mixing COW and
            // non-COW bitmaps?
            return NULL;
        default:
            assert(false);
            roaring_unreachable;
            return NULL;
    }
}

container_t *shared_container_extract_copy(shared_container_t *sc,
                                           uint8_t *typecode) {
    assert(sc->typecode != SHARED_CONTAINER_TYPE);
    *typecode = sc->typecode;
    container_t *answer;
    if (croaring_refcount_dec(&sc->counter)) {
        answer = sc->container;
        sc->container = NULL;  // paranoid
        roaring_free(sc);
    } else {
        answer = container_clone(sc->container, *typecode);
    }
    assert(*typecode != SHARED_CONTAINER_TYPE);
    return answer;
}

void shared_container_free(shared_container_t *container) {
    if (croaring_refcount_dec(&container->counter)) {
        assert(container->typecode != SHARED_CONTAINER_TYPE);
        container_free(container->container, container->typecode);
        container->container = NULL;  // paranoid
        roaring_free(container);
    }
}

extern inline container_t *container_not(const container_t *c1, uint8_t type1,
                                         uint8_t *result_type);

extern inline container_t *container_not_range(const container_t *c1,
                                               uint8_t type1,
                                               uint32_t range_start,
                                               uint32_t range_end,
                                               uint8_t *result_type);

extern inline container_t *container_inot(container_t *c1, uint8_t type1,
                                          uint8_t *result_type);

extern inline container_t *container_inot_range(container_t *c1, uint8_t type1,
                                                uint32_t range_start,
                                                uint32_t range_end,
                                                uint8_t *result_type);

extern inline container_t *container_range_of_ones(uint32_t range_start,
                                                   uint32_t range_end,
                                                   uint8_t *result_type);

// where are the correponding things for union and intersection??
extern inline container_t *container_lazy_xor(const container_t *c1,
                                              uint8_t type1,
                                              const container_t *c2,
                                              uint8_t type2,
                                              uint8_t *result_type);

extern inline container_t *container_lazy_ixor(container_t *c1, uint8_t type1,
                                               const container_t *c2,
                                               uint8_t type2,
                                               uint8_t *result_type);

extern inline container_t *container_andnot(const container_t *c1,
                                            uint8_t type1,
                                            const container_t *c2,
                                            uint8_t type2,
                                            uint8_t *result_type);

roaring_container_iterator_t container_init_iterator(const container_t *c,
                                                     uint8_t typecode,
                                                     uint16_t *value) {
    switch (typecode) {
        case BITSET_CONTAINER_TYPE: {
            const bitset_container_t *bc = const_CAST_bitset(c);
            uint32_t wordindex = 0;
            uint64_t word;
            while ((word = bc->words[wordindex]) == 0) {
                wordindex++;
            }
            // word is non-zero
            int32_t index = wordindex * 64 + roaring_trailing_zeroes(word);
            *value = index;
            return ROARING_INIT_ROARING_CONTAINER_ITERATOR_T{
                .index = index,
            };
        }
        case ARRAY_CONTAINER_TYPE: {
            const array_container_t *ac = const_CAST_array(c);
            *value = ac->array[0];
            return ROARING_INIT_ROARING_CONTAINER_ITERATOR_T{
                .index = 0,
            };
        }
        case RUN_CONTAINER_TYPE: {
            const run_container_t *rc = const_CAST_run(c);
            *value = rc->runs[0].value;
            return ROARING_INIT_ROARING_CONTAINER_ITERATOR_T{
                .index = 0,
            };
        }
        default:
            assert(false);
            roaring_unreachable;
            return ROARING_INIT_ROARING_CONTAINER_ITERATOR_T{0};
    }
}

roaring_container_iterator_t container_init_iterator_last(const container_t *c,
                                                          uint8_t typecode,
                                                          uint16_t *value) {
    switch (typecode) {
        case BITSET_CONTAINER_TYPE: {
            const bitset_container_t *bc = const_CAST_bitset(c);
            uint32_t wordindex = BITSET_CONTAINER_SIZE_IN_WORDS - 1;
            uint64_t word;
            while ((word = bc->words[wordindex]) == 0) {
                wordindex--;
            }
            // word is non-zero
            int32_t index =
                wordindex * 64 + (63 - roaring_leading_zeroes(word));
            *value = index;
            return ROARING_INIT_ROARING_CONTAINER_ITERATOR_T{
                .index = index,
            };
        }
        case ARRAY_CONTAINER_TYPE: {
            const array_container_t *ac = const_CAST_array(c);
            int32_t index = ac->cardinality - 1;
            *value = ac->array[index];
            return ROARING_INIT_ROARING_CONTAINER_ITERATOR_T{
                .index = index,
            };
        }
        case RUN_CONTAINER_TYPE: {
            const run_container_t *rc = const_CAST_run(c);
            int32_t run_index = rc->n_runs - 1;
            const rle16_t *last_run = &rc->runs[run_index];
            *value = last_run->value + last_run->length;
            return ROARING_INIT_ROARING_CONTAINER_ITERATOR_T{
                .index = run_index,
            };
        }
        default:
            assert(false);
            roaring_unreachable;
            return ROARING_INIT_ROARING_CONTAINER_ITERATOR_T{0};
    }
}

bool container_iterator_next(const container_t *c, uint8_t typecode,
                             roaring_container_iterator_t *it,
                             uint16_t *value) {
    switch (typecode) {
        case BITSET_CONTAINER_TYPE: {
            const bitset_container_t *bc = const_CAST_bitset(c);
            it->index++;

            uint32_t wordindex = it->index / 64;
            if (wordindex >= BITSET_CONTAINER_SIZE_IN_WORDS) {
                return false;
            }

            uint64_t word =
                bc->words[wordindex] & (UINT64_MAX << (it->index % 64));
            // next part could be optimized/simplified
            while (word == 0 &&
                   (wordindex + 1 < BITSET_CONTAINER_SIZE_IN_WORDS)) {
                wordindex++;
                word = bc->words[wordindex];
            }
            if (word != 0) {
                it->index = wordindex * 64 + roaring_trailing_zeroes(word);
                *value = it->index;
                return true;
            }
            return false;
        }
        case ARRAY_CONTAINER_TYPE: {
            const array_container_t *ac = const_CAST_array(c);
            it->index++;
            if (it->index < ac->cardinality) {
                *value = ac->array[it->index];
                return true;
            }
            return false;
        }
        case RUN_CONTAINER_TYPE: {
            if (*value == UINT16_MAX) {  // Avoid overflow to zero
                return false;
            }

            const run_container_t *rc = const_CAST_run(c);
            uint32_t limit =
                rc->runs[it->index].value + rc->runs[it->index].length;
            if (*value < limit) {
                (*value)++;
                return true;
            }

            it->index++;
            if (it->index < rc->n_runs) {
                *value = rc->runs[it->index].value;
                return true;
            }
            return false;
        }
        default:
            assert(false);
            roaring_unreachable;
            return false;
    }
}

bool container_iterator_prev(const container_t *c, uint8_t typecode,
                             roaring_container_iterator_t *it,
                             uint16_t *value) {
    switch (typecode) {
        case BITSET_CONTAINER_TYPE: {
            if (--it->index < 0) {
                return false;
            }

            const bitset_container_t *bc = const_CAST_bitset(c);
            int32_t wordindex = it->index / 64;
            uint64_t word =
                bc->words[wordindex] & (UINT64_MAX >> (63 - (it->index % 64)));

            while (word == 0 && --wordindex >= 0) {
                word = bc->words[wordindex];
            }
            if (word == 0) {
                return false;
            }

            it->index = (wordindex * 64) + (63 - roaring_leading_zeroes(word));
            *value = it->index;
            return true;
        }
        case ARRAY_CONTAINER_TYPE: {
            if (--it->index < 0) {
                return false;
            }
            const array_container_t *ac = const_CAST_array(c);
            *value = ac->array[it->index];
            return true;
        }
        case RUN_CONTAINER_TYPE: {
            if (*value == 0) {
                return false;
            }

            const run_container_t *rc = const_CAST_run(c);
            (*value)--;
            if (*value >= rc->runs[it->index].value) {
                return true;
            }

            if (--it->index < 0) {
                return false;
            }

            *value = rc->runs[it->index].value + rc->runs[it->index].length;
            return true;
        }
        default:
            assert(false);
            roaring_unreachable;
            return false;
    }
}

bool container_iterator_lower_bound(const container_t *c, uint8_t typecode,
                                    roaring_container_iterator_t *it,
                                    uint16_t *value_out, uint16_t val) {
    if (val > container_maximum(c, typecode)) {
        return false;
    }
    switch (typecode) {
        case BITSET_CONTAINER_TYPE: {
            const bitset_container_t *bc = const_CAST_bitset(c);
            it->index = bitset_container_index_equalorlarger(bc, val);
            *value_out = it->index;
            return true;
        }
        case ARRAY_CONTAINER_TYPE: {
            const array_container_t *ac = const_CAST_array(c);
            it->index = array_container_index_equalorlarger(ac, val);
            *value_out = ac->array[it->index];
            return true;
        }
        case RUN_CONTAINER_TYPE: {
            const run_container_t *rc = const_CAST_run(c);
            it->index = run_container_index_equalorlarger(rc, val);
            if (rc->runs[it->index].value <= val) {
                *value_out = val;
            } else {
                *value_out = rc->runs[it->index].value;
            }
            return true;
        }
        default:
            assert(false);
            roaring_unreachable;
            return false;
    }
}

bool container_iterator_read_into_uint32(const container_t *c, uint8_t typecode,
                                         roaring_container_iterator_t *it,
                                         uint32_t high16, uint32_t *buf,
                                         uint32_t count, uint32_t *consumed,
                                         uint16_t *value_out) {
    *consumed = 0;
    if (count == 0) {
        return false;
    }
    switch (typecode) {
        case BITSET_CONTAINER_TYPE: {
            const bitset_container_t *bc = const_CAST_bitset(c);
            uint32_t wordindex = it->index / 64;
            uint64_t word =
                bc->words[wordindex] & (UINT64_MAX << (it->index % 64));
            do {
                // Read set bits.
                while (word != 0 && *consumed < count) {
                    *buf = high16 |
                           (wordindex * 64 + roaring_trailing_zeroes(word));
                    word = word & (word - 1);
                    buf++;
                    (*consumed)++;
                }
                // Skip unset bits.
                while (word == 0 &&
                       wordindex + 1 < BITSET_CONTAINER_SIZE_IN_WORDS) {
                    wordindex++;
                    word = bc->words[wordindex];
                }
            } while (word != 0 && *consumed < count);

            if (word != 0) {
                it->index = wordindex * 64 + roaring_trailing_zeroes(word);
                *value_out = it->index;
                return true;
            }
            return false;
        }
        case ARRAY_CONTAINER_TYPE: {
            const array_container_t *ac = const_CAST_array(c);
            uint32_t num_values =
                minimum_uint32(ac->cardinality - it->index, count);
            for (uint32_t i = 0; i < num_values; i++) {
                buf[i] = high16 | ac->array[it->index + i];
            }
            *consumed += num_values;
            it->index += num_values;
            if (it->index < ac->cardinality) {
                *value_out = ac->array[it->index];
                return true;
            }
            return false;
        }
        case RUN_CONTAINER_TYPE: {
            const run_container_t *rc = const_CAST_run(c);
            do {
                uint32_t largest_run_value =
                    rc->runs[it->index].value + rc->runs[it->index].length;
                uint32_t num_values = minimum_uint32(
                    largest_run_value - *value_out + 1, count - *consumed);
                for (uint32_t i = 0; i < num_values; i++) {
                    buf[i] = high16 | (*value_out + i);
                }
                *value_out += num_values;
                buf += num_values;
                *consumed += num_values;

                // We check for `value == 0` because `it->value += num_values`
                // can overflow when `value == UINT16_MAX`, and `count >
                // length`. In this case `value` will overflow to 0.
                if (*value_out > largest_run_value || *value_out == 0) {
                    it->index++;
                    if (it->index < rc->n_runs) {
                        *value_out = rc->runs[it->index].value;
                    } else {
                        return false;
                    }
                }
            } while (*consumed < count);
            return true;
        }
        default:
            assert(false);
            roaring_unreachable;
            return 0;
    }
}

bool container_iterator_read_into_uint64(const container_t *c, uint8_t typecode,
                                         roaring_container_iterator_t *it,
                                         uint64_t high48, uint64_t *buf,
                                         uint32_t count, uint32_t *consumed,
                                         uint16_t *value_out) {
    *consumed = 0;
    if (count == 0) {
        return false;
    }
    switch (typecode) {
        case BITSET_CONTAINER_TYPE: {
            const bitset_container_t *bc = const_CAST_bitset(c);
            uint32_t wordindex = it->index / 64;
            uint64_t word =
                bc->words[wordindex] & (UINT64_MAX << (it->index % 64));
            do {
                // Read set bits.
                while (word != 0 && *consumed < count) {
                    *buf = high48 |
                           (wordindex * 64 + roaring_trailing_zeroes(word));
                    word = word & (word - 1);
                    buf++;
                    (*consumed)++;
                }
                // Skip unset bits.
                while (word == 0 &&
                       wordindex + 1 < BITSET_CONTAINER_SIZE_IN_WORDS) {
                    wordindex++;
                    word = bc->words[wordindex];
                }
            } while (word != 0 && *consumed < count);

            if (word != 0) {
                it->index = wordindex * 64 + roaring_trailing_zeroes(word);
                *value_out = it->index;
                return true;
            }
            return false;
        }
        case ARRAY_CONTAINER_TYPE: {
            const array_container_t *ac = const_CAST_array(c);
            uint32_t num_values =
                minimum_uint32(ac->cardinality - it->index, count);
            for (uint32_t i = 0; i < num_values; i++) {
                buf[i] = high48 | ac->array[it->index + i];
            }
            *consumed += num_values;
            it->index += num_values;
            if (it->index < ac->cardinality) {
                *value_out = ac->array[it->index];
                return true;
            }
            return false;
        }
        case RUN_CONTAINER_TYPE: {
            const run_container_t *rc = const_CAST_run(c);
            do {
                uint32_t largest_run_value =
                    rc->runs[it->index].value + rc->runs[it->index].length;
                uint32_t num_values = minimum_uint32(
                    largest_run_value - *value_out + 1, count - *consumed);
                for (uint32_t i = 0; i < num_values; i++) {
                    buf[i] = high48 | (*value_out + i);
                }
                *value_out += num_values;
                buf += num_values;
                *consumed += num_values;

                // We check for `value == 0` because `it->value += num_values`
                // can overflow when `value == UINT16_MAX`, and `count >
                // length`. In this case `value` will overflow to 0.
                if (*value_out > largest_run_value || *value_out == 0) {
                    it->index++;
                    if (it->index < rc->n_runs) {
                        *value_out = rc->runs[it->index].value;
                    } else {
                        return false;
                    }
                }
            } while (*consumed < count);
            return true;
        }
        default:
            assert(false);
            roaring_unreachable;
            return 0;
    }
}

#ifdef __cplusplus
}
}
}  // extern "C" { namespace roaring { namespace internal {
#endif

#undef ROARING_INIT_ROARING_CONTAINER_ITERATOR_T
