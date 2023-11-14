
#include <roaring/containers/containers.h>
#include <roaring/memory.h>

#ifdef __cplusplus
extern "C" { namespace roaring { namespace internal {
#endif

extern inline const container_t *container_unwrap_shared(
        const container_t *candidate_shared_container, uint8_t *type);

extern inline container_t *container_mutable_unwrap_shared(
        container_t *candidate_shared_container, uint8_t *type);

extern inline int container_get_cardinality(
        const container_t *c, uint8_t typecode);

extern inline container_t *container_iand(
        container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
        uint8_t *result_type);

extern inline container_t *container_ior(
        container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
        uint8_t *result_type);

extern inline container_t *container_ixor(
        container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
        uint8_t *result_type);

extern inline container_t *container_iandnot(
        container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
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

void container_printf_as_uint32_array(
    const container_t *c, uint8_t typecode,
    uint32_t base
){
    c = container_unwrap_shared(c, &typecode);
    switch (typecode) {
        case BITSET_CONTAINER_TYPE:
            bitset_container_printf_as_uint32_array(
                const_CAST_bitset(c), base);
            return;
        case ARRAY_CONTAINER_TYPE:
            array_container_printf_as_uint32_array(
                const_CAST_array(c), base);
            return;
        case RUN_CONTAINER_TYPE:
            run_container_printf_as_uint32_array(
                const_CAST_run(c), base);
            return;
        default:
            roaring_unreachable;
    }
}

bool container_internal_validate(const container_t *container,
                                 uint8_t typecode, const char **reason) {
    if (container == NULL) {
        *reason = "container is NULL";
        return false;
    }
    // Not using container_unwrap_shared because it asserts if shared containers are nested
    if (typecode == SHARED_CONTAINER_TYPE) {
        const shared_container_t *shared_container = const_CAST_shared(container);
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
            return bitset_container_validate(const_CAST_bitset(container), reason);
        case ARRAY_CONTAINER_TYPE:
            return array_container_validate(const_CAST_array(container), reason);
        case RUN_CONTAINER_TYPE:
            return run_container_validate(const_CAST_run(container), reason);
        default:
            *reason = "invalid typecode";
            return false;
    }
}

extern inline bool container_nonzero_cardinality(
        const container_t *c, uint8_t typecode);

extern inline int container_to_uint32_array(
        uint32_t *output,
        const container_t *c, uint8_t typecode,
        uint32_t base);

extern inline container_t *container_add(
        container_t *c,
        uint16_t val,
        uint8_t typecode,  // !!! 2nd arg?
        uint8_t *new_typecode);

extern inline bool container_contains(
        const container_t *c,
        uint16_t val,
        uint8_t typecode);  // !!! 2nd arg?

extern inline container_t *container_and(
        const container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
        uint8_t *result_type);

extern inline container_t *container_or(
        const container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
        uint8_t *result_type);

extern inline container_t *container_xor(
        const container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
        uint8_t *result_type);

container_t *get_copy_of_container(
    container_t *c, uint8_t *typecode,
    bool copy_on_write
){
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
            // Shared containers are not cloneable. Are you mixing COW and non-COW bitmaps?
            return NULL;
        default:
            assert(false);
            roaring_unreachable;
            return NULL;
    }
}

container_t *shared_container_extract_copy(
    shared_container_t *sc, uint8_t *typecode
){
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

extern inline container_t *container_not(
        const container_t *c1, uint8_t type1,
        uint8_t *result_type);

extern inline container_t *container_not_range(
        const container_t *c1, uint8_t type1,
        uint32_t range_start, uint32_t range_end,
        uint8_t *result_type);

extern inline container_t *container_inot(
        container_t *c1, uint8_t type1,
        uint8_t *result_type);

extern inline container_t *container_inot_range(
        container_t *c1, uint8_t type1,
        uint32_t range_start, uint32_t range_end,
        uint8_t *result_type);

extern inline container_t *container_range_of_ones(
        uint32_t range_start, uint32_t range_end,
        uint8_t *result_type);

// where are the correponding things for union and intersection??
extern inline container_t *container_lazy_xor(
        const container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
        uint8_t *result_type);

extern inline container_t *container_lazy_ixor(
        container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
        uint8_t *result_type);

extern inline container_t *container_andnot(
        const container_t *c1, uint8_t type1,
        const container_t *c2, uint8_t type2,
        uint8_t *result_type);

#ifdef __cplusplus
} } }  // extern "C" { namespace roaring { namespace internal {
#endif
