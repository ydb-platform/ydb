/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "opal/constants.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/output.h"

static void opal_pointer_array_construct(opal_pointer_array_t *);
static void opal_pointer_array_destruct(opal_pointer_array_t *);
static bool grow_table(opal_pointer_array_t *table, int at_least);

OBJ_CLASS_INSTANCE(opal_pointer_array_t, opal_object_t,
                   opal_pointer_array_construct,
                   opal_pointer_array_destruct);

/*
 * opal_pointer_array constructor
 */
static void opal_pointer_array_construct(opal_pointer_array_t *array)
{
    OBJ_CONSTRUCT(&array->lock, opal_mutex_t);
    array->lowest_free = 0;
    array->number_free = 0;
    array->size = 0;
    array->max_size = INT_MAX;
    array->block_size = 8;
    array->free_bits = NULL;
    array->addr = NULL;
}

/*
 * opal_pointer_array destructor
 */
static void opal_pointer_array_destruct(opal_pointer_array_t *array)
{
    /* free table */
    if( NULL != array->free_bits) {
        free(array->free_bits);
        array->free_bits = NULL;
    }
    if( NULL != array->addr ) {
        free(array->addr);
        array->addr = NULL;
    }

    array->size = 0;

    OBJ_DESTRUCT(&array->lock);
}

#define TYPE_ELEM_COUNT(TYPE, CAP) (((CAP) + 8 * sizeof(TYPE) - 1) / (8 * sizeof(TYPE)))

/**
 * Translate an index position into the free bits array into 2 values, the
 * index of the element and the index of the bit position.
 */
#define GET_BIT_POS(IDX, BIDX, PIDX)                                    \
    do {                                                                \
        uint32_t __idx = (uint32_t)(IDX);                               \
        (BIDX) = (__idx / (8 * sizeof(uint64_t)));                      \
        (PIDX) = (__idx % (8 * sizeof(uint64_t)));                      \
    } while(0)

/**
 * A classical find first zero bit (ffs) on a large array. It checks starting
 * from the indicated position until it finds a zero bit. If SET is true,
 * the bit is set. The position of the bit is returned in store.
 *
 * According to Section 6.4.4.1 of the C standard we don't need to prepend a type
 * indicator to constants (the type is inferred by the compiler according to
 * the number of bits necessary to represent it).
 */
#define FIND_FIRST_ZERO(START_IDX, STORE)                               \
    do {                                                                \
        uint32_t __b_idx, __b_pos;                                      \
        if( 0 == table->number_free ) {                                 \
            (STORE) = table->size;                                      \
            break;                                                      \
        }                                                               \
        GET_BIT_POS((START_IDX), __b_idx, __b_pos);                     \
        for (; table->free_bits[__b_idx] == 0xFFFFFFFFFFFFFFFFu; __b_idx++); \
        assert(__b_idx < (uint32_t)table->size);                        \
        uint64_t __check_value = table->free_bits[__b_idx];             \
        __b_pos = 0;                                                    \
                                                                        \
        if( 0x00000000FFFFFFFFu == (__check_value & 0x00000000FFFFFFFFu) ) { \
            __check_value >>= 32; __b_pos += 32;                        \
        }                                                               \
        if( 0x000000000000FFFFu == (__check_value & 0x000000000000FFFFu) ) { \
            __check_value >>= 16; __b_pos += 16;                        \
        }                                                               \
        if( 0x00000000000000FFu == (__check_value & 0x00000000000000FFu) ) { \
            __check_value >>= 8; __b_pos += 8;                          \
        }                                                               \
        if( 0x000000000000000Fu == (__check_value & 0x000000000000000Fu) ) { \
            __check_value >>= 4; __b_pos += 4;                          \
        }                                                               \
        if( 0x0000000000000003u == (__check_value & 0x0000000000000003u) ) { \
            __check_value >>= 2; __b_pos += 2;                          \
        }                                                               \
        if( 0x0000000000000001u == (__check_value & 0x0000000000000001u) ) { \
            __b_pos += 1;                                               \
        }                                                               \
        (STORE) = (__b_idx * 8 * sizeof(uint64_t)) + __b_pos;           \
    } while(0)

/**
 * Set the IDX bit in the free_bits array. The bit should be previously unset.
 */
#define SET_BIT(IDX)                                                    \
    do {                                                                \
        uint32_t __b_idx, __b_pos;                                      \
        GET_BIT_POS((IDX), __b_idx, __b_pos);                           \
        assert( 0 == (table->free_bits[__b_idx] & (((uint64_t)1) << __b_pos))); \
        table->free_bits[__b_idx] |= (((uint64_t)1) << __b_pos);        \
    } while(0)

/**
 * Unset the IDX bit in the free_bits array. The bit should be previously set.
 */
#define UNSET_BIT(IDX)                                                  \
    do {                                                                \
        uint32_t __b_idx, __b_pos;                                      \
        GET_BIT_POS((IDX), __b_idx, __b_pos);                           \
        assert( (table->free_bits[__b_idx] & (((uint64_t)1) << __b_pos))); \
        table->free_bits[__b_idx] ^= (((uint64_t)1) << __b_pos);        \
    } while(0)

#if 0
/**
 * Validate the pointer array by making sure that the elements and
 * the free bits array are in sync. It also check that the number
 * of remaining free element is consistent.
 */
static void opal_pointer_array_validate(opal_pointer_array_t *array)
{
    int i, cnt = 0;
    uint32_t b_idx, p_idx;

    for( i = 0; i < array->size; i++ ) {
        GET_BIT_POS(i, b_idx, p_idx);
        if( NULL == array->addr[i] ) {
            cnt++;
            assert( 0 == (array->free_bits[b_idx] & (((uint64_t)1) << p_idx)) );
        } else {
            assert( 0 != (array->free_bits[b_idx] & (((uint64_t)1) << p_idx)) );
        }
    }
    assert(cnt == array->number_free);
}
#endif

/**
 * initialize an array object
 */
int opal_pointer_array_init(opal_pointer_array_t* array,
                            int initial_allocation,
                            int max_size, int block_size)
{
    size_t num_bytes;

    /* check for errors */
    if (NULL == array || max_size < block_size) {
        return OPAL_ERR_BAD_PARAM;
    }

    array->max_size = max_size;
    array->block_size = (0 == block_size ? 8 : block_size);
    array->lowest_free = 0;

    num_bytes = (0 < initial_allocation ? initial_allocation : block_size);

    /* Allocate and set the array to NULL */
    array->addr = (void **)calloc(num_bytes, sizeof(void*));
    if (NULL == array->addr) { /* out of memory */
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    array->free_bits = (uint64_t*)calloc(TYPE_ELEM_COUNT(uint64_t, num_bytes), sizeof(uint64_t));
    if (NULL == array->free_bits) {  /* out of memory */
        free(array->addr);
        array->addr = NULL;
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    array->number_free = num_bytes;
    array->size = num_bytes;

    return OPAL_SUCCESS;
}

/**
 * add a pointer to dynamic pointer table
 *
 * @param table Pointer to opal_pointer_array_t object (IN)
 * @param ptr Pointer to be added to table    (IN)
 *
 * @return Array index where ptr is inserted or OPAL_ERROR if it fails
 */
int opal_pointer_array_add(opal_pointer_array_t *table, void *ptr)
{
    int index = table->size + 1;

    OPAL_THREAD_LOCK(&(table->lock));

    if (table->number_free == 0) {
        /* need to grow table */
        if (!grow_table(table, index) ) {
            OPAL_THREAD_UNLOCK(&(table->lock));
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
    }

    assert( (table->addr != NULL) && (table->size > 0) );
    assert( (table->lowest_free >= 0) && (table->lowest_free < table->size) );
    assert( (table->number_free > 0) && (table->number_free <= table->size) );

    /*
     * add pointer to table, and return the index
     */

    index = table->lowest_free;
    assert(NULL == table->addr[index]);
    table->addr[index] = ptr;
    table->number_free--;
    SET_BIT(index);
    if (table->number_free > 0) {
        FIND_FIRST_ZERO(index, table->lowest_free);
    } else {
        table->lowest_free = table->size;
    }

#if 0
    opal_pointer_array_validate(table);
#endif
    OPAL_THREAD_UNLOCK(&(table->lock));
    return index;
}

/**
 * Set the value of the dynamic array at a specified location.
 *
 *
 * @param table Pointer to opal_pointer_array_t object (IN)
 * @param ptr Pointer to be added to table    (IN)
 *
 * @return Error code
 *
 * Assumption: NULL element is free element.
 */
int opal_pointer_array_set_item(opal_pointer_array_t *table, int index,
                                void * value)
{
    assert(table != NULL);

    if (OPAL_UNLIKELY(0 > index)) {
        return OPAL_ERROR;
    }

    /* expand table if required to set a specific index */

    OPAL_THREAD_LOCK(&(table->lock));
    if (table->size <= index) {
        if (!grow_table(table, index)) {
            OPAL_THREAD_UNLOCK(&(table->lock));
            return OPAL_ERROR;
        }
    }
    assert(table->size > index);
    /* mark element as free, if NULL element */
    if( NULL == value ) {
        if( NULL != table->addr[index] ) {
            if (index < table->lowest_free) {
                table->lowest_free = index;
            }
            table->number_free++;
            UNSET_BIT(index);
        }
    } else {
        if (NULL == table->addr[index]) {
            table->number_free--;
            SET_BIT(index);
            /* Reset lowest_free if required */
            if ( index == table->lowest_free ) {
                FIND_FIRST_ZERO(index, table->lowest_free);
            }
        } else {
            assert( index != table->lowest_free );
        }
    }
    table->addr[index] = value;

#if 0
    opal_pointer_array_validate(table);
    opal_output(0,"opal_pointer_array_set_item: OUT: "
                " table %p (size %ld, lowest free %ld, number free %ld)"
                " addr[%d] = %p\n",
                table, table->size, table->lowest_free, table->number_free,
                index, table->addr[index]);
#endif

    OPAL_THREAD_UNLOCK(&(table->lock));
    return OPAL_SUCCESS;
}

/**
 * Test whether a certain element is already in use. If not yet
 * in use, reserve it.
 *
 * @param array Pointer to array (IN)
 * @param index Index of element to be tested (IN)
 * @param value New value to be set at element index (IN)
 *
 * @return true/false True if element could be reserved
 *                    False if element could not be reserved (e.g.in use).
 *
 * In contrary to array_set, this function does not allow to overwrite
 * a value, unless the previous value is NULL ( equiv. to free ).
 */
bool opal_pointer_array_test_and_set_item (opal_pointer_array_t *table,
                                           int index, void *value)
{
    assert(table != NULL);
    assert(index >= 0);

#if 0
    opal_output(0,"opal_pointer_array_test_and_set_item: IN:  "
               " table %p (size %ld, lowest free %ld, number free %ld)"
               " addr[%d] = %p\n",
               table, table->size, table->lowest_free, table->number_free,
               index, table->addr[index]);
#endif

    /* expand table if required to set a specific index */
    OPAL_THREAD_LOCK(&(table->lock));
    if ( index < table->size && table->addr[index] != NULL ) {
        /* This element is already in use */
        OPAL_THREAD_UNLOCK(&(table->lock));
        return false;
    }

    /* Do we need to grow the table? */

    if (table->size <= index) {
        if (!grow_table(table, index)) {
            OPAL_THREAD_UNLOCK(&(table->lock));
            return false;
        }
    }

    /*
     * allow a specific index to be changed.
     */
    assert(NULL == table->addr[index]);
    table->addr[index] = value;
    table->number_free--;
    SET_BIT(index);
    /* Reset lowest_free if required */
    if( table->number_free > 0 ) {
        if ( index == table->lowest_free ) {
            FIND_FIRST_ZERO(index, table->lowest_free);
        }
    } else {
        table->lowest_free = table->size;
    }

#if 0
    opal_pointer_array_validate(table);
    opal_output(0,"opal_pointer_array_test_and_set_item: OUT: "
               " table %p (size %ld, lowest free %ld, number free %ld)"
               " addr[%d] = %p\n",
               table, table->size, table->lowest_free, table->number_free,
               index, table->addr[index]);
#endif

    OPAL_THREAD_UNLOCK(&(table->lock));
    return true;
}

int opal_pointer_array_set_size(opal_pointer_array_t *array, int new_size)
{
    OPAL_THREAD_LOCK(&(array->lock));
    if(new_size > array->size) {
        if (!grow_table(array, new_size)) {
            OPAL_THREAD_UNLOCK(&(array->lock));
            return OPAL_ERROR;
        }
    }
    OPAL_THREAD_UNLOCK(&(array->lock));
    return OPAL_SUCCESS;
}

static bool grow_table(opal_pointer_array_t *table, int at_least)
{
    int i, new_size, new_size_int;
    void *p;

    new_size = table->block_size * ((at_least + 1 + table->block_size - 1) / table->block_size);
    if( new_size >= table->max_size ) {
        new_size = table->max_size;
        if( at_least >= table->max_size ) {
            return false;
        }
    }

    p = (void **) realloc(table->addr, new_size * sizeof(void *));
    if (NULL == p) {
        return false;
    }

    table->number_free += (new_size - table->size);
    table->addr = (void**)p;
    for (i = table->size; i < new_size; ++i) {
        table->addr[i] = NULL;
    }
    new_size_int = TYPE_ELEM_COUNT(uint64_t, new_size);
    if( (int)(TYPE_ELEM_COUNT(uint64_t, table->size)) != new_size_int ) {
        p = (uint64_t*)realloc(table->free_bits, new_size_int * sizeof(uint64_t));
        if (NULL == p) {
            return false;
        }
        table->free_bits = (uint64_t*)p;
        for (i = TYPE_ELEM_COUNT(uint64_t, table->size);
             i < new_size_int; i++ ) {
            table->free_bits[i] = 0;
        }
    }
    table->size = new_size;
#if 0
    opal_output(0, "grow_table %p to %d (max_size %d, block %d, number_free %d)\n",
                (void*)table, table->size, table->max_size, table->block_size, table->number_free);
#endif
    return true;
}
