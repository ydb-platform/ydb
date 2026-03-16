/*
 * Copyright (c) 2014      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <string.h>

#include "alfg.h"

/* Mask corresponding to the primitive polynomial
 *---------------------------------------------------
 *
 * p(x) = 1 + x^25 + x^27 + x^29 + x^30 + x^31 + x^32
 *
 *---------------------------------------------------
 */
#define MASK 0x80000057U

/* Additive lagged Fibonacci parameters:
 *---------------------------------------------------
 *
 * x_n = (x_(n - TAP1) + x_(n - TAP2) ) mod M
 *
 *---------------------------------------------------
 */
#define TAP1 127
#define TAP2 97
#define CBIT 21  /* Canonical bit */


/**
 * @brief      Galois shift register: Used to seed the ALFG's
 *             canonical rectangle
 *
 * @param[in]  unsigned int *seed: used to seed the Galois register
 * @param[out] uint32_t lsb: least significant bit of the Galois
 *             register after shift
 */
static uint32_t galois(unsigned int *seed){

    uint32_t lsb;
    lsb = (*seed & 1) ? 1 : 0;
    *seed >>= 1;
    /* tap it with the mask */
    *seed = *seed ^ (lsb*MASK);

    return lsb;
}

/* PMIX global rng buffer */
static pmix_rng_buff_t alfg_buffer;

/**
 * @brief   Routine to seed the ALFG register
 *
 * @param[in]   uint32_t seed
 * @param[out]  pmix_rng_buff_t *buff: handle to ALFG buffer state
 */
int pmix_srand(pmix_rng_buff_t *buff, uint32_t seed) {

    int i, j;
    uint32_t seed_cpy = seed;
    buff->tap1 = TAP1 - 1;
    buff->tap2 = TAP2 - 1;

    /* zero out the register */
    for( i = 0; i < TAP1; i++){
        buff->alfg[i] = 0;
    }
    /* set the canonical bit */
    buff->alfg[CBIT] = 1;

    /* seed the ALFG register by blasting
     * the canonical rectangle with bits
    */
    for ( j = 1; j < TAP1; j++){
        for( i = 1; i < 32; i++){
            buff->alfg[j] = buff->alfg[j] ^ ((galois(&seed_cpy))<<i);
        }
    }
    /* copy the ALFG to the global buffer */
    memcpy(&alfg_buffer, buff, sizeof(alfg_buffer));

    return 1;

}

/**
 * @brief       The additive lagged Fibonnaci PRNG
 *
 * @param[in]   pmix_rng_buff_t *buff: handle to ALFG buffer state
 * @param[out]  32-bit unsigned random integer
 */

uint32_t pmix_rand(pmix_rng_buff_t *buff){

    int *tap1 = &(buff->tap1);
    int *tap2 = &(buff->tap2);
    uint64_t overflow;
    uint32_t temp;

    /* prevent overflow */
    overflow = (uint64_t) buff->alfg[*tap1] + (uint64_t) buff->alfg[*tap2];
    /* circular buffer arithmetic */
    temp = (*tap1 + 1) == TAP1 ?  0 :  (*tap1 + 1);
    /* Division modulo 2^32 */
    buff->alfg[temp] = (uint32_t) ( overflow & ((1ULL<<32) -1));

    /* increment tap points */
    *tap1 = (*tap1 + 1)%TAP1;
    *tap2 = (*tap2 + 1)%TAP1;

    return buff->alfg[temp];

}

/**
 * @brief      A wrapper for pmix_rand() with our global ALFG buffer;
 *
 * @param[in]  none
 * @param[out] int, the same as normal rand(3)
 */
int pmix_random(void){
    /* always return a positive int */
    return (int)(pmix_rand(&alfg_buffer) & 0x7FFFFFFF);
}
