#ifndef _MONT_H
#define _MONT_H

#include "common.h"

/*
 * How many numbers in Montgomery form a temporary scratchpad should contain.
 */
#define SCRATCHPAD_NR 7

typedef enum _ModulusType { ModulusGeneric, ModulusP256, ModulusP384, ModulusP521, ModulusEd448 } ModulusType;

typedef struct mont_context {
    ModulusType modulus_type;
    unsigned words;         /* Number of words allocated to hold the number */
    unsigned bytes;         /* Number of bytes allocated to hold the number */
    unsigned modulus_len;   /* Max bytes taken by an affine coordinate */
    uint64_t *modulus;
    uint64_t *one;
    uint64_t *r2_mod_n;     /* R^2 mod N */
    uint64_t m0;
    uint64_t *r_mod_n;      /* R mod N */
    uint64_t *modulus_min_2;
} MontContext;

int mont_context_init(MontContext **out, const uint8_t *modulus, size_t mod_len);
void mont_context_free(MontContext *ctx);
size_t mont_bytes(const MontContext *ctx);

int mont_new_number(uint64_t **out, unsigned count, const struct mont_context *ctx);
int mont_new_random_number(uint64_t **out, unsigned count, uint64_t seed, const struct mont_context *ctx);
int mont_new_from_bytes(uint64_t **out, const uint8_t *number, size_t len, const MontContext *ctx);
int mont_new_from_uint64(uint64_t **out, uint64_t x, const MontContext *ctx);

int mont_to_bytes(uint8_t *number, size_t len, const uint64_t* mont_number, const MontContext *ctx);
int mont_add(uint64_t* out, const uint64_t* a, const uint64_t* b, uint64_t *tmp, const MontContext *ctx);
int mont_mult(uint64_t* out, const uint64_t* a, const uint64_t *b, uint64_t *tmp, const MontContext *ctx);
int mont_shift_left(uint64_t* out, const uint64_t* a, uint64_t k, const MontContext *ctx);
int mont_sub(uint64_t *out, const uint64_t *a, const uint64_t *b, uint64_t *tmp, const MontContext *ctx);
int mont_inv_prime(uint64_t *out, uint64_t *a, const MontContext *ctx);
int mont_set(uint64_t *out, uint64_t x, const MontContext *ctx);

int mont_is_zero(const uint64_t *a, const MontContext *ctx);
int mont_is_one(const uint64_t *a, const MontContext *ctx);
int mont_is_equal(const uint64_t *a, const uint64_t *b, const MontContext *ctx);
int mont_copy(uint64_t *out, const uint64_t *a, const MontContext *ctx);

void mont_printf(const char *prefix, const uint64_t *mont_number, const MontContext *ctx);

#endif
