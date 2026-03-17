#ifndef _BLOCK_BASE
#define _BLOCK_BASE

#include "common.h"

struct _BlockBase;

typedef int (*CipherOperation)(const struct _BlockBase *state, const uint8_t *in, uint8_t *out, size_t data_len);

typedef struct _BlockBase {
    CipherOperation encrypt;
    CipherOperation decrypt;
    int (*destructor)(struct _BlockBase *state);
    size_t block_len;
} BlockBase;

struct block_state;

#ifdef MODULE_NAME

#ifndef NON_STANDARD_START_OPERATION
static int block_init(struct block_state *state, const uint8_t *key, size_t keylen);
#endif

#ifndef NON_STANDARD_ENCRYPT_OPERATION
static void block_encrypt(struct block_state *self, const uint8_t *in, uint8_t *out);
#endif

static void block_decrypt(struct block_state *self, const uint8_t *in, uint8_t *out);
static void block_finalize(struct block_state* self);

#endif

#endif
