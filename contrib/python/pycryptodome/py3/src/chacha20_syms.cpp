#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int chacha20_init(void **pState,
                  const uint8_t *key,
                  size_t keySize,
                  const uint8_t *nonce,
                  size_t nonceSize);

int chacha20_destroy(void *state);

int chacha20_encrypt(void *state,
                     const uint8_t in[],
                     uint8_t out[],
                     size_t len);

int chacha20_seek(void *state,
                  unsigned long block_high,
                  unsigned long block_low,
                  unsigned offset);
int hchacha20(  const uint8_t key[32],
                const uint8_t nonce16[16],
                uint8_t subkey[32]);
}

BEGIN_SYMS("Crypto.Cipher._chacha20")
SYM(chacha20_init)
SYM(chacha20_destroy)
SYM(chacha20_encrypt)
SYM(chacha20_seek)
SYM(hchacha20)
END_SYMS()
