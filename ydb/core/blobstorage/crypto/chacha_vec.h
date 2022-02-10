#pragma once

#include <util/system/types.h>

#if (__ARM_NEON__ || defined(_arm64_))
#   define CHACHA_GPR_TOO   1
#   define CHACHA_VBPI      2
#elif __ALTIVEC__
#   define CHACHA_GPR_TOO   1
#   define CHACHA_VBPI      3
#elif __SSE2__
#   define CHACHA_GPR_TOO   0
#   if __clang__
#       define CHACHA_VBPI  4
#   else
#       define CHACHA_VBPI  3
#   endif
#endif

// Blocks computed per loop iteration
#define CHACHA_BPI (CHACHA_VBPI + CHACHA_GPR_TOO)

// 8 (high speed), 20 (conservative), 12 (middle)
#define CHACHA_ROUNDS 8


// Architecture-neutral way to specify 16-byte vector of ints
using vec = ui32 __attribute__ ((vector_size (16)));

class ChaChaVec
{
public:
    using NonceType = ui64;
    static constexpr size_t KEY_SIZE = 32;
    static constexpr size_t BLOCK_SIZE = 64;

public:
    ChaChaVec(ui8 rounds = CHACHA_ROUNDS): rounds_(rounds) {}

    void SetKey(const ui8* key, size_t size);
    void SetIV(const ui8* iv, const ui8* blockIdx);
    void SetIV(const ui8* iv);
    void Encipher(const ui8* plaintext, ui8* ciphertext, size_t size);
    // Only for tests
    void EncipherOld(const ui8* plaintext, ui8* ciphertext, size_t size);
    void Decipher(const ui8* ciphertext, ui8* plaintext, size_t size);

    ~ChaChaVec();
private:
    template<bool Aligned>
    void EncipherImpl(const ui8* plaintext, ui8* ciphertext, size_t len);

    vec s0_, s1_, s2_, s3_;
    ui8 rounds_;
};
