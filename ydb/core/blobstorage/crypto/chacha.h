#pragma once

#include <util/system/types.h>

#define CHACHA_MINKEYLEN  16
#define CHACHA_NONCELEN   8
#define CHACHA_CTRLEN     8
#define CHACHA_STATELEN   (CHACHA_NONCELEN + CHACHA_CTRLEN)
#define CHACHA_BLOCKLEN   64
#define CHACHA_ROUNDS     8


class ChaCha
{
public:
    using NonceType = ui64;
    static constexpr size_t KEY_SIZE = 32;
    static constexpr size_t BLOCK_SIZE = 64;

public:
    ChaCha(ui8 rounds = CHACHA_ROUNDS): rounds_(rounds) {}

    void SetKey(const ui8* key, size_t size);
    void SetIV(const ui8* iv, const ui8* blockIdx);
    void SetIV(const ui8* iv);
    void Encipher(const ui8* plaintext, ui8* ciphertext, size_t size);
    void Decipher(const ui8* ciphertext, ui8* plaintext, size_t size);

    ~ChaCha();

private:
    ui32 state_[16];
    ui8 rounds_;
};
