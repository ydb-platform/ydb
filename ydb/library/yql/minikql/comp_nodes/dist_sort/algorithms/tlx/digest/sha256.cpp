/*******************************************************************************
 * tlx/digest/sha256.cpp
 *
 * Public domain implementation of SHA-256 (SHA-2) processor. Copied from
 * https://github.com/kalven/sha-2, which is based on LibTomCrypt.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/digest/sha256.hpp>

#include <tlx/math/ror.hpp>
#include <tlx/string/hexdump.hpp>

#include <algorithm>
#include <cstdint>

namespace tlx {

/*
 * LibTomCrypt, modular cryptographic library -- Tom St Denis
 *
 * LibTomCrypt is a library that provides various cryptographic algorithms in a
 * highly modular and flexible manner.
 *
 * The library is free for all purposes without any express guarantee it works.
 */

typedef std::uint32_t u32;
typedef std::uint64_t u64;

namespace {

static const u32 K[64] = {
    0x428a2f98UL, 0x71374491UL, 0xb5c0fbcfUL, 0xe9b5dba5UL, 0x3956c25bUL,
    0x59f111f1UL, 0x923f82a4UL, 0xab1c5ed5UL, 0xd807aa98UL, 0x12835b01UL,
    0x243185beUL, 0x550c7dc3UL, 0x72be5d74UL, 0x80deb1feUL, 0x9bdc06a7UL,
    0xc19bf174UL, 0xe49b69c1UL, 0xefbe4786UL, 0x0fc19dc6UL, 0x240ca1ccUL,
    0x2de92c6fUL, 0x4a7484aaUL, 0x5cb0a9dcUL, 0x76f988daUL, 0x983e5152UL,
    0xa831c66dUL, 0xb00327c8UL, 0xbf597fc7UL, 0xc6e00bf3UL, 0xd5a79147UL,
    0x06ca6351UL, 0x14292967UL, 0x27b70a85UL, 0x2e1b2138UL, 0x4d2c6dfcUL,
    0x53380d13UL, 0x650a7354UL, 0x766a0abbUL, 0x81c2c92eUL, 0x92722c85UL,
    0xa2bfe8a1UL, 0xa81a664bUL, 0xc24b8b70UL, 0xc76c51a3UL, 0xd192e819UL,
    0xd6990624UL, 0xf40e3585UL, 0x106aa070UL, 0x19a4c116UL, 0x1e376c08UL,
    0x2748774cUL, 0x34b0bcb5UL, 0x391c0cb3UL, 0x4ed8aa4aUL, 0x5b9cca4fUL,
    0x682e6ff3UL, 0x748f82eeUL, 0x78a5636fUL, 0x84c87814UL, 0x8cc70208UL,
    0x90befffaUL, 0xa4506cebUL, 0xbef9a3f7UL, 0xc67178f2UL
};

static inline u32 min(u32 x, u32 y) {
    return x < y ? x : y;
}

static inline u32 load32(const std::uint8_t* y) {
    return (u32(y[0]) << 24) | (u32(y[1]) << 16) |
           (u32(y[2]) << 8) | (u32(y[3]) << 0);
}
static inline void store64(u64 x, std::uint8_t* y) {
    for (int i = 0; i != 8; ++i)
        y[i] = (x >> ((7 - i) * 8)) & 255;
}
static inline void store32(u32 x, std::uint8_t* y) {
    for (int i = 0; i != 4; ++i)
        y[i] = (x >> ((3 - i) * 8)) & 255;
}

static inline u32 Ch(u32 x, u32 y, u32 z) {
    return z ^ (x & (y ^ z));
}
static inline u32 Maj(u32 x, u32 y, u32 z) {
    return ((x | y) & z) | (x & y);
}
static inline u32 Sh(u32 x, u32 n) {
    return x >> n;
}
static inline u32 Sigma0(u32 x) {
    return ror32(x, 2) ^ ror32(x, 13) ^ ror32(x, 22);
}
static inline u32 Sigma1(u32 x) {
    return ror32(x, 6) ^ ror32(x, 11) ^ ror32(x, 25);
}
static inline u32 Gamma0(u32 x) {
    return ror32(x, 7) ^ ror32(x, 18) ^ Sh(x, 3);
}
static inline u32 Gamma1(u32 x) {
    return ror32(x, 17) ^ ror32(x, 19) ^ Sh(x, 10);
}

static void sha256_compress(std::uint32_t state[8], const std::uint8_t* buf) {
    u32 S[8], W[64], t0, t1, t;

    // Copy state into S
    for (size_t i = 0; i < 8; i++)
        S[i] = state[i];

    // Copy the state into 512-bits into W[0..15]
    for (size_t i = 0; i < 16; i++)
        W[i] = load32(buf + (4 * i));

    // Fill W[16..63]
    for (size_t i = 16; i < 64; i++)
        W[i] = Gamma1(W[i - 2]) + W[i - 7] + Gamma0(W[i - 15]) + W[i - 16];

    // Compress
    auto RND =
        [&](u32 a, u32 b, u32 c, u32& d, u32 e, u32 f, u32 g, u32& h, u32 i)
        {
            t0 = h + Sigma1(e) + Ch(e, f, g) + K[i] + W[i];
            t1 = Sigma0(a) + Maj(a, b, c);
            d += t0;
            h = t0 + t1;
        };

    for (size_t i = 0; i < 64; ++i)
    {
        RND(S[0], S[1], S[2], S[3], S[4], S[5], S[6], S[7], i);
        t = S[7], S[7] = S[6], S[6] = S[5], S[5] = S[4],
        S[4] = S[3], S[3] = S[2], S[2] = S[1], S[1] = S[0], S[0] = t;
    }

    // Feedback
    for (size_t i = 0; i < 8; i++)
        state[i] = state[i] + S[i];
}

} // namespace

SHA256::SHA256() {
    curlen_ = 0;
    length_ = 0;
    state_[0] = 0x6A09E667UL;
    state_[1] = 0xBB67AE85UL;
    state_[2] = 0x3C6EF372UL;
    state_[3] = 0xA54FF53AUL;
    state_[4] = 0x510E527FUL;
    state_[5] = 0x9B05688CUL;
    state_[6] = 0x1F83D9ABUL;
    state_[7] = 0x5BE0CD19UL;
}

SHA256::SHA256(const void* data, std::uint32_t size)
    : SHA256() {
    process(data, size);
}

SHA256::SHA256(const std::string& str)
    : SHA256() {
    process(str);
}

void SHA256::process(const void* data, u32 size) {
    const u32 block_size = sizeof(SHA256::buf_);
    auto in = static_cast<const std::uint8_t*>(data);

    while (size > 0)
    {
        if (curlen_ == 0 && size >= block_size)
        {
            sha256_compress(state_, in);
            length_ += block_size * 8;
            in += block_size;
            size -= block_size;
        }
        else
        {
            u32 n = min(size, (block_size - curlen_));
            std::copy(in, in + n, buf_ + curlen_);
            curlen_ += n;
            in += n;
            size -= n;

            if (curlen_ == block_size)
            {
                sha256_compress(state_, buf_);
                length_ += 8 * block_size;
                curlen_ = 0;
            }
        }
    }
}

void SHA256::process(const std::string& str) {
    return process(str.data(), str.size());
}

void SHA256::finalize(void* digest) {
    // Increase the length of the message
    length_ += curlen_ * 8;

    // Append the '1' bit
    buf_[curlen_++] = static_cast<std::uint8_t>(0x80);

    // If the length_ is currently above 56 bytes we append zeros then
    // sha256_compress().  Then we can fall back to padding zeros and length
    // encoding like normal.
    if (curlen_ > 56)
    {
        while (curlen_ < 64)
            buf_[curlen_++] = 0;
        sha256_compress(state_, buf_);
        curlen_ = 0;
    }

    // Pad up to 56 bytes of zeroes
    while (curlen_ < 56)
        buf_[curlen_++] = 0;

    // Store length
    store64(length_, buf_ + 56);
    sha256_compress(state_, buf_);

    // Copy output
    for (size_t i = 0; i < 8; i++)
        store32(state_[i], static_cast<std::uint8_t*>(digest) + (4 * i));
}

std::string SHA256::digest() {
    std::string out(kDigestLength, '0');
    finalize(const_cast<char*>(out.data()));
    return out;
}

std::string SHA256::digest_hex() {
    std::uint8_t digest[kDigestLength];
    finalize(digest);
    return hexdump_lc(digest, kDigestLength);
}

std::string SHA256::digest_hex_uc() {
    std::uint8_t digest[kDigestLength];
    finalize(digest);
    return hexdump(digest, kDigestLength);
}

std::string sha256_hex(const void* data, std::uint32_t size) {
    return SHA256(data, size).digest_hex();
}

std::string sha256_hex(const std::string& str) {
    return SHA256(str).digest_hex();
}

std::string sha256_hex_uc(const void* data, std::uint32_t size) {
    return SHA256(data, size).digest_hex_uc();
}

std::string sha256_hex_uc(const std::string& str) {
    return SHA256(str).digest_hex_uc();
}

} // namespace tlx

/******************************************************************************/
