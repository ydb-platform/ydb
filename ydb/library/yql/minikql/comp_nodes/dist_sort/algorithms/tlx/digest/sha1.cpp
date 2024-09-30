/*******************************************************************************
 * tlx/digest/sha1.cpp
 *
 * Public domain implementation of SHA-1 processor. Based on LibTomCrypt from
 * https://github.com/libtom/libtomcrypt.git
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/digest/sha1.hpp>

#include <cstdint>
#include <tlx/math/rol.hpp>
#include <tlx/string/hexdump.hpp>

namespace tlx {

/*
 * LibTomCrypt, modular cryptographic library -- Tom St Denis
 *
 * LibTomCrypt is a library that provides various cryptographic algorithms in a
 * highly modular and flexible manner.
 *
 * The library is free for all purposes without any express guarantee it works.
 */

namespace digest_detail {

static inline std::uint32_t min(std::uint32_t x, std::uint32_t y) {
    return x < y ? x : y;
}

static inline void store64h(std::uint64_t x, unsigned char* y) {
    for (int i = 0; i != 8; ++i)
        y[i] = (x >> ((7 - i) * 8)) & 255;
}
static inline std::uint32_t load32h(const std::uint8_t* y) {
    return (std::uint32_t(y[0]) << 24) | (std::uint32_t(y[1]) << 16) |
           (std::uint32_t(y[2]) << 8) | (std::uint32_t(y[3]) << 0);
}
static inline void store32h(std::uint32_t x, std::uint8_t* y) {
    for (int i = 0; i != 4; ++i)
        y[i] = (x >> ((3 - i) * 8)) & 255;
}

static inline
std::uint32_t F0(const std::uint32_t& x, const std::uint32_t& y, const std::uint32_t& z) {
    return (z ^ (x & (y ^ z)));
}
static inline
std::uint32_t F1(const std::uint32_t& x, const std::uint32_t& y, const std::uint32_t& z) {
    return (x ^ y ^ z);
}
static inline
std::uint32_t F2(const std::uint32_t& x, const std::uint32_t& y, const std::uint32_t& z) {
    return ((x & y) | (z & (x | y)));
}
static inline
std::uint32_t F3(const std::uint32_t& x, const std::uint32_t& y, const std::uint32_t& z) {
    return (x ^ y ^ z);
}

static void sha1_compress(std::uint32_t state[4], const std::uint8_t* buf) {
    std::uint32_t a, b, c, d, e, W[80], i, t;

    /* copy the state into 512-bits into W[0..15] */
    for (i = 0; i < 16; i++) {
        W[i] = load32h(buf + (4 * i));
    }

    /* copy state */
    a = state[0];
    b = state[1];
    c = state[2];
    d = state[3];
    e = state[4];

    /* expand it */
    for (i = 16; i < 80; i++) {
        W[i] = rol32(W[i - 3] ^ W[i - 8] ^ W[i - 14] ^ W[i - 16], 1);
    }

    /* compress */
    for (i = 0; i < 20; ++i) {
        e = (rol32(a, 5) + F0(b, c, d) + e + W[i] + 0x5a827999UL);
        b = rol32(b, 30);
        t = e, e = d, d = c, c = b, b = a, a = t;
    }
    for ( ; i < 40; ++i) {
        e = (rol32(a, 5) + F1(b, c, d) + e + W[i] + 0x6ed9eba1UL);
        b = rol32(b, 30);
        t = e, e = d, d = c, c = b, b = a, a = t;
    }
    for ( ; i < 60; ++i) {
        e = (rol32(a, 5) + F2(b, c, d) + e + W[i] + 0x8f1bbcdcUL);
        b = rol32(b, 30);
        t = e, e = d, d = c, c = b, b = a, a = t;
    }
    for ( ; i < 80; ++i) {
        e = (rol32(a, 5) + F3(b, c, d) + e + W[i] + 0xca62c1d6UL);
        b = rol32(b, 30);
        t = e, e = d, d = c, c = b, b = a, a = t;
    }

    /* store */
    state[0] = state[0] + a;
    state[1] = state[1] + b;
    state[2] = state[2] + c;
    state[3] = state[3] + d;
    state[4] = state[4] + e;
}

} // namespace digest_detail

SHA1::SHA1() {
    curlen_ = 0;
    length_ = 0;
    state_[0] = 0x67452301UL;
    state_[1] = 0xefcdab89UL;
    state_[2] = 0x98badcfeUL;
    state_[3] = 0x10325476UL;
    state_[4] = 0xc3d2e1f0UL;
}

SHA1::SHA1(const void* data, std::uint32_t size) : SHA1() {
    process(data, size);
}

SHA1::SHA1(const std::string& str) : SHA1() {
    process(str);
}

void SHA1::process(const void* data, std::uint32_t size) {
    const std::uint32_t block_size = sizeof(SHA1::buf_);
    auto in = static_cast<const std::uint8_t*>(data);

    while (size > 0)
    {
        if (curlen_ == 0 && size >= block_size)
        {
            digest_detail::sha1_compress(state_, in);
            length_ += block_size * 8;
            in += block_size;
            size -= block_size;
        }
        else
        {
            std::uint32_t n = digest_detail::min(size, (block_size - curlen_));
            std::uint8_t* b = buf_ + curlen_;
            for (const std::uint8_t* a = in; a != in + n; ++a, ++b) {
                *b = *a;
            }
            curlen_ += n;
            in += n;
            size -= n;

            if (curlen_ == block_size)
            {
                digest_detail::sha1_compress(state_, buf_);
                length_ += 8 * block_size;
                curlen_ = 0;
            }
        }
    }
}

void SHA1::process(const std::string& str) {
    return process(str.data(), str.size());
}

void SHA1::finalize(void* digest) {
    // Increase the length of the message
    length_ += curlen_ * 8;

    // Append the '1' bit
    buf_[curlen_++] = static_cast<std::uint8_t>(0x80);

    // If the length_ is currently above 56 bytes we append zeros then
    // sha1_compress().  Then we can fall back to padding zeros and length
    // encoding like normal.
    if (curlen_ > 56) {
        while (curlen_ < 64)
            buf_[curlen_++] = 0;
        digest_detail::sha1_compress(state_, buf_);
        curlen_ = 0;
    }

    // Pad up to 56 bytes of zeroes
    while (curlen_ < 56)
        buf_[curlen_++] = 0;

    // Store length
    digest_detail::store64h(length_, buf_ + 56);
    digest_detail::sha1_compress(state_, buf_);

    // Copy output
    for (size_t i = 0; i < 5; i++)
        digest_detail::store32h(state_[i], static_cast<std::uint8_t*>(digest) + (4 * i));
}

std::string SHA1::digest() {
    std::string out(kDigestLength, '0');
    finalize(const_cast<char*>(out.data()));
    return out;
}

std::string SHA1::digest_hex() {
    std::uint8_t digest[kDigestLength];
    finalize(digest);
    return hexdump_lc(digest, kDigestLength);
}

std::string SHA1::digest_hex_uc() {
    std::uint8_t digest[kDigestLength];
    finalize(digest);
    return hexdump(digest, kDigestLength);
}

std::string sha1_hex(const void* data, std::uint32_t size) {
    return SHA1(data, size).digest_hex();
}

std::string sha1_hex(const std::string& str) {
    return SHA1(str).digest_hex();
}

std::string sha1_hex_uc(const void* data, std::uint32_t size) {
    return SHA1(data, size).digest_hex_uc();
}

std::string sha1_hex_uc(const std::string& str) {
    return SHA1(str).digest_hex_uc();
}

} // namespace tlx

/******************************************************************************/
