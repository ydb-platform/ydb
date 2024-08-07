#pragma once

#include <iostream>
#include <fstream>
#include <bitset>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <vector>
#include <bit>

// ---------- COMPRESSION ------------------
class BitWriter {
public:
    explicit BitWriter(std::ostream &os) : out(&os), buffer(0), count(8) {}

    // Write a single bit at the available right-most position of the `buffer`.
    void writeBit(bool bit) {
        if (bit) {
            // 1. mask = 1 << (count - 1)
            // Shift binary representation of 1 to the left by (count - 1) positions
            // (create a mask with a single bit set at position (count - 1)).
            //
            // 2. buffer |= mask
            // Apply bitwise OR assignment operator.
            buffer |= (1 << (count - 1));
        }
        count--;

        // If `buffer` is filled, write it out and reinitialize.
        if (count == 0) {
            writeBuf();
            buffer = 0;
            count = 8;
        }
    }

    // Write the `nbits` right-most bits of `u64` to the `buffer` in left-to-right order.
    //
    // E.g., given:
    // * `u64`   = ...0001010101010_000111
    // * `nbits` = 6,
    // it will write `000111` to the buffer.
    void writeBits(uint64_t u64, int nbits) {
        // Left-shit `u64` leaving only `nbits` of meaningful bits (on leading positions).
        // Was:    ...0001010101010_000111
        // Becase: 000111...00000000000000
        u64 <<= (64 - nbits);
        while (nbits >= 8) {
            auto byte = static_cast<uint8_t>(u64 >> 56);
            writeByte(byte);
            u64 <<= 8;
            nbits -= 8;
        }

        while (nbits > 0) {
            bool bit = (u64 >> 63) == 1;
            writeBit(bit);

            u64 <<= 1;
            nbits--;
        }
    }

    // Write a single byte to the stream, regardless of alignment.
    void writeByte(uint8_t byte) {
        //            writing ->
        // buffer:         [xxx*****]
        //  x -- non-empty (3)
        //  * -- empty     (5 = count)
        // 1. Shift `byte` on the number of already taken positions of `buffer`
        //    (3 in this example).
        // Was:    11001100
        // Became: 00011001
        // 2. Write the mask to the `buffer`.
        // 3. Write out the `buffer`.
        // 4. Write the remaining (right-remaining) part of the `byte` to the `buffer`
        //    (00000100 in this example)
        buffer |= (byte >> (8 - count));
        writeBuf();
        buffer = byte << count;
    }

    // Empty the currently in-process `buffer` by filling it with 'bit'
    // (all unused right-most bits will be filled with `bit`).
    void flush(bool bit) {
        // `count` will become 8 after `buffer` is written out?
        while (count != 8) {
            writeBit(bit);
        }
    }

private:
    void writeBuf() {
        out->write(reinterpret_cast<const char *>(&buffer), sizeof(buffer));
    }

    std::ostream *out;
    uint8_t buffer;
    // How many right-most bits are available for writing in the current byte (the last byte of the buffer).
    uint8_t count;
};

constexpr int32_t
FIRST_DELTA_BITS = 14;

uint8_t leadingZeros(uint64_t v) {
    uint64_t mask = 0x8000000000000000;
    uint8_t ret = 0;
    while (ret < 64 && (v & mask) == 0) {
        mask >>= 1;
        ret++;
    }
    return ret;
}

uint8_t trailingZeros(uint64_t v) {
    uint64_t mask = 0x0000000000000001;
    uint8_t ret = 0;
    while (ret < 64 && (v & mask) == 0) {
        mask <<= 1;
        ret++;
    }
    return ret;
}

// Diff from initial article implementation:
// 1.) Leading zeroes are encoded and decoded as 6 bits and not as 5 (as it's done in the article).
// 2.) Max DOD encoded as 64 bits and not as 32.
class Compressor {
public:
    Compressor(std::ostream &os, uint64_t header) : bw(os), header_(header), leading_zeros_(INT8_MAX) {
        bw.writeBits(header_, 64);
    }

    void compress(uint64_t t, uint64_t v) {
        if (t_ == 0) {
            if (t - header_ < 0) {
                std::cerr << "First time passed for compression is less than header." << std::endl;
                std::cerr << "Header: " << header_ << ". Time: " << t << "." << std::endl;
                exit(0);
            }
            int64_t delta = static_cast<int64_t>(t) - static_cast<int64_t>(header_);
            t_ = t;
            t_delta_ = delta;
            value_ = v;

            bw.writeBits(delta, FIRST_DELTA_BITS);
            bw.writeBits(value_, 64);
        } else {
            compressTimestamp(t);
            compressValue(v);
        }
    }

    void finish() {
        if (t_ == 0) {
            bw.writeBits((1 << FIRST_DELTA_BITS) - 1, FIRST_DELTA_BITS);
            bw.writeBits(0, 64);
            bw.flush(false);
            return;
        }

        // 0x0F           = 00001111 -> 1111 (cutted).
        bw.writeBits(0x0F, 4);
        // 0xFFFFFFFF     = 11111111 11111111 11111111 11111111
        bw.writeBits(0xFFFFFFFFFFFFFFFF, 64);
        bw.writeBit(false);
        bw.flush(false);
    }

private:
    void compressTimestamp(uint64_t t) {
        auto delta = static_cast<int64_t>(t) - static_cast<int64_t>(t_);
        int64_t dod = delta - t_delta_;

        t_ = t;
        t_delta_ = delta;

        if (dod == 0) {
            bw.writeBit(false);
        } else if (-63 <= dod && dod <= 64) {
            bw.writeBits(0x02, 2);
            writeInt64Bits(dod, 7);
        } else if (-255 <= dod && dod <= 256) {
            bw.writeBits(0x06, 3);
            writeInt64Bits(dod, 9);
        } else if (-2047 <= dod && dod <= 2048) {
            bw.writeBits(0x0E, 4);
            writeInt64Bits(dod, 12);
        } else {
            bw.writeBits(0x0F, 4);
            writeInt64Bits(dod, 64);
        }
    }

    void writeInt64Bits(int64_t i, int nbits) {
        uint64_t u = 0;
        if (i >= 0 || nbits >= 64) {
            u = static_cast<uint64_t>(i);
        } else {
            u = static_cast<uint64_t>((1 << nbits) + i);
        }
        bw.writeBits(u, int(nbits));
    }

    void compressValue(uint64_t v) {
        uint64_t xor_val = value_ ^ v;
        value_ = v;

        if (xor_val == 0) {
            bw.writeBit(false);
            return;
        }

        uint8_t leading_zeros_val = leadingZeros(xor_val);
        uint8_t trailing_zeros_val = trailingZeros(xor_val);

        bw.writeBit(true);

        if (leading_zeros_ <= leading_zeros_val && trailing_zeros_ <= trailing_zeros_val) {
            bw.writeBit(false);
            int significant_bits = 64 - leading_zeros_ - trailing_zeros_;
            bw.writeBits(xor_val >> trailing_zeros_, significant_bits);
            return;
        }

        leading_zeros_ = leading_zeros_val;
        trailing_zeros_ = trailing_zeros_val;

        bw.writeBit(true);
        bw.writeBits(leading_zeros_, 6);
        int significant_bits = 64 - leading_zeros_ - trailing_zeros_;
        bw.writeBits(static_cast<uint64_t>(significant_bits), 6);
        bw.writeBits(xor_val >> trailing_zeros_val, significant_bits);
    }

    // Util to apply bit operations.
    BitWriter bw;
    // Header bits.
    uint64_t header_;
    // Last time passed for compression.
    uint64_t t_ = 0;
    // 1.) In case first (time, value) pair passed after header, find delta with header time.
    // 2.) Otherwise, last time delta with new passed time and `t_`.
    int64_t t_delta_ = 0;
    uint8_t leading_zeros_ = 0;
    uint8_t trailing_zeros_ = 0;
    // Last value passed for compression.
    uint64_t value_ = 0;
};
// ---------- COMPRESSION ------------------



// ---------- DECOMPRESSION ----------------
class BitReader {
public:
    explicit BitReader(std::istream &is) : in(is), buffer_(0), count_(0) {}

    // Read single bit from the stream.
    bool readBit() {
        if (count_ == 0) {
            refreshBuffer();
            count_ = 8;
        }
        count_--;

        // 1.) Bitwise AND (0x80 = 10000000)
        // 2.) Left shift buffer on 1 bit.
        // 3.) If digit == 1, return true, false -- otherwise.
        uint8_t digit = (buffer_ & 0x80);
        buffer_ <<= 1;
        bool res = digit != 0;
        return res;
    }

    // Read single byte from the stream.
    uint8_t readByte() {
        if (count_ == 0) {
            refreshBuffer();
            return buffer_;
        }
        uint8_t byte = buffer_;
        refreshBuffer();
        byte |= (buffer_ >> count_);
        buffer_ <<= (8 - count_);
        return byte;
    }

    // Read `nbits` bits from the stream.
    uint64_t readBits(int nbits) {
        uint64_t u64 = 0;

        while (nbits >= 8) {
            uint8_t byte = readByte();
            u64 = (u64 << 8) | static_cast<uint64_t>(byte);
            nbits -= 8;
        }

        while (nbits > 0) {
            uint8_t byte = readBit();
            u64 <<= 1;
            if (byte) {
                u64 |= 1;
            }
            nbits--;
        }

        return u64;
    }

private:
    // Read a new byte from the stream.
    void refreshBuffer() {
        char read_byte;
        in.read(&read_byte, 1);
        buffer_ = read_byte;
    }

    std::istream &in;
    uint8_t buffer_;
    // How many right-most bits are available for reading in the current byte.
    // Note: reading is applied from left to right.
    uint8_t count_;
};

class Decompressor {
public:
    explicit Decompressor(std::istream &is) : br(is) {
        header_ = br.readBits(64);
    }

    [[nodiscard]] uint64_t getHeader() const {
        return header_;
    }

    std::optional<std::pair<uint64_t, uint64_t>> next() {
        if (t_ == 0) {
            return {decompressFirst()};
        } else {
            return decompress();
        }
    }

private:
    [[nodiscard]] std::pair<uint64_t, uint64_t> decompressFirst() {
        uint64_t delta_u64 = br.readBits(FIRST_DELTA_BITS);
        int64_t delta = *reinterpret_cast<int64_t *>(&delta_u64);

        if (delta == ((1 << FIRST_DELTA_BITS) - 1)) {
            return std::make_pair(0, 0);
        }

        uint64_t value = br.readBits(64);
        t_delta_ = delta;
        t_ = header_ + t_delta_;
        value_ = value;

        return std::make_pair(t_, value_);
    }

    std::optional<std::pair<uint64_t, uint64_t>> decompress() {
        std::optional<uint64_t> t = decompressTimestamp();
        if (t) {
            uint64_t v = decompressValue();

            return {std::make_pair(*t, v)};
        }
        return std::nullopt;
    }

    std::optional<uint64_t> decompressTimestamp() {
        uint8_t n = dodTimestampBits();

        if (n == 0) {
            t_ += t_delta_;
            return t_;
        }

        uint64_t bits = br.readBits(n);

        if (n == 64 && bits == 0xFFFFFFFFFFFFFFFF) {
            return std::nullopt;
        }

        int64_t bits_int64 = *reinterpret_cast<int64_t *>(&bits);
        int64_t dod = bits_int64;
        if (n != 64 && (1 << (n - 1)) < bits_int64) {
            dod = bits_int64 - (1 << n);
        }

        t_delta_ += dod;
        t_ += t_delta_;
        return t_;
    }

    uint8_t dodTimestampBits() {
        uint8_t dod = 0;
        for (int i = 0; i < 4; i++) {
            dod <<= 1;
            bool bit = br.readBit();
            if (bit) {
                dod |= 1;
            } else {
                break;
            }
        }

        if (dod == 0x00) {
            // Case of dod == 0.
            return 0;
        } else if (dod == 0x02) {
            // Case of dod == 10.
            return 7;
        } else if (dod == 0x06) {
            return 9;
        } else if (dod == 0x0E) {
            return 12;
        } else if (dod == 0x0F) {
            return 64;
        } else {
            std::cerr << "invalid bit header for bit length to read" << std::endl;
            exit(1);
        }
    }

    uint64_t decompressValue() {
        uint8_t read = 0;
        for (int i = 0; i < 2; i++) {
            bool bit = br.readBit();
            if (bit) {
                read <<= 1;
                read++;
            } else {
                break;
            }
        }

        if (read == 0x1 || read == 0x3) {
            if (read == 0x3) {
                uint8_t leading_zeroes = br.readBits(6);
                uint8_t significant_bits = br.readBits(6);
                if (significant_bits == 0) {
                    significant_bits = 64;
                }
                leading_zeros_ = leading_zeroes;
                trailing_zeros_ = 64 - significant_bits - leading_zeros_;
            }

            uint64_t value_bits = br.readBits(64 - leading_zeros_ - trailing_zeros_);
            value_bits <<= trailing_zeros_;
            value_ ^= value_bits;
        }

        return value_;
    }

    BitReader br;
    uint64_t header_ = 0;
    uint64_t t_ = 0;
    int64_t t_delta_ = 0;
    uint8_t leading_zeros_ = 0;
    uint8_t trailing_zeros_ = 0;
    uint64_t value_ = 0;
};
// ---------- DECOMPRESSION ----------------