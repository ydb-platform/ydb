#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace NKikimr::NArrow::NSerialization::NGorilla {
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
}