#pragma once

#include <iostream>
#include <fstream>
#include <bitset>

namespace NKikimr::NArrow::NSerialization::NGorilla {
    class BitWriter {
    public:
        explicit BitWriter(std::ostream &os) : out(os), buffer(0), count(8) {}

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
            auto casted_buffer = reinterpret_cast<const char *>(&buffer);
            out.write(casted_buffer, sizeof(buffer));
        }

        std::ostream &out;
        uint8_t buffer;
        // How many right-most bits are available for writing in the current byte (the last byte of the buffer).
        uint8_t count;
    };
}