/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Erik Moqvist
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <string.h>
#include "bitstream.h"

void bitstream_writer_init(struct bitstream_writer_t *self_p,
                           uint8_t *buf_p)
{
    self_p->buf_p = buf_p;
    self_p->byte_offset = 0;
    self_p->bit_offset = 0;
}

int bitstream_writer_size_in_bits(struct bitstream_writer_t *self_p)
{
    return (8 * self_p->byte_offset + self_p->bit_offset);
}

int bitstream_writer_size_in_bytes(struct bitstream_writer_t *self_p)
{
    return (self_p->byte_offset + (self_p->bit_offset + 7) / 8);
}

void bitstream_writer_write_bit(struct bitstream_writer_t *self_p,
                                int value)
{
    if (self_p->bit_offset == 0) {
        self_p->buf_p[self_p->byte_offset] = (value << 7);
        self_p->bit_offset = 1;
    } else {
        self_p->buf_p[self_p->byte_offset] |= (value << (8 - self_p->bit_offset - 1));

        if (self_p->bit_offset == 7) {
            self_p->bit_offset = 0;
            self_p->byte_offset++;
        } else {
            self_p->bit_offset++;
        }
    }
}

void bitstream_writer_write_bytes(struct bitstream_writer_t *self_p,
                                  const uint8_t *buf_p,
                                  int length)
{
    int i;
    uint8_t *dst_p;

    dst_p = &self_p->buf_p[self_p->byte_offset];

    if (self_p->bit_offset == 0) {
        memcpy(dst_p, buf_p, sizeof(uint8_t) * length);
    } else {
        for (i = 0; i < length; i++) {
            dst_p[i] |= (buf_p[i] >> self_p->bit_offset);
            dst_p[i + 1] = (uint8_t)(buf_p[i] << (8 - self_p->bit_offset));
        }
    }

    self_p->byte_offset += length;
}

void bitstream_writer_write_u8(struct bitstream_writer_t *self_p,
                               uint8_t value)
{
    if (self_p->bit_offset == 0) {
        self_p->buf_p[self_p->byte_offset] = value;
    } else {
        self_p->buf_p[self_p->byte_offset] |= (value >> self_p->bit_offset);
        self_p->buf_p[self_p->byte_offset + 1] =
            (uint8_t)(value << (8 - self_p->bit_offset));
    }

    self_p->byte_offset++;
}

void bitstream_writer_write_u16(struct bitstream_writer_t *self_p,
                                uint16_t value)
{
    if (self_p->bit_offset == 0) {
        self_p->buf_p[self_p->byte_offset] = (value >> 8);
    } else {
        self_p->buf_p[self_p->byte_offset] |= (value >> (8 + self_p->bit_offset));
        self_p->buf_p[self_p->byte_offset + 2] =
            (uint8_t)(value << (8 - self_p->bit_offset));
        value >>= self_p->bit_offset;
    }

    self_p->buf_p[self_p->byte_offset + 1] = (uint8_t)value;
    self_p->byte_offset += 2;
}

void bitstream_writer_write_u32(struct bitstream_writer_t *self_p,
                                uint32_t value)
{
    int i;

    if (self_p->bit_offset == 0) {
        self_p->buf_p[self_p->byte_offset] = (value >> 24);
    } else {
        self_p->buf_p[self_p->byte_offset] |= (value >> (24 + self_p->bit_offset));
        self_p->buf_p[self_p->byte_offset + 4] =
            (uint8_t)(value << (8 - self_p->bit_offset));
        value >>= self_p->bit_offset;
    }

    for (i = 3; i > 0; i--) {
        self_p->buf_p[self_p->byte_offset + i] = value;
        value >>= 8;
    }

    self_p->byte_offset += 4;
}

void bitstream_writer_write_u64(struct bitstream_writer_t *self_p,
                                uint64_t value)
{
    int i;


    if (self_p->bit_offset == 0) {
        self_p->buf_p[self_p->byte_offset] = (value >> 56);
    } else {
        self_p->buf_p[self_p->byte_offset] |= (value >> (56 + self_p->bit_offset));
        self_p->buf_p[self_p->byte_offset + 8] =
            (uint8_t)(value << (8 - self_p->bit_offset));
        value >>= self_p->bit_offset;
    }

    for (i = 7; i > 0; i--) {
        self_p->buf_p[self_p->byte_offset + i] = (uint8_t)value;
        value >>= 8;
    }

    self_p->byte_offset += 8;
}

void bitstream_writer_write_u64_bits(struct bitstream_writer_t *self_p,
                                     uint64_t value,
                                     int number_of_bits)
{
    int i;
    int first_byte_bits;
    int last_byte_bits;
    int full_bytes;

    if (number_of_bits == 0) {
        return;
    }

    /* Align beginning. */
    first_byte_bits = (8 - self_p->bit_offset);

    if (first_byte_bits != 8) {
        if (number_of_bits < first_byte_bits) {
            self_p->buf_p[self_p->byte_offset] |=
                (uint8_t)(value << (first_byte_bits - number_of_bits));
            self_p->bit_offset += number_of_bits;
        } else {
            self_p->buf_p[self_p->byte_offset] |= (value >> (number_of_bits
                                                             - first_byte_bits));
            self_p->byte_offset++;
            self_p->bit_offset = 0;
        }

        number_of_bits -= first_byte_bits;

        if (number_of_bits <= 0) {
            return;
        }
    }

    /* Align end. */
    last_byte_bits = (number_of_bits % 8);
    full_bytes = (number_of_bits / 8);

    if (last_byte_bits != 0) {
        self_p->buf_p[self_p->byte_offset + full_bytes] =
            (uint8_t)(value << (8 - last_byte_bits));
        value >>= last_byte_bits;
        self_p->bit_offset = last_byte_bits;
    }

    /* Copy middle bytes. */
    for (i = full_bytes; i > 0; i--) {
        self_p->buf_p[self_p->byte_offset + i - 1] = (uint8_t)value;
        value >>= 8;
    }

    self_p->byte_offset += full_bytes;
}

void bitstream_writer_write_repeated_bit(struct bitstream_writer_t *self_p,
                                         int value,
                                         int length)
{
    int rest;

    if (value != 0) {
        value = 0xff;
    }

    rest = (length % 8);
    bitstream_writer_write_u64_bits(self_p, value & ((1 << rest) - 1), rest);
    bitstream_writer_write_repeated_u8(self_p, value, length / 8);
}

void bitstream_writer_write_repeated_u8(struct bitstream_writer_t *self_p,
                                        uint8_t value,
                                        int length)
{
    int i;

    for (i = 0; i < length; i++) {
        bitstream_writer_write_u8(self_p, value);
    }
}

void bitstream_writer_insert_bit(struct bitstream_writer_t *self_p,
                                 int value)
{
    struct bitstream_writer_bounds_t bounds;

    bitstream_writer_bounds_save(&bounds,
                                 self_p,
                                 (8 * self_p->byte_offset) + self_p->bit_offset,
                                 1);
    bitstream_writer_write_bit(self_p, value);
    bitstream_writer_bounds_restore(&bounds);
}

void bitstream_writer_insert_bytes(struct bitstream_writer_t *self_p,
                                   const uint8_t *buf_p,
                                   int length)
{
    struct bitstream_writer_bounds_t bounds;

    bitstream_writer_bounds_save(&bounds,
                                 self_p,
                                 (8 * self_p->byte_offset) + self_p->bit_offset,
                                 8 * length);
    bitstream_writer_write_bytes(self_p, buf_p, length);
    bitstream_writer_bounds_restore(&bounds);
}

void bitstream_writer_insert_u8(struct bitstream_writer_t *self_p,
                                uint8_t value)
{
    struct bitstream_writer_bounds_t bounds;

    bitstream_writer_bounds_save(&bounds,
                                 self_p,
                                 (8 * self_p->byte_offset) + self_p->bit_offset,
                                 8);
    bitstream_writer_write_u8(self_p, value);
    bitstream_writer_bounds_restore(&bounds);
}

void bitstream_writer_insert_u16(struct bitstream_writer_t *self_p,
                                 uint16_t value)
{
    struct bitstream_writer_bounds_t bounds;

    bitstream_writer_bounds_save(&bounds,
                                 self_p,
                                 (8 * self_p->byte_offset) + self_p->bit_offset,
                                 16);
    bitstream_writer_write_u16(self_p, value);
    bitstream_writer_bounds_restore(&bounds);
}

void bitstream_writer_insert_u32(struct bitstream_writer_t *self_p,
                                 uint32_t value)
{
    struct bitstream_writer_bounds_t bounds;

    bitstream_writer_bounds_save(&bounds,
                                 self_p,
                                 (8 * self_p->byte_offset) + self_p->bit_offset,
                                 32);
    bitstream_writer_write_u32(self_p, value);
    bitstream_writer_bounds_restore(&bounds);
}

void bitstream_writer_insert_u64(struct bitstream_writer_t *self_p,
                                 uint64_t value)
{
    struct bitstream_writer_bounds_t bounds;

    bitstream_writer_bounds_save(&bounds,
                                 self_p,
                                 (8 * self_p->byte_offset) + self_p->bit_offset,
                                 64);
    bitstream_writer_write_u64(self_p, value);
    bitstream_writer_bounds_restore(&bounds);
}

void bitstream_writer_insert_u64_bits(struct bitstream_writer_t *self_p,
                                      uint64_t value,
                                      int number_of_bits)
{
    struct bitstream_writer_bounds_t bounds;

    bitstream_writer_bounds_save(&bounds,
                                 self_p,
                                 (8 * self_p->byte_offset) + self_p->bit_offset,
                                 number_of_bits);
    bitstream_writer_write_u64_bits(self_p, value, number_of_bits);
    bitstream_writer_bounds_restore(&bounds);
}

void bitstream_writer_seek(struct bitstream_writer_t *self_p,
                           int offset)
{
    offset += ((8 * self_p->byte_offset) + self_p->bit_offset);
    self_p->byte_offset = (offset / 8);
    self_p->bit_offset = (offset % 8);
}

void bitstream_writer_bounds_save(struct bitstream_writer_bounds_t *self_p,
                                  struct bitstream_writer_t *writer_p,
                                  int bit_offset,
                                  int length)
{
    int number_of_bits;

    self_p->writer_p = writer_p;
    number_of_bits = (bit_offset % 8);

    if (number_of_bits == 0) {
        self_p->first_byte_offset = -1;
    } else {
        self_p->first_byte_offset = (bit_offset / 8);
        self_p->first_byte = writer_p->buf_p[self_p->first_byte_offset];
        self_p->first_byte &= (0xff00 >> number_of_bits);
    }

    number_of_bits = ((bit_offset + length) % 8);

    if (number_of_bits == 0) {
        self_p->last_byte_offset = -1;
    } else {
        self_p->last_byte_offset = ((bit_offset + length) / 8);
        self_p->last_byte = writer_p->buf_p[self_p->last_byte_offset];
        self_p->last_byte &= ~(0xff00 >> number_of_bits);
        writer_p->buf_p[self_p->last_byte_offset] = 0;
    }

    if (self_p->first_byte_offset != -1) {
        writer_p->buf_p[self_p->first_byte_offset] = 0;
    }
}

void bitstream_writer_bounds_restore(struct bitstream_writer_bounds_t *self_p)
{
    if (self_p->first_byte_offset != -1) {
        self_p->writer_p->buf_p[self_p->first_byte_offset] |= self_p->first_byte;
    }

    if (self_p->last_byte_offset != -1) {
        self_p->writer_p->buf_p[self_p->last_byte_offset] |= self_p->last_byte;
    }
}

void bitstream_reader_init(struct bitstream_reader_t *self_p,
                           const uint8_t *buf_p)
{
    self_p->buf_p = buf_p;
    self_p->byte_offset = 0;
    self_p->bit_offset = 0;
}

int bitstream_reader_read_bit(struct bitstream_reader_t *self_p)
{
    int value;

    if (self_p->bit_offset == 0) {
        value = (self_p->buf_p[self_p->byte_offset] >> 7);
        self_p->bit_offset = 1;
    } else {
        value = ((self_p->buf_p[self_p->byte_offset] >> (7 - self_p->bit_offset)) & 0x1);

        if (self_p->bit_offset == 7) {
            self_p->bit_offset = 0;
            self_p->byte_offset++;
        } else {
            self_p->bit_offset++;
        }
    }

    return (value);
}

void bitstream_reader_read_bytes(struct bitstream_reader_t *self_p,
                                 uint8_t *buf_p,
                                 int length)
{
    int i;
    const uint8_t *src_p;

    src_p = &self_p->buf_p[self_p->byte_offset];

    if (self_p->bit_offset == 0) {
        memcpy(buf_p, src_p, sizeof(uint8_t) * length);
    } else {
        for (i = 0; i < length; i++) {
            buf_p[i] = (src_p[i] << self_p->bit_offset);
            buf_p[i] |= (src_p[i + 1] >> (8 - self_p->bit_offset));
        }
    }

    self_p->byte_offset += length;
}

uint8_t bitstream_reader_read_u8(struct bitstream_reader_t *self_p)
{
    uint8_t value;

    value = (self_p->buf_p[self_p->byte_offset] << self_p->bit_offset);
    self_p->byte_offset++;

    if (self_p->bit_offset != 0) {
        value |= (self_p->buf_p[self_p->byte_offset] >> (8 - self_p->bit_offset));
    }

    return (value);
}

uint16_t bitstream_reader_read_u16(struct bitstream_reader_t *self_p)
{
    uint16_t value;
    int i;
    int offset;
    const uint8_t *src_p;

    src_p = &self_p->buf_p[self_p->byte_offset];
    offset = (16 + self_p->bit_offset);
    value = 0;

    for (i = 0; i < 2; i++) {
        offset -= 8;
        value |= ((uint16_t)src_p[i] << offset);
    }

    if (offset != 0) {
        value |= (src_p[2] >> (8 - offset));
    }

    self_p->byte_offset += 2;

    return (value);
}

uint32_t bitstream_reader_read_u32(struct bitstream_reader_t *self_p)
{
    uint32_t value;
    int i;
    int offset;
    const uint8_t *src_p;

    src_p = &self_p->buf_p[self_p->byte_offset];
    offset = (32 + self_p->bit_offset);
    value = 0;

    for (i = 0; i < 4; i++) {
        offset -= 8;
        value |= ((uint32_t)src_p[i] << offset);
    }

    if (offset != 0) {
        value |= (src_p[4] >> (8 - offset));
    }

    self_p->byte_offset += 4;

    return (value);
}

uint64_t bitstream_reader_read_u64(struct bitstream_reader_t *self_p)
{
    uint64_t value;
    int i;
    int offset;
    const uint8_t *src_p;

    src_p = &self_p->buf_p[self_p->byte_offset];
    offset = (64 + self_p->bit_offset);
    value = 0;

    for (i = 0; i < 8; i++) {
        offset -= 8;
        value |= ((uint64_t)src_p[i] << offset);
    }

    if (offset != 0) {
        value |= ((uint64_t)src_p[8] >> (8 - offset));
    }

    self_p->byte_offset += 8;

    return (value);
}

uint64_t bitstream_reader_read_u64_bits(struct bitstream_reader_t *self_p,
                                        int number_of_bits)
{
    uint64_t value;
    int i;
    int first_byte_bits;
    int last_byte_bits;
    int full_bytes;

    if (number_of_bits == 0) {
        return (0);
    }

    /* Align beginning. */
    first_byte_bits = (8 - self_p->bit_offset);

    if (first_byte_bits != 8) {
        if (number_of_bits < first_byte_bits) {
            value = (self_p->buf_p[self_p->byte_offset] >> (first_byte_bits
                                                            - number_of_bits));
            value &= ((1 << number_of_bits) - 1);
            self_p->bit_offset += number_of_bits;
        } else {
            value = self_p->buf_p[self_p->byte_offset];
            value &= ((1 << first_byte_bits) - 1);
            self_p->byte_offset++;
            self_p->bit_offset = 0;
        }

        number_of_bits -= first_byte_bits;

        if (number_of_bits <= 0) {
            return (value);
        }
    } else {
        value = 0;
    }

    /* Copy middle bytes. */
    full_bytes = (number_of_bits / 8);

    for (i = 0; i < full_bytes; i++) {
        value <<= 8;
        value |= self_p->buf_p[self_p->byte_offset + i];
    }

    /* Last byte. */
    last_byte_bits = (number_of_bits % 8);

    if (last_byte_bits != 0) {
        value <<= last_byte_bits;
        value |= (self_p->buf_p[self_p->byte_offset + full_bytes]
                  >> (8 - last_byte_bits));
        self_p->bit_offset = last_byte_bits;
    }

    self_p->byte_offset += full_bytes;

    return (value);
}

void bitstream_reader_seek(struct bitstream_reader_t *self_p,
                           int offset)
{
    offset += ((8 * self_p->byte_offset) + self_p->bit_offset);
    self_p->byte_offset = (offset / 8);
    self_p->bit_offset = (offset % 8);
}

int bitstream_reader_tell(struct bitstream_reader_t *self_p)
{
    return ((8 * self_p->byte_offset) + self_p->bit_offset);
}
