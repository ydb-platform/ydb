/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Author: Steve Borho <steve@borho.org>
 *         Min Chen <chenm003@163.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com.
 *****************************************************************************/

#ifndef X265_BITSTREAM_H
#define X265_BITSTREAM_H 1

namespace X265_NS {
// private namespace

class BitInterface
{
public:

    virtual void     write(uint32_t val, uint32_t numBits)  = 0;
    virtual void     writeByte(uint32_t val)                = 0;
    virtual void     resetBits()                            = 0;
    virtual uint32_t getNumberOfWrittenBits() const         = 0;
    virtual void     writeAlignOne()                        = 0;
    virtual void     writeAlignZero()                       = 0;
    virtual ~BitInterface() {}
};

class BitCounter : public BitInterface
{
protected:

    uint32_t  m_bitCounter;

public:

    BitCounter() : m_bitCounter(0) {}

    void     write(uint32_t, uint32_t num)  { m_bitCounter += num; }
    void     writeByte(uint32_t)            { m_bitCounter += 8;   }
    void     resetBits()                    { m_bitCounter = 0;    }
    uint32_t getNumberOfWrittenBits() const { return m_bitCounter; }
    void     writeAlignOne()                { }
    void     writeAlignZero()               { }
};


class Bitstream : public BitInterface
{
public:

    Bitstream();
    ~Bitstream()                             { X265_FREE(m_fifo); }

    void     resetBits()                     { m_partialByteBits = m_byteOccupancy = 0; m_partialByte = 0; }
    uint32_t getNumberOfWrittenBytes() const { return m_byteOccupancy; }
    uint32_t getNumberOfWrittenBits()  const { return m_byteOccupancy * 8 + m_partialByteBits; }
    const uint8_t* getFIFO() const           { return m_fifo; }
    void     copyBits(Bitstream* stream)     { m_partialByteBits = stream->m_partialByteBits; m_byteOccupancy = stream->m_byteOccupancy; m_partialByte = stream->m_partialByte; }

    void     write(uint32_t val, uint32_t numBits);
    void     writeByte(uint32_t val);

    void     writeAlignOne();      // insert one bits until the bitstream is byte-aligned
    void     writeAlignZero();     // insert zero bits until the bitstream is byte-aligned
    void     writeByteAlignment(); // insert 1 bit, then pad to byte-align with zero

private:

    uint8_t *m_fifo;
    uint32_t m_byteAlloc;
    uint32_t m_byteOccupancy;
    uint32_t m_partialByteBits;
    uint8_t  m_partialByte;

    void     push_back(uint8_t val);
};

static const uint8_t bitSize[256] =
{
    1, 1, 3, 3, 5, 5, 5, 5, 7, 7, 7, 7, 7, 7, 7, 7,
    9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
    13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
    13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
    13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
};

static inline int bs_size_ue(unsigned int val)
{
    return bitSize[val + 1];
}

static inline int bs_size_ue_big(unsigned int val)
{
    if (val < 255)
        return bitSize[val + 1];
    else
        return bitSize[(val + 1) >> 8] + 16;
}

static inline int bs_size_se(int val)
{
    int tmp = 1 - val * 2;

    if (tmp < 0) tmp = val * 2;
    if (tmp < 256)
        return bitSize[tmp];
    else
        return bitSize[tmp >> 8] + 16;
}

class SyntaxElementWriter
{
public:

    BitInterface* m_bitIf;

    SyntaxElementWriter() : m_bitIf(NULL) {}

    /* silently discard the name of the syntax element */
    inline void WRITE_CODE(uint32_t code, uint32_t length, const char *) { writeCode(code, length); }
    inline void WRITE_UVLC(uint32_t code,                  const char *) { writeUvlc(code); }
    inline void WRITE_SVLC(int32_t  code,                  const char *) { writeSvlc(code); }
    inline void WRITE_FLAG(bool flag,                      const char *) { writeFlag(flag); }

    void writeCode(uint32_t code, uint32_t length) { m_bitIf->write(code, length); }
    void writeUvlc(uint32_t code);
    void writeSvlc(int32_t code)                   { uint32_t ucode = (code <= 0) ? -code << 1 : (code << 1) - 1; writeUvlc(ucode); }
    void writeFlag(bool code)                      { m_bitIf->write(code, 1); }
};

}

#endif // ifndef X265_BITSTREAM_H
