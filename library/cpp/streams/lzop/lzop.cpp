#include "lzop.h"

#include <util/generic/buffer.h>

#include <util/system/info.h>

#include <contrib/libs/minilzo/minilzo.h>

// See https://svn.yandex.ru/statbox/packages/yandex/statbox-binaries/include/Statbox/LZOP.h
// https://github.yandex-team.ru/logbroker/push-client/blob/c820971a769df920d6ea9152a053474e75986914/src/lzo.c
// https://github.yandex-team.ru/logbroker/push-client/blob/c820971a769df920d6ea9152a053474e75986914/src/lzo.h
// As the source for the inspiration.
////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {
    namespace NLzop {
        static unsigned const char MAGIC[9] =
            {
                0x89, 0x4c, 0x5a, 0x4f,
                0x00,
                0x0d, 0x0a, 0x1a, 0x0a};

        // 32-bit Version.
        inline unsigned int RoundUpToPow2(unsigned int x) {
            x -= 1;
            x |= (x >> 1);
            x |= (x >> 2);
            x |= (x >> 4);
            x |= (x >> 8);
            x |= (x >> 16);
            return x + 1;
        }

        inline unsigned char* Get8(unsigned char* p, unsigned* v, ui32* adler32, ui32* crc32) {
            *v = 0;
            *v |= (unsigned)(*p++);

            *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 1), 1);
            *crc32 = lzo_crc32(*crc32, (const unsigned char*)(p - 1), 1);

            return p;
        }

        inline unsigned char* Put8(unsigned char* p, unsigned v, ui32* adler32, ui32* crc32) {
            *p++ = v & 0xff;

            *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 1), 1);
            *crc32 = lzo_crc32(*crc32, (const unsigned char*)(p - 1), 1);

            return p;
        }

        inline unsigned char* Get16(unsigned char* p, unsigned* v, ui32* adler32, ui32* crc32) {
            *v = 0;
            *v |= (unsigned)(*p++) << 8;
            *v |= (unsigned)(*p++);

            *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 2), 2);
            *crc32 = lzo_crc32(*crc32, (const unsigned char*)(p - 2), 2);

            return p;
        }

        inline unsigned char* Put16(unsigned char* p, unsigned v, ui32* adler32, ui32* crc32) {
            *p++ = (v >> 8) & 0xff;
            *p++ = (v)&0xff;

            *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 2), 2);
            *crc32 = lzo_crc32(*crc32, (const unsigned char*)(p - 2), 2);

            return p;
        }

        inline unsigned char* Get32(unsigned char* p, unsigned* v, ui32* adler32, ui32* crc32) {
            *v = 0;
            *v |= (unsigned)(*p++) << 24;
            *v |= (unsigned)(*p++) << 16;
            *v |= (unsigned)(*p++) << 8;
            *v |= (unsigned)(*p++);

            *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 4), 4);
            *crc32 = lzo_crc32(*crc32, (const unsigned char*)(p - 4), 4);

            return p;
        }

        inline unsigned char* Put32(unsigned char* p, unsigned v, ui32* adler32, ui32* crc32) {
            *p++ = (v >> 24) & 0xff;
            *p++ = (v >> 16) & 0xff;
            *p++ = (v >> 8) & 0xff;
            *p++ = (v)&0xff;

            *adler32 = lzo_adler32(*adler32, (const unsigned char*)(p - 4), 4);
            *crc32 = lzo_crc32(*crc32, (const unsigned char*)(p - 4), 4);

            return p;
        }

        enum ELzoFlag {
            LZO_ADLER32_D = 0x00000001L,
            LZO_ADLER32_C = 0x00000002L,
            LZO_STDIN = 0x00000004L,
            LZO_STDOUT = 0x00000008L,
            LZO_NAME_DEFAULT = 0x00000010L,
            LZO_DOSISH = 0x00000020L,
            LZO_H_EXTRA_FIELD = 0x00000040L,
            LZO_H_GMTDIFF = 0x00000080L,
            LZO_CRC32_D = 0x00000100L,
            LZO_CRC32_C = 0x00000200L,
            LZO_MULTIPART = 0x00000400L,
            LZO_H_FILTER = 0x00000800L,
            LZO_H_CRC32 = 0x00001000L,
            LZO_H_PATH = 0x00002000L,
            LZO_MASK = 0x00003FFFL
        };

        enum ELzoResult {
            LZO_END_OF_STREAM = 0,
            LZO_MORE_DATA = 1,
            LZO_OK = 2,
            LZO_WRONG_MAGIC = -12,
            LZO_VERSION_TOO_LOW = -13,
            LZO_EXTRACT_VERSION_TOO_HIGH = -14,
            LZO_EXTRACT_VERSION_TOO_LOW = -15,
            LZO_WRONG_CHECKSUM = -16,
            LZO_WRONG_METHOD = -18,
            LZO_COMPRESS_ERROR = -1,
            LZO_WRONG_DST_LEN = -2,
            LZO_DST_LEN_TOO_BIG = -3,
            LZO_WRONG_SRC_LEN = -4,
            LZO_INVALID_SRC_ADLER32 = -5,
            LZO_INVALID_SRC_CRC32 = -6,
            LZO_DECOMPRESS_ERROR = -7,
            LZO_INVALID_DST_ADLER32 = -8,
            LZO_INVALID_DST_CRC32 = -9,
        };

        // XXX(sandello): I don't really know where this comes from.
        struct THeader {
            unsigned Version;
            unsigned LibVersion;
            unsigned VersionNeededToExtract;
            unsigned char Method;
            unsigned char Level;

            ui32 Flags;
            ui32 Filter;
            ui32 Mode;
            ui32 MTimeLow;
            ui32 MTimeHigh;

            ui32 HeaderChecksum;

            ui32 ExtraFieldLen;
            ui32 ExtraFieldChecksum;

            const unsigned char* MethodName;

            char Name[255 + 1];
        };

    }
}

////////////////////////////////////////////////////////////////////////////////

class TLzopCompress::TImpl
   : public IOutputStream {
public:
    inline TImpl(IOutputStream* slave, ui16 blockSize)
        : Slave(slave)
        , HeaderWasWritten(false)
        , UncompressedBuffer(blockSize)
        , CompressedBuffer(8 + 4 * blockSize)
    {
        ResetHeader();
    }

protected:
    void DoWrite(const void* buffer, size_t length) override;
    void DoFlush() override;
    void DoFinish() override;

private:
    IOutputStream* Slave;

    NPrivate::NLzop::THeader Header;
    bool HeaderWasWritten;

    TBuffer UncompressedBuffer;
    TBuffer CompressedBuffer;

    void EnsureCompressedSpace(size_t demand);
    void EnsureUncompressedSpace(size_t demand);

    void ProduceHeader();
    void ProduceData();
    void ProduceTrailer();
    void ResetHeader();
};

void TLzopCompress::TImpl::DoWrite(const void* buffer, size_t length) {
    const char* data = (const char*)buffer;
    while (length > 0) {
        size_t bytesToFit = Min(UncompressedBuffer.Capacity(), length);
        size_t bytesToWrite = Min(UncompressedBuffer.Avail(), length);
        if (bytesToWrite > 0) {
            UncompressedBuffer.Append(data, bytesToWrite);
            data += bytesToWrite;
            length -= bytesToWrite;
        } else {
            EnsureUncompressedSpace(bytesToFit);
        }
    }
}

void TLzopCompress::TImpl::DoFlush() {
    EnsureUncompressedSpace(UncompressedBuffer.Capacity());
    EnsureCompressedSpace(CompressedBuffer.Capacity());
}

void TLzopCompress::TImpl::DoFinish() {
    EnsureUncompressedSpace(UncompressedBuffer.Capacity());
    ProduceTrailer();
    Flush();
}

void TLzopCompress::TImpl::EnsureCompressedSpace(size_t demand) {
    Y_ASSERT(demand <= CompressedBuffer.Capacity());
    if (CompressedBuffer.Avail() < demand) {
        Slave->Write(CompressedBuffer.Data(), CompressedBuffer.Size());
        CompressedBuffer.Clear();
    }
    Y_ASSERT(demand <= CompressedBuffer.Avail());
}

void TLzopCompress::TImpl::EnsureUncompressedSpace(size_t demand) {
    Y_ASSERT(demand <= UncompressedBuffer.Capacity());
    if (UncompressedBuffer.Avail() < demand) {
        ProduceData();
    }
    Y_ASSERT(demand <= UncompressedBuffer.Avail());
}

void TLzopCompress::TImpl::ResetHeader() {
    ::memset(&Header, 0, sizeof(Header));
}

void TLzopCompress::TImpl::ProduceHeader() {
    using namespace NPrivate::NLzop;

    ui32 adler32 = 1;
    ui32 crc32 = 0;

    unsigned char* p;
    unsigned char* pb;

    EnsureCompressedSpace(sizeof(MAGIC) + sizeof(Header));
    pb = p = (unsigned char*)CompressedBuffer.Pos();

    // Magic.
    ::memcpy(p, MAGIC, sizeof(MAGIC));
    p += sizeof(MAGIC);

    // .Version
    p = Put16(p, 0x1030U, &adler32, &crc32);
    // .LibVersion
    p = Put16(p, lzo_version() & 0xFFFFU, &adler32, &crc32);
    // .VersionNeededToExtract
    p = Put16(p, 0x0900, &adler32, &crc32);
    // .Method
    // XXX(sandello): Method deviates from Statbox' implementation.
    // In compatibility we trust.
    p = Put8(p, 2, &adler32, &crc32); // 1 = LZO1X_1, 2 = LZO1X_1_15
    // .Level
    p = Put8(p, 3, &adler32, &crc32);
    // .Flags
    p = Put32(p, 0, &adler32, &crc32);
    // .Mode
    p = Put32(p, 0644, &adler32, &crc32);
    // .MTimeLow
    p = Put32(p, 0, &adler32, &crc32);
    // .MTimeHigh
    p = Put32(p, 0, &adler32, &crc32);
    // .Name
    p = Put8(p, 0, &adler32, &crc32);
    // .HeaderChecksum
    p = Put32(p, adler32, &adler32, &crc32);

    CompressedBuffer.Proceed(CompressedBuffer.Size() + (p - pb));
}

void TLzopCompress::TImpl::ProduceTrailer() {
    using namespace NPrivate::NLzop;

    ui32 adler32 = 1;
    ui32 crc32 = 0;

    unsigned char* p;
    unsigned char* pb;

    EnsureCompressedSpace(4);
    pb = p = (unsigned char*)CompressedBuffer.Pos();

    p = Put32(p, 0, &adler32, &crc32);

    CompressedBuffer.Proceed(CompressedBuffer.Size() + (p - pb));
}

void TLzopCompress::TImpl::ProduceData() {
    using namespace NPrivate::NLzop;

    ui32 srcLen = (ui32)UncompressedBuffer.Size();
    ui32 dstLen;

    ui32 adler32 = 1;
    ui32 crc32 = 0;

    unsigned char* p;
    unsigned char* pb;

    lzo_uint result;

    // See include/lzo/lzo1x.h from lzo-2.06.
    // const size_t LZO1X_1_MEM_COMPRESS = (lzo_uint32)(16384L * lzo_sizeof_dict_t);
    unsigned char scratch[LZO1X_1_MEM_COMPRESS];

    if (!HeaderWasWritten) {
        ProduceHeader();
        HeaderWasWritten = true;
    }

    EnsureCompressedSpace(8 + 4 * srcLen);
    pb = p = (unsigned char*)CompressedBuffer.Pos();

    p = Put32(p, srcLen, &adler32, &crc32);
    p += 4;

    // XXX(sandello): Used compression Method deviates from Statbox's implementation.
    // Here we use |lzo1x_1_compress| (implemented in minilzo) whilst Statbox
    // uses |lzo1x_1_15_compress|.
    if (lzo1x_1_compress(
            (unsigned char*)UncompressedBuffer.Data(),
            UncompressedBuffer.Size(),
            p,
            &result,
            scratch) != LZO_E_OK)
    {
        ythrow yexception() << "LZOP Error: " << (int)LZO_COMPRESS_ERROR;
    }

    dstLen = result;

    if (dstLen < srcLen) {
        Put32(pb + 4, dstLen, &adler32, &crc32);
        /**/
        result = dstLen;
    } else {
        Put32(pb + 4, srcLen, &adler32, &crc32);
        ::memcpy(p, UncompressedBuffer.Data(), UncompressedBuffer.Size());
        result = srcLen;
    }

    result += 4 + 4; // srcLen + dstLen + (adler32|crc32, disabled)

    UncompressedBuffer.Clear();
    CompressedBuffer.Proceed(CompressedBuffer.Size() + result);
}

TLzopCompress::TLzopCompress(IOutputStream* slave, ui16 maxBlockSize)
    : Impl_(new TImpl(slave, maxBlockSize))
{
}

TLzopCompress::~TLzopCompress() {
    try {
        Finish();
    } catch (...) {
    }
}

void TLzopCompress::DoWrite(const void* buffer, size_t length) {
    if (!Impl_) {
        ythrow yexception() << "Stream is dead";
    }
    Impl_->Write((const char*)buffer, length);
}

void TLzopCompress::DoFlush() {
    if (!Impl_) {
        ythrow yexception() << "Stream is dead";
    }
    Impl_->Flush();
}

void TLzopCompress::DoFinish() {
    THolder<TImpl> impl(Impl_.Release());
    if (!!impl) {
        impl->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TLzopDecompress::TImpl
   : public IInputStream {
public:
    inline TImpl(IInputStream* slave, ui32 initialBufferSize)
        : Slave(slave)
        , Exhausted(false)
        , Hint(0)
        , InputData(NPrivate::NLzop::RoundUpToPow2(initialBufferSize))
        , OutputData(NPrivate::NLzop::RoundUpToPow2(initialBufferSize))
        , InputOffset(0)
        , OutputOffset(0)
    {
        ResetHeader();
    }

protected:
    size_t DoRead(void* buffer, size_t length) override;

private:
    IInputStream* Slave;
    bool Exhausted;
    unsigned int Hint;

    NPrivate::NLzop::THeader Header;

    TBuffer InputData;
    TBuffer OutputData;

    size_t InputOffset;
    size_t OutputOffset;

    void Trim(TBuffer& buffer, size_t& length);

    int ConsumeHeader();
    int ConsumeData();
    void ResetHeader();
};

size_t TLzopDecompress::TImpl::DoRead(void* buffer, size_t length) {
    size_t bytesRead = 0;
    size_t bytesAvailable = 0;

    do {
        bytesAvailable = Min(OutputData.Size() - OutputOffset, length);
        if (!bytesAvailable && !Exhausted) {
            int rv;
            while ((rv = ConsumeData()) == NPrivate::NLzop::LZO_MORE_DATA) {
                if (Hint) {
                    InputData.Reserve(NPrivate::NLzop::RoundUpToPow2(Hint));
                    Hint = 0;
                } else if (InputData.Avail() == 0) {
                    InputData.Reserve(2 * InputData.Capacity());
                }

                size_t tmp = Slave->Load(InputData.Pos(), InputData.Avail());
                if (tmp) {
                    InputData.Advance(tmp);
                } else {
                    Exhausted = true;
                    break;
                }
            }

            Trim(InputData, InputOffset);

            switch (rv) {
                case NPrivate::NLzop::LZO_OK:
                case NPrivate::NLzop::LZO_MORE_DATA:
                    break;
                case NPrivate::NLzop::LZO_END_OF_STREAM:
                    ResetHeader();
                    break;
                default:
                    ythrow yexception() << "LZOP Error: " << rv;
                    break;
            }
        } else if (bytesAvailable) {
            ::memcpy(
                (char*)buffer + bytesRead,
                OutputData.Data() + OutputOffset,
                bytesAvailable);
            bytesRead += bytesAvailable;
            OutputOffset += bytesAvailable;

            Trim(OutputData, OutputOffset);
        } else {
            break;
        }
    } while (!bytesRead);

    return bytesRead;
}

void TLzopDecompress::TImpl::ResetHeader() {
    ::memset(&Header, 0, sizeof(Header));
}

void TLzopDecompress::TImpl::Trim(TBuffer& buffer, size_t& length) {
    size_t remaining = buffer.Size() - length;
    ::memmove(
        buffer.Data(),
        buffer.Data() + length,
        remaining);
    buffer.Resize(remaining);
    length = 0;
}

int TLzopDecompress::TImpl::ConsumeHeader() {
    using namespace NPrivate::NLzop;

    THeader* h = &Header;

    ui32 adler32 = 1;
    ui32 crc32 = 0;
    ui32 checksum;

    unsigned tmp;

    unsigned char* p;
    unsigned char* pb;
    unsigned char* pe;

    pb = p = (unsigned char*)InputData.Data() + InputOffset;
    pe = (unsigned char*)InputData.Pos();

    // Magic.
    if (pe < p + sizeof(MAGIC))
        return LZO_MORE_DATA;
    if (memcmp(MAGIC, p, sizeof(MAGIC)) != 0) {
        return LZO_WRONG_MAGIC;
    }
    p += sizeof(MAGIC);

    // .Version
    if (pe - p < 2)
        return LZO_MORE_DATA;
    p = Get16(p, &h->Version, &adler32, &crc32);
    if (h->Version < 0x0900) {
        return LZO_VERSION_TOO_LOW;
    }

    // .LibVersion, .VersionNeededToExtract
    if (pe - p < 2)
        return LZO_MORE_DATA;
    p = Get16(p, &h->LibVersion, &adler32, &crc32);
    if (h->Version >= 0x0940) {
        if (pe - p < 2)
            return LZO_MORE_DATA;
        p = Get16(p, &h->VersionNeededToExtract, &adler32, &crc32);
        if (h->VersionNeededToExtract > 0x1030) {
            return LZO_EXTRACT_VERSION_TOO_HIGH;
        }
        if (h->VersionNeededToExtract < 0x0900) {
            return LZO_EXTRACT_VERSION_TOO_LOW;
        }
    }

    // .Method, .Level
    if (pe - p < 1)
        return LZO_MORE_DATA;
    p = Get8(p, &tmp, &adler32, &crc32);
    h->Method = tmp;
    if (h->Version >= 0x0940) {
        if (pe - p < 1)
            return LZO_MORE_DATA;
        p = Get8(p, &tmp, &adler32, &crc32);
        h->Level = tmp;
    }

    // .Flags
    if (pe - p < 4)
        return LZO_MORE_DATA;
    p = Get32(p, &h->Flags, &adler32, &crc32);

    // .Filter
    if (h->Flags & LZO_H_FILTER) {
        if (pe - p < 4)
            return LZO_MORE_DATA;
        p = Get32(p, &h->Filter, &adler32, &crc32);
    }

    // .Mode
    if (pe - p < 4)
        return LZO_MORE_DATA;
    p = Get32(p, &h->Mode, &adler32, &crc32);

    // .MTimeLow
    if (pe - p < 4)
        return LZO_MORE_DATA;
    p = Get32(p, &h->MTimeLow, &adler32, &crc32);

    // .MTimeHigh
    if (h->Version >= 0x0940) {
        if (pe - p < 4)
            return LZO_MORE_DATA;
        p = Get32(p, &h->MTimeHigh, &adler32, &crc32);
    }
    if (h->Version < 0x0120) {
        if (h->MTimeLow == 0xffffffffUL) {
            h->MTimeLow = 0;
        }
        h->MTimeHigh = 0;
    }

    // .Name
    if (pe - p < 1)
        return LZO_MORE_DATA;
    p = Get8(p, &tmp, &adler32, &crc32);
    if (tmp > 0) {
        if (pe - p < tmp)
            return LZO_MORE_DATA;
        adler32 = lzo_adler32(adler32, p, tmp);
        crc32 = lzo_crc32(crc32, p, tmp);

        ::memcpy(h->Name, p, tmp);
        p += tmp;
    }

    if (h->Flags & LZO_H_CRC32) {
        checksum = crc32;
    } else {
        checksum = adler32;
    }

    // .HeaderChecksum
    if (pe - p < 4)
        return LZO_MORE_DATA;
    p = Get32(p, &h->HeaderChecksum, &adler32, &crc32);
    if (h->HeaderChecksum != checksum) {
        return LZO_WRONG_CHECKSUM;
    }

    // XXX(sandello): This is internal Statbox constraint.
    // XXX(aozeritsky): Statbox uses Method = 2, Java uses Method = 1
    // XXX(aozeritsky): Both methods use the same decompression function
    if (!(h->Method == 1 || h->Method == 2)) {
        return LZO_WRONG_METHOD;
    }

    if (h->Flags & LZO_H_EXTRA_FIELD) {
        if (pe - p < 4)
            return LZO_MORE_DATA;
        p = Get32(p, &h->ExtraFieldLen, &adler32, &crc32);
        if (pe - p < h->ExtraFieldLen)
            return LZO_MORE_DATA;
        p += h->ExtraFieldLen;
    }

    // OK
    InputOffset += p - pb;
    return LZO_OK;
}

int TLzopDecompress::TImpl::ConsumeData() {
    using namespace NPrivate::NLzop;

    THeader* h = &Header;

    ui32 adler32 = 1;
    ui32 crc32 = 0;

    ui32 dAdler32 = 1;
    ui32 dCrc32 = 0;
    ui32 cAdler32 = 1;
    ui32 cCrc32 = 0;

    ui32 dstLen;
    ui32 srcLen;

    unsigned char* p;
    unsigned char* pb;
    unsigned char* pe;

    if (h->Version == 0) {
        return ConsumeHeader();
    }

    pb = p = (unsigned char*)InputData.Data() + InputOffset;
    pe = (unsigned char*)InputData.Pos();

    // dstLen
    if (pe - p < 4)
        return LZO_MORE_DATA;
    p = Get32(p, &dstLen, &adler32, &crc32);

    if (dstLen == 0) {
        InputOffset += p - pb;
        return LZO_END_OF_STREAM;
    }
    if (dstLen == 0xffffffffUL) {
        return LZO_WRONG_DST_LEN;
    }
    if (dstLen > 64 * 1024 * 1024) {
        return LZO_DST_LEN_TOO_BIG;
    }

    // srcLen
    if (pe - p < 4)
        return LZO_MORE_DATA;
    p = Get32(p, &srcLen, &adler32, &crc32);

    if (srcLen <= 0 || srcLen > dstLen) {
        return LZO_WRONG_SRC_LEN;
    }

    if (h->Flags & LZO_ADLER32_D) {
        if (pe - p < 4)
            return LZO_MORE_DATA;
        p = Get32(p, &dAdler32, &adler32, &crc32);
    }
    if (h->Flags & LZO_CRC32_D) {
        if (pe - p < 4)
            return LZO_MORE_DATA;
        p = Get32(p, &dCrc32, &adler32, &crc32);
    }

    if (h->Flags & LZO_ADLER32_C) {
        if (srcLen < dstLen) {
            if (pe - p < 4)
                return LZO_MORE_DATA;
            p = Get32(p, &cAdler32, &adler32, &crc32);
        } else {
            if (!(h->Flags & LZO_ADLER32_D))
                ythrow yexception() << "h->Flags & LZO_ADLER32_C & ~LZO_ADLER32_D";
            cAdler32 = dAdler32;
        }
    }
    if (h->Flags & LZO_CRC32_C) {
        if (srcLen < dstLen) {
            if (pe - p < 4)
                return LZO_MORE_DATA;
            p = Get32(p, &cCrc32, &adler32, &crc32);
        } else {
            if (!(h->Flags & LZO_CRC32_D))
                ythrow yexception() << "h->Flags & LZO_CRC32_C & ~LZO_CRC32_D";
            cCrc32 = dCrc32;
        }
    }

    // Rock'n'roll! Check'n'consume!
    if (pe - p < srcLen) {
        Hint = (p - pb) + srcLen;
        return LZO_MORE_DATA;
    }

    if (h->Flags & LZO_ADLER32_C) {
        ui32 checksum;
        checksum = lzo_adler32(1, p, srcLen);
        if (checksum != cAdler32) {
            return LZO_INVALID_SRC_ADLER32;
        }
    }
    if (h->Flags & LZO_CRC32_C) {
        ui32 checksum;
        checksum = lzo_crc32(1, p, srcLen);
        if (checksum != cCrc32) {
            return LZO_INVALID_SRC_CRC32;
        }
    }

    if (OutputData.Avail() < dstLen) {
        OutputData.Reserve(RoundUpToPow2(2 * (OutputData.Size() + dstLen)));
    }

    unsigned char* output = (unsigned char*)OutputData.Pos();
    OutputData.Advance(dstLen);

    if (srcLen < dstLen) {
        lzo_uint tmp;
        int rv;

        tmp = dstLen;
        rv = lzo1x_decompress_safe(
            p,
            srcLen,
            output,
            &tmp,
            0);

        if (rv != LZO_E_OK || tmp != dstLen) {
            return LZO_DECOMPRESS_ERROR;
        }
    } else {
        if (!(dstLen == srcLen)) {
            ythrow yexception() << "dstLen == srcLen";
        }
        ::memcpy(output, p, srcLen);
    }

    p += srcLen;

    // Check again.
    if (h->Flags & LZO_ADLER32_D) {
        ui32 checksum;
        checksum = lzo_adler32(1, output, dstLen);
        if (checksum != dAdler32) {
            return LZO_INVALID_DST_ADLER32;
        }
    }
    if (h->Flags & LZO_CRC32_D) {
        ui32 checksum;
        checksum = lzo_crc32(1, output, dstLen);
        if (checksum != dCrc32) {
            return LZO_INVALID_DST_CRC32;
        }
    }

    // OK
    InputOffset += p - pb;
    return LZO_OK;
}

TLzopDecompress::TLzopDecompress(IInputStream* slave, ui32 initialBufferSize)
    : Impl_(new TImpl(slave, initialBufferSize))
{
}

TLzopDecompress::~TLzopDecompress() {
}

size_t TLzopDecompress::DoRead(void* buffer, size_t length) {
    return Impl_->Read(buffer, length);
}
