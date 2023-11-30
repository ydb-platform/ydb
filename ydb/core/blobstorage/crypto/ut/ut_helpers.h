#pragma once

#include <ydb/library/actors/util/rope.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/align.h>

#include <utility>

class TAlignedBuf {
    ui8 *Buf;
    ui8 *AlignedBuf;
    size_t BufSize;
    size_t Align;

    void AllocBuf() {
        Buf = new ui8[BufSize + Align];
        AlignedBuf = reinterpret_cast<ui8*>(AlignUp<intptr_t>(intptr_t(Buf), Align));
    }

public:
    TAlignedBuf(size_t s, size_t align)
      : BufSize(s)
      , Align(align)
    {
        AllocBuf();
    }

    TAlignedBuf(const TAlignedBuf& other)
      : BufSize(other.BufSize)
      , Align(other.Align)
    {
        AllocBuf();
    }

    TAlignedBuf(TAlignedBuf&& other)
    {
        std::swap(Buf, other.Buf);
        std::swap(AlignedBuf, other.AlignedBuf);
        std::swap(BufSize, other.BufSize);
        std::swap(Align, other.Align);
    }

    ui8 *Data() {
        return AlignedBuf;
    }

    const ui8 *Data() const {
        return AlignedBuf;
    }

    size_t Size() const {
        return BufSize;
    }

    ~TAlignedBuf() {
        delete[] Buf;
    }
};

class TRopeAlignedBufferBackend : public IContiguousChunk {
    TAlignedBuf Buffer;

public:
    TRopeAlignedBufferBackend(size_t size, size_t align)
        : Buffer(size, align)
    {}

    TContiguousSpan GetData() const override {
        return {reinterpret_cast<const char *>(Buffer.Data()), Buffer.Size()};
    }

    TMutableContiguousSpan GetDataMut() override {
        return {reinterpret_cast<char *>(Buffer.Data()), Buffer.Size()};
    }

    TMutableContiguousSpan UnsafeGetDataMut() override {
        return {reinterpret_cast<char *>(Buffer.Data()), Buffer.Size()};
    }

    size_t GetOccupiedMemorySize() const override {
        return Buffer.Size();
    }
};

void inline Print(const ui8* out, size_t size) {
    for (ui32 i = 0; i < size; ++i) {
        if (i % 16 == 0) {
            Cerr << LeftPad(i, 3) << ": ";
        }
        Cerr << Hex(out[i], HF_FULL) << " ";
        if ((i + 1) % 16 == 0) {
            Cerr << Endl;
        }
    }
    Cerr << Endl;
}


#define UNIT_ASSERT_ARRAYS_EQUAL(A, B, size)                            \
    do {                                                                \
        for (size_t i = 0; i < size; i++) {                             \
            UNIT_ASSERT_EQUAL_C((A)[i], (B)[i],                         \
                "arrays are not equal "                                 \
                " a[" << i << "]# " << Hex((A)[i], HF_FULL) << " != "   \
                " b[" << i << "]# " << Hex((B)[i], HF_FULL));           \
        }                                                               \
    } while (0)
