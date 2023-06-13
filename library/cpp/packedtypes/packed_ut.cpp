#include "packed.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/defaults.h>
#include <util/generic/ylimits.h>
#include <util/generic/buffer.h>
#include <util/stream/mem.h>
#include <util/stream/buffer.h>

namespace NPrivate {
#if 0
static ui64 gSeed = 42;

template<typename T>
static T PseudoRandom(T max = Max<T>()) {
    Y_ASSERT(max != 0);
    // stupid and non-threadsafe, but very predictable chaos generator
    gSeed += 1;
    gSeed *= 419;
    gSeed = gSeed ^ (ui64(max) << 17u);
    return gSeed % max;
};
#endif
}

Y_UNIT_TEST_SUITE(TPackedTest) {
    void TestPackUi32Sub(ui32 v, const TVector<char>& p) {
        TBufferOutput out;
        PackUI32(out, v);
        const TBuffer& buf = out.Buffer();
        UNIT_ASSERT_VALUES_EQUAL(buf.Size(), p.size());
        UNIT_ASSERT(!memcmp(buf.Data(), &p[0], buf.Size()));

        {
            TBufferInput in(buf);
            ui32 v2;
            UnPackUI32(in, v2);
            UNIT_ASSERT_VALUES_EQUAL(v, v2);
        }

        {
            TZCMemoryInput in(buf.Data(), buf.Size());
            ui32 v2;
            UnPackUI32(in, v2);
            UNIT_ASSERT_VALUES_EQUAL(v, v2);
        }
    }

    Y_UNIT_TEST(TestPackUi32) {
        ui32 v;
        TVector<char> pv;

        v = 0;
        pv.resize(1);
        pv[0] = 0x0;
        TestPackUi32Sub(v, pv);

        v = 0x1600;
        pv.resize(2);
        pv[0] = 0x96;
        pv[1] = 0x00;
        TestPackUi32Sub(v, pv);

        v = 0xEF98;
        pv.resize(3);
        pv[0] = 0xC0;
#if defined(_big_endian_)
        pv[1] = 0xEF;
        pv[2] = 0x98;
#elif defined(_little_endian_)
        pv[1] = 0x98;
        pv[2] = 0xEF;
#endif
        TestPackUi32Sub(v, pv);

        v = 0xF567FE4;
        pv.resize(4);
        pv[0] = 0xEF;
        pv[1] = 0x56;
#if defined(_big_endian_)
        pv[2] = 0x7F;
        pv[3] = 0xE4;
#elif defined(_little_endian_)
        pv[2] = 0xE4;
        pv[3] = 0x7F;
#endif
        TestPackUi32Sub(v, pv);
    }

#if 0
    Y_UNIT_TEST(ReadWrite32) {
        TBuffer buffer(65536);

        char* writePtr = buffer.Data();
        TVector<ui32> correctNumbers;
        for (size_t i = 0; i < 1000; ++i) {
            ui32 randNum = NPrivate::PseudoRandom<ui32>();
            correctNumbers.push_back(randNum);
            writePtr = Pack32(randNum, writePtr);
        }

        const char* readPtr = buffer.Data();
        for (size_t i = 0; i < correctNumbers.size(); ++i) {
            ui32 value = 0xCCCCCCCC;
            readPtr = Unpack32(value, readPtr);
            UNIT_ASSERT_VALUES_EQUAL(value, correctNumbers[i]);
        }
    }

    Y_UNIT_TEST(ReadWrite64) {
        TBuffer buffer(65536);

        char* writePtr = buffer.Data();
        TVector<ui64> correctNumbers;
        for (size_t i = 0; i < 1000; ++i) {
            ui64 randNum = NPrivate::PseudoRandom<ui64>();
            correctNumbers.push_back(randNum);
            writePtr = Pack64(randNum, writePtr);
        }

        const char* readPtr = buffer.Data();
        for (size_t i = 0; i < correctNumbers.size(); ++i) {
            ui64 value = 0xDEADBEEF;
            readPtr = Unpack64(value, readPtr);
            UNIT_ASSERT_VALUES_EQUAL(value, correctNumbers[i]);
        }
    }
#endif
}
