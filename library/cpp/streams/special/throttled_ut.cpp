// DO_NOT_STYLE

#include "throttled.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>
#include <util/stream/null.h>
#include <util/stream/buffer.h>
#include <util/string/printf.h>
#include <util/system/filemap.h>
#include <util/system/tempfile.h>
#include <util/system/info.h>
#include <util/system/mincore.h>

namespace {
    template <typename U, typename V>
    void AssertEqualArrays(const U* begin1, const U* end1, const V* begin2) {
        auto m = Mismatch(begin1, end1, begin2);
        if (m.first != end1) {
            UNIT_FAIL(Sprintf("mismatch at position %zu: %hhu != %hhu", (m.first - begin1), *m.first, *m.second));
        }
    }

    void DoCorrectnessTestWrite(ui32 bytesPerSecond, ui32 bufferSize, ui32 totalOutputSize, TDuration samplingInterval) {
        TVector<ui8> data(bufferSize);
        ui8 i1 = 0;
        for (auto& d : data) {
            d = ++i1; // unisgned wrap is not an UB
        }
        auto slave = MakeAtomicShared<TBufferOutput>();
        TThrottledOutputStream out(slave, bytesPerSecond, samplingInterval);
        for (size_t written = 0, step = 0; written < totalOutputSize; written += step) {
            step = Min<size_t>(data.size(), totalOutputSize - written);
            out.Write(data.data(), step);
        }
        out.Finish();

        UNIT_ASSERT_VALUES_EQUAL(totalOutputSize, slave->Buffer().Size());
        for (size_t checked = 0, step = 0; checked < totalOutputSize; checked += step) {
            step = Min<size_t>(data.size(), totalOutputSize - checked);
            AssertEqualArrays(data.begin(), data.begin() + step, (ui8*)(slave->Buffer().data() + checked));
        }
    }

    TBuffer PrepareInput(ui32 bufferSize, ui32 totalOutputSize) {
        TVector<ui8> data(bufferSize);
        ui8 i1 = 0;
        for (auto& d : data) {
            d = ++i1; // unisgned wrap is not an UB
        }
        TBuffer buffer;
        TBufferOutput out(buffer);
        for (size_t written = 0, step = 0; written < totalOutputSize; written += step) {
            step = Min<size_t>(data.size(), totalOutputSize - written);
            out.Write(data.data(), step);
        }
        out.Finish();
        return buffer;
    }

    void DoCorrectnessTestRead(ui32 bytesPerSecond, ui32 bufferSize, ui32 totalOutputSize, TDuration samplingInterval) {
        const TBuffer testInput = PrepareInput(bufferSize, totalOutputSize);

        TThrottledInputStream inp(MakeAtomicShared<TBufferInput>(testInput), TThrottle::TOptions::FromMaxPerSecond(bytesPerSecond, samplingInterval));
        const auto readString = inp.ReadAll();

        UNIT_ASSERT_VALUES_EQUAL(testInput.size(), readString.size());
        AssertEqualArrays(testInput.data(), testInput.data() + testInput.size(), readString.data());
    }

    void DoCorrectnessTest(ui32 bytesPerSecond, ui32 bufferSize, ui32 totalOutputSize, TDuration samplingInterval) {
        DoCorrectnessTestWrite(bytesPerSecond, bufferSize, totalOutputSize, samplingInterval);
        DoCorrectnessTestRead(bytesPerSecond, bufferSize, totalOutputSize, samplingInterval);
    }

    ui64 CheckStreamSpeed(IOutputStream& stream, TConstArrayRef<char> data, ui32 totalOutputSize, TDuration samplingInterval) {
        TInstant start = Now();
        for (size_t written = 0, step = 0; written < totalOutputSize; written += step) {
            step = Min<size_t>(data.size(), totalOutputSize - written);
            stream.Write(data.data(), step);
        }
        TInstant end = Now();

        // Add extra sampling interval because there's no sleep after the last block
        ui64 time = (end - start).MicroSeconds() + samplingInterval.MicroSeconds();
        return time ? (1000000ULL * totalOutputSize / time) : Max<ui64>();
    }

    void DoSpeedTestOverNullOutput(ui32 bytesPerSecond, ui32 bufferSize, ui32 totalOutputSize, TDuration samplingInterval) {
        // Tt's important to create a buffer up-front (malloc+memset) because it interferes with timings in debug and
        // sanitized builds.
        TVector<char> data(bufferSize);

        TAtomicSharedPtr<IOutputStream> slave(new TNullOutput);
        ui64 speed = CheckStreamSpeed(*slave, data, totalOutputSize, samplingInterval);
        UNIT_ASSERT_GT_C(speed, bytesPerSecond, "values: " << speed << " and " << bytesPerSecond);

        TThrottledOutputStream out(slave, bytesPerSecond, samplingInterval);
        speed = CheckStreamSpeed(out, data, totalOutputSize, samplingInterval);
        UNIT_ASSERT_LE_C(speed, bytesPerSecond, "values: " << speed << " and " << bytesPerSecond);
    }

    struct TTestInput : IInputStream {
        TTestInput(char symbol, size_t size)
            : Symbol(symbol)
            , Size(size)
        {
        }

    private:
        size_t DoRead(void* buf, size_t len) final {
            size_t readCount = 0;
            for (char* out = (char*)buf; Position < Size && readCount < len; ++Position, ++readCount) {
                out[readCount] = Symbol;
            }
            return readCount;
        }

        const char Symbol;
        const size_t Size;
        size_t Position = 0;
    };

    ui64 CheckReadStreamSpeed(IInputStream& stream, TVector<char>& buffer, ui32 totalOutputSize, TDuration samplingInterval) {
        TInstant start = Now();
        for (size_t readBytes = 0, step = 0; readBytes < totalOutputSize; readBytes += step) {
            step = Min<size_t>(buffer.size(), totalOutputSize - readBytes);
            stream.Read(buffer.data(), buffer.size());
        }
        TInstant end = Now();

        // Add extra sampling interval because there's no sleep after the last block
        ui64 time = (end - start).MicroSeconds() + samplingInterval.MicroSeconds();
        return time ? (1000000ULL * totalOutputSize / time) : Max<ui64>();
    }

    void DoSpeedTestRead(ui32 bytesPerSecond, ui32 bufferSize, ui32 totalOutputSize, TDuration samplingInterval) {
        TVector<char> data(bufferSize);

        auto testInput = MakeAtomicShared<TTestInput>('\0', totalOutputSize);
        ui64 speed = CheckReadStreamSpeed(*testInput, data, totalOutputSize, samplingInterval);
        UNIT_ASSERT_GT_C(speed, bytesPerSecond, "values: " << speed << " and " << bytesPerSecond);

        TThrottledInputStream inp(testInput, TThrottle::TOptions::FromMaxPerSecond(bytesPerSecond, samplingInterval));
        speed = CheckReadStreamSpeed(inp, data, totalOutputSize, samplingInterval);
        UNIT_ASSERT_LE_C(speed, bytesPerSecond, "values: " << speed << " and " << bytesPerSecond);
    }

    void DoSpeedTestOverNullStream(ui32 bytesPerSecond, ui32 bufferSize, ui32 totalOutputSize, TDuration samplingInterval) {
        DoSpeedTestOverNullOutput(bytesPerSecond, bufferSize, totalOutputSize, samplingInterval);
        DoSpeedTestRead(bytesPerSecond, bufferSize, totalOutputSize, samplingInterval);
    }
}

Y_UNIT_TEST_SUITE(ThrottledStreamTestSuite) {
    Y_UNIT_TEST(ThrottledCorrectness1) {
        DoCorrectnessTest(1_MB, 10_KB, 1_MB, TDuration::MilliSeconds(100));
    }

    Y_UNIT_TEST(ThrottledCorrectness2) {
        DoCorrectnessTest(10_KB, 20_KB, 20_KB, TDuration::MilliSeconds(100));
    }

    Y_UNIT_TEST(ThrottledCorrectness3) {
        DoCorrectnessTest(167, 31, 191, TDuration::MilliSeconds(11));
    }

    Y_UNIT_TEST(ThrottledCorrectness4) {
        DoCorrectnessTest(1000 * 1000, 10 * 1000, 1000 * 1000, TDuration::MilliSeconds(100));
    }

    Y_UNIT_TEST(ThrottledSpeed) {
        DoSpeedTestOverNullStream(10_MB, 10_KB, 40_MB, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ThrottledSpeedSampling125ms) {
        DoSpeedTestOverNullStream(10_MB, 10_KB, 10_MB, TDuration::MilliSeconds(125));
    }

    Y_UNIT_TEST(ThrottledSpeedSampling125msSmall) {
        DoSpeedTestOverNullOutput(10_MB, 10_MB, 5_MB, TDuration::MilliSeconds(125));
    }

    Y_UNIT_TEST(ThrottledSpeedSampling2000ms) {
        DoSpeedTestOverNullStream(10_MB, 10_KB, 50_MB, TDuration::Seconds(2));
    }

    Y_UNIT_TEST(ThrottledSpeedLargeBuffers) {
        DoSpeedTestOverNullOutput(2_KB, 10_KB, 10_KB, TDuration::Seconds(1));
    }
}

Y_UNIT_TEST_SUITE(ThrottledLockMemoryTestSuite) {
    static const char* FileName_("./mapped_file");

    Y_UNIT_TEST(LockFileCorrectness) {
        TDuration samplingInterval = TDuration::MilliSeconds(100);
        ui64 pageSize = NSystemInfo::GetPageSize();
        TThrottle::TOptions throttleOptions(TThrottle::TOptions::FromMaxPerInterval(4_MB, samplingInterval));

        TTempFile cleanup(FileName_);
        TFile file(FileName_, CreateAlways | WrOnly);
        file.Resize(8_MB);
        file.Resize(20_MB);
        file.Resize(16_MB);
        file.Close();

        TFileMap mappedFile(FileName_, TMemoryMapCommon::oRdWr);
        mappedFile.Map(0, mappedFile.Length());

        ThrottledLockMemory(mappedFile.Ptr(), mappedFile.Length(), throttleOptions);

        TVector<unsigned char> incore(mappedFile.Length() / pageSize);
        InCoreMemory(mappedFile.Ptr(), mappedFile.Length(), incore.data(), incore.size());

        //mlock doesn't work under msan/asan. https://lists.llvm.org/pipermail/llvm-commits/Week-of-Mon-20131028/193334.html
#if !defined( _msan_enabled_) && !defined(_asan_enabled_)
        for (const auto& flag : incore) {
            UNIT_ASSERT(IsPageInCore(flag));
        }
#endif
    }

    Y_UNIT_TEST(FileInMemoryAlgoCorrectness) {
        TDuration samplingInterval = TDuration::Seconds(1);
        ui64 pageSize = NSystemInfo::GetPageSize();
        TThrottle::TOptions throttleOptions (TThrottle::TOptions::FromMaxPerInterval(2_MB, samplingInterval));

        TTempFile cleanup(FileName_);
        TFile file(FileName_, CreateAlways | WrOnly);
        file.Resize(80_MB);
        file.Resize(20_MB);
        file.Resize(120_MB);
        file.Close();

        TFileMap mappedFile(FileName_, TMemoryMapCommon::oRdWr);
        mappedFile.Map(0, mappedFile.Length());

        ThrottledLockMemory(mappedFile.Ptr(), mappedFile.Length(), throttleOptions);

        TVector<unsigned char> incore(mappedFile.Length() / pageSize);
        InCoreMemory(mappedFile.Ptr(), mappedFile.Length(), incore.data(), incore.size());

        //mlock doesn't work under msan/asan. https://lists.llvm.org/pipermail/llvm-commits/Week-of-Mon-20131028/193334.html
#if !defined( _msan_enabled_) && !defined(_asan_enabled_)
        for (const auto& flag : incore) {
            UNIT_ASSERT(IsPageInCore(flag));
        }

        TInstant start = Now();
        ThrottledLockMemory(mappedFile.Ptr(), mappedFile.Length(), throttleOptions);
        TInstant end = Now();

        ui64 countIntervals = (end - start).Seconds() / samplingInterval.Seconds();
        UNIT_ASSERT(countIntervals <= 7);
#endif
    }
}

