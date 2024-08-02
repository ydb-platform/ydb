#include "erasure.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ymath.h>
#include <util/random/entropy.h>
#include <util/random/mersenne64.h>
#include <util/stream/null.h>
#include <util/string/printf.h>
#include <util/system/hp_timer.h>
#include <iostream>
#include <numeric>
#include <library/cpp/digest/crc32c/crc32c.h>

#define SHORT_TEST
//#define LONG_TEST


#ifdef SHORT_TEST
#   define N_REPEATS 3
#   define BUFFER_SIZE (1024 * 1024)
#   define ATTEMPTS 100
#elif defined( LONG_TEST )
#   define N_REPEATS 5
#   define BUFFER_SIZE (1ull * 1024 * 1024 * 1024)
#   define ATTEMPTS 100000
#endif


namespace NKikimr {

TString GenerateData(ui32 dataSize) {
    TString testString;
    testString.resize(dataSize);
    NPrivate::TMersenne64 randGen(Seed());
    ui64 *writePos64 = (ui64 *)testString.data();
    ui32 ui64Parts = testString.size() / sizeof(ui64);
    for (ui32 i = 0; i < ui64Parts; ++i) {
        *writePos64++ = randGen.GenRand();
    }

    char *writePosChar = (char *)writePos64;
    ui32 charParts = testString.size() % sizeof(ui64);
    for (ui32 i = 0; i < charParts; ++i) {
        writePosChar[i] = (char)randGen.GenRand();
    }
    return testString;
}

std::pair<double, double> CalcAvgSd(TVector<double> &times) {
    double avg = 0;
    double sd = 0;
    auto min = std::min_element(times.begin(), times.end());
    *min = times.back();
    times.pop_back();
    auto max = std::max_element(times.begin(), times.end());
    *max = times.back();
    times.pop_back();
    // Calc average and standard deviation
    for (const double &time : times) {
        avg += time;
    }
    avg /= times.size();
    for (const double &time : times) {
        sd += (avg - time) * (avg - time);
    }
    sd = sqrt(sd / times.size());
    return std::make_pair(avg, sd);
}

template <bool measureSplit, bool measureRestore>
std::pair<double, double> MeasureTime(TErasureType &type, TVector<ui32> &missedParts, ui32 dataSize,
                                    bool isRestoreParts, bool isRestoreFullData) {

    const size_t attempts = dataSize < 10000 ? ATTEMPTS : 10;

    THPTimer timer;
    TVector<double> times;
    ui32 partMask = ~(ui32)0;
    for (const ui32 &part : missedParts) {
        partMask &= ~(ui32)(1ul << part);
    }
    for (ui32 i = 0; i < N_REPEATS; ++i) {
        std::vector<TString> originalData;
        std::vector<TString> testData;
        originalData.resize(attempts);
        testData.resize(attempts);
        for (size_t i = 0; i < attempts; ++i) {
            originalData[i] = GenerateData(dataSize);
            testData[i] = originalData[i];
        }

        double time = 0;
        // Split the data into parts
        std::vector<TDataPartSet> partSets;
        partSets.resize(attempts);

        for (auto& partSet : partSets) {
            const ui32 partSize = type.PartSize(TErasureType::CrcModeNone, dataSize);
            partSet.Parts.resize(type.TotalPartCount());
            for (ui32 i = 0; i < type.TotalPartCount(); ++i) {
                auto data = partSet.Parts[i].OwnedString.GetContiguousSpan();
                TString newOwnedString(partSize, '\0');
                if (data.size() > 0) {
                    Y_ABORT_UNLESS(partSize >= data.size());
                    memcpy(newOwnedString.Detach(), data.data(), data.size());
                }
                partSet.Parts[i].ReferenceTo(partSet.Parts[i].OwnedString);
            }
        }
        if (measureSplit) {
            timer.Reset();
        }
        for (size_t i = 0; i < attempts; ++i) {
            type.SplitData(TErasureType::CrcModeNone, testData[i], partSets[i]);
        }

        if (measureSplit) {
            time += timer.PassedReset() / attempts;
        }
        if (isRestoreFullData) {
            std::vector<TRope> restoredData;
            restoredData.resize(attempts);
            for (auto& restored : restoredData) {
                restored = TRope(TString(dataSize, '\0'));
            }
            // Remove the 'missing' parts
            for (auto& partSet : partSets) {
                partSet.PartsMask &= partMask;
            }
            if (measureRestore) {
                timer.Reset();
            }
            for (size_t i = 0; i < attempts; ++i) {
                type.RestoreData(TErasureType::CrcModeNone, partSets[i], restoredData[i],
                        isRestoreParts, isRestoreFullData, isRestoreParts);
            }
            if (measureRestore) {
                time += timer.PassedReset() / attempts;
            }
            for (size_t i = 0; i < attempts; ++i) {
                UNIT_ASSERT_EQUAL(TRope(originalData[i]), restoredData[i]);
            }
        }
        times.push_back(time);
    }
    return CalcAvgSd(times);
}

TVector<TVector<ui32>> ChooseCombinationCase(TErasureType &type) {
    if (type.GetErasure() == TErasureType::EErasureSpecies::Erasure4Plus2Stripe ||
            type.GetErasure() == TErasureType::EErasureSpecies::Erasure4Plus2Block ) {
        return { {0, 1}
                ,{0, 4}
                ,{0, 5}
                ,{4, 5} };
    }
    return {};
}

const char *Bool2str(bool val) {
    return val ? "true " : "false";
}

void MeasureRestoreTime(TErasureType &type) {
    Cout << "EErasureType = " << type.ToString() << "  Measuring restore time, time in milliseconds" << Endl;
    TVector<TVector<ui32>> testCombin = ChooseCombinationCase(type);
    TVector<ui64> dataSizes {100, 4*1024, 4111, 8*1024, 8207, 4062305, 4*1024*1024};
    for (const ui64 &size : dataSizes) {
    Cout << "    size=" << size << Endl;
        for (TVector<ui32> &combination : testCombin) {
            for (ui32 variant = 1; variant < 2; ++variant) {
                bool isRestoreParts = false;
                bool isRestoreFullData = false;
                switch (variant) {
                    case 0:
                        isRestoreParts = true;
                        isRestoreFullData = true;
                        break;
                    case 1:
                        isRestoreFullData = true;
                        break;
                    case 2:
                        isRestoreParts = true;
                        break;
                }
                TStringStream mode;
                mode << "        combination = ";
                for (const ui32 &part : combination) {
                    mode << part << ", ";
                }
                mode << "mode = " << Bool2str(isRestoreParts) << "," << Bool2str(isRestoreFullData) << " ";
                std::pair<double, double> time = MeasureTime<false, true>(type, combination, size,
                                                                    isRestoreParts, isRestoreFullData);
                Cout << mode.Str() << Sprintf(" %9.6lf +- %9.6lf",
                                        1000 * time.first,  1000 * time.second) << Endl;
            }
        }
    }
}

void MeasureSplitTime(TErasureType &type) {
    Cout << "EErasureType = " << type.ToString() << Endl;
    TVector<ui32> combination {0, 1};
    //TVector<ui64> dataSizes {100, 4012, 4*1024, 4111, 8*1024, 8207, 4062305, 4*1024*1024};
    TVector<ui64> dataSizes {100, 4012, 4*1024, 4111};
    for (const ui64 &size : dataSizes) {
        TStringStream mode;
        std::pair<double, double> time = MeasureTime<true, false>(type, combination, size, false, false);
        Cout << size << "\n    "
            <<  mode.Str() << Sprintf("time = %.3lf +- %.3lf (us)", 1000000*time.first, 1000000*time.second)
            << Sprintf(", speed=%.3fGB/s", size / time.first / 1000000000.0) << Endl;
    }
}

Y_UNIT_TEST_SUITE(TErasurePerfTest) {
    Y_UNIT_TEST(Split) {
        TErasureType type(TErasureType::EErasureSpecies::Erasure4Plus2Block);
        MeasureSplitTime(type);
    }

    Y_UNIT_TEST(Restore) {
        TErasureType type(TErasureType::EErasureSpecies::Erasure4Plus2Block);
        MeasureRestoreTime(type);
    }
}

inline TRope RopeFromStringReference(TString string) {
    if (string.Empty()) {
        return TRope();
    }
    return TRope(std::move(string));
}

template <bool convertToRope>
void RopeMeasureSplitTime(TErasureType &type, ui64 dataSize, const TString& buffer) {
    NPrivate::TMersenne64 randGen(Seed());
    THPTimer timer;

    const size_t attempts = dataSize < 10000 ? ATTEMPTS : 10;
#ifdef LONG_TEST
    double time = 0;
#endif
    for (ui64 i = 0; i < attempts; ++i) {
        ui64 begin = randGen.GenRand() % (buffer.size() - dataSize);
        TString rope = TString(buffer.data() + begin, buffer.data() + begin + dataSize);

        TRope ropes[6];

        timer.Reset();

        TDataPartSet partSet;
        type.SplitData(TErasureType::CrcModeNone, rope, partSet);
        if (convertToRope) {
            for (int i = 0; i < 6; ++i) {
                ropes[i] = partSet.Parts[i].OwnedString;
            }
        }
#ifdef LONG_TEST
        time += timer.PassedReset();
#endif
    }


#ifdef LONG_TEST
    double bs = (dataSize * attempts) / time;
    Cerr << bs << " byte / s";
    if (convertToRope) {
        Cerr << " (convert to rope mode)\n";
    } else {
        Cerr << " (string erasure mode)\n";
    }
#endif
}

TString GenerateTestBuffer(ui64 size) {
    NPrivate::TMersenne64 randGen(Seed());
    TString buffer = TString::Uninitialized(size);
    ui64 a = randGen.GenRand();
    ui64 b = randGen.GenRand();
    for (ui64 i = 0; i < size / sizeof(ui64); ++i) {
        *((ui64*) buffer.Detach() + i) = a * i + b;
    }
    return buffer;
}

Y_UNIT_TEST_SUITE(TErasureSmallBlobSizePerfTest) {

    TString buffer = GenerateTestBuffer(BUFFER_SIZE);

    Y_UNIT_TEST(StringErasureMode) {
        TErasureType type(TErasureType::EErasureSpecies::Erasure4Plus2Block);
        RopeMeasureSplitTime<false>(type, 128, buffer);
    }

    Y_UNIT_TEST(ConvertToRopeMode) {
        TErasureType type(TErasureType::EErasureSpecies::Erasure4Plus2Block);
        RopeMeasureSplitTime<true>(type, 128, buffer);
    }
}


}
