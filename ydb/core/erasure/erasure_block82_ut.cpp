#include "erasure.h"
#include "ut_util.h"

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/hex.h>

namespace NKikimr {
namespace {

constexpr auto Species = TErasureType::Erasure8Plus2Block;
constexpr auto NoCrc = TErasureType::CrcModeNone;
constexpr auto WholeCrc = TErasureType::CrcModeWholePart;
constexpr ui32 AllParts = (1u << 10) - 1;
using TParts = std::array<TString, 10>;
using TRopeParts = std::array<TRope, 10>;

TString Data(size_t size) {
    NPrivate::TMersenne64 randGen(82011);
    return GenerateRandomString(randGen, size);
}

// Byte-wise reference: eight data columns, ten stored rows and an imaginary
// zero row. Build all eleven diagonal sums before removing the last diagonal.
// No production splitting, geometry or parity helpers are used here.
TParts Reference(const TString& data, TErasureType::ECrcMode crc) {
    TParts parts;
    const size_t size = (data.size() + 639) / 640 * 80;
    for (auto& part : parts) {
        part = TString(size, '\0');
    }
    size_t pos = 0;
    for (size_t column = 0; column < 8; ++column) {
        const size_t used = (data.size() / 640 + (column < data.size() / 80 % 8)) * 80
            + (column == 7 ? data.size() % 80 : 0);
        if (used) {
            memcpy(parts[column].Detach(), data.data() + pos, used);
        }
        pos += used;
    }
    UNIT_ASSERT_VALUES_EQUAL(pos, data.size());
    for (size_t offset = 0; offset < size; offset += 80) {
        std::array<std::array<ui8, 8>, 11> diagonals{};
        for (size_t column = 0; column < 8; ++column) {
            for (size_t row = 0; row < 10; ++row) {
                for (size_t byte = 0; byte < 8; ++byte) {
                    const ui8 value = parts[column][offset + row * 8 + byte];
                    parts[8].Detach()[offset + row * 8 + byte] ^= value;
                    diagonals[(row + column) % 11][byte] ^= value;
                }
            }
        }
        for (size_t row = 0; row < 10; ++row) {
            for (size_t byte = 0; byte < 8; ++byte) {
                parts[9][offset + row * 8 + byte] = diagonals[row][byte] ^ diagonals[10][byte];
            }
        }
    }
    if (crc == WholeCrc) {
        for (auto& part : parts) {
            const ui32 hash = data.empty() ? 0 : Crc32c(part.data(), part.size());
            part.append(reinterpret_cast<const char*>(&hash), sizeof(hash));
        }
    }
    return parts;
}

TRope Fragmented(const TString& data) {
    TRope rope;
    for (size_t pos = 0; pos < data.size();) {
        const size_t len = Min<size_t>(1 + pos % 97, data.size() - pos);
        rope.Insert(rope.End(), TRope(data.substr(pos, len)));
        pos += len;
    }
    return rope;
}

void CheckParts(const TDataPartSet& actual, const TParts& expected, ui32 mask = AllParts) {
    UNIT_ASSERT_VALUES_EQUAL(actual.Parts.size(), expected.size());
    for (ui32 i = 0; i < expected.size(); ++i) {
        if (mask >> i & 1) {
            UNIT_ASSERT_VALUES_EQUAL_C(actual.Parts[i].OwnedString.ConvertToString(), expected[i], "part# " << i);
        }
    }
}

const std::array<ui32, 21> Sizes = {
    0, 1, 7, 8, 9, 31, 32, 79, 80, 81, 159, 160, 161,
    559, 560, 561, 639, 640, 641, 1279, 1281
};

} // namespace

Y_UNIT_TEST_SUITE(ErasureBlock82) {
    Y_UNIT_TEST(RegistryAndGeometry) {
        const TErasureType type(Species);
        UNIT_ASSERT_VALUES_EQUAL(Species, 19);
        UNIT_ASSERT_VALUES_EQUAL(type.ToString(), "block-8-2");
        UNIT_ASSERT_VALUES_EQUAL(TErasureType::ErasureSpeciesToStr(Species), "8Plus2Block");
        UNIT_ASSERT_VALUES_EQUAL(TErasureType::ErasureSpeciesByName("block-8-2"), Species);
        UNIT_ASSERT_EQUAL(type.ErasureFamily(), TErasureType::ErasureParityBlock);
        UNIT_ASSERT_VALUES_EQUAL(type.DataParts(), 8);
        UNIT_ASSERT_VALUES_EQUAL(type.ParityParts(), 2);
        UNIT_ASSERT_VALUES_EQUAL(type.TotalPartCount(), 10);
        UNIT_ASSERT_VALUES_EQUAL(type.MinimalRestorablePartCount(), 8);
        UNIT_ASSERT_VALUES_EQUAL(type.Prime(), 11);
        UNIT_ASSERT_VALUES_EQUAL(type.MinimalBlockSize(), 640);
        for (ui32 size : Sizes) {
            const ui64 expected = (size + 639) / 640 * 80;
            UNIT_ASSERT_VALUES_EQUAL(type.PartUserSize(size), expected);
            UNIT_ASSERT_VALUES_EQUAL(type.PartSize(NoCrc, size), expected);
            UNIT_ASSERT_VALUES_EQUAL(type.PartSize(WholeCrc, size), expected + 4);
        }
        for (ui32 size : {1, 79, 80, 81, 159, 160, 161, 1000}) {
            for (auto crc : {NoCrc, WholeCrc}) {
                const ui32 suffix = crc == WholeCrc ? 4 : 0;
                UNIT_ASSERT_VALUES_EQUAL(type.SuggestDataSize(crc, size + suffix, true), size / 80 * 640);
                UNIT_ASSERT_VALUES_EQUAL(type.SuggestDataSize(crc, size + suffix, false), (size + 79) / 80 * 640);
            }
        }
    }

    Y_UNIT_TEST(GoldenParity) {
        TString data = TString::Uninitialized(640);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = 17 * i + (i >> 3);
        }
        TDataPartSet parts;
        TErasureType(Species).SplitData(NoCrc, data, parts);
        CheckParts(parts, Reference(data, NoCrc));
        UNIT_ASSERT_VALUES_EQUAL(parts.Parts[8].OwnedString.ConvertToString(), HexDecode(
            "20a07090008070902070d0a0a0d07020907080009070a02070e0a0b0d06020900080309060a0d070"
            "e050b0e0603090009030e060b0d060e050a0e0903080009020e050b0a060f050a0f0900080f09020"));
        UNIT_ASSERT_VALUES_EQUAL(parts.Parts[9].OwnedString.ConvertToString(), HexDecode(
            "e51345772dd30dbff6b62a920e7e6a52bba11f01eb8157e1bca4846c042c6ce401b7d933a11f7913"
            "725a7e1ec2fad636d745332d2f2dd345c233b4b5b6b7c8d9ba2b9c8dfe6fa02132b3343536374859"));
    }

    Y_UNIT_TEST(SplitMatchesIndependentReference) {
        const TErasureType type(Species);
        // Every tail position, plus several complete columns in every data part.
        for (ui32 size = 0; size <= 1920; ++size) {
            const auto data = Data(size);
            for (auto crc : {NoCrc, WholeCrc}) {
                const auto expected = Reference(data, crc);
                TDataPartSet parts;
                type.SplitData(crc, data, parts);
                CheckParts(parts, expected);
                TRopeParts canonical;
                UNIT_ASSERT(ErasureSplit(crc, type, Fragmented(data), canonical, nullptr, GetDefaultRcBufAllocator()));
                for (ui32 i = 0; i < 10; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(canonical[i].ConvertToString(), expected[i]);
                }
            }
        }
    }

    Y_UNIT_TEST(AllLossesAndLegacyRestoreModes) {
        const TErasureType type(Species);
        struct TRestoreMode {
            bool RestoreParts;
            bool RestoreFullData;
            bool RestoreParityParts;
        };
        constexpr TRestoreMode modes[] = {
            {false, true, false}, // Whole blob only.
            {true, false, false}, // Data parts only.
            {true, true, false},  // Data parts and whole blob.
            {true, false, true}, // Data and parity parts.
            {true, true, true},   // All parts and whole blob.
        };
        for (ui32 size : Sizes) {
            const auto data = Data(size);
            for (auto crc : {NoCrc, WholeCrc}) {
                const auto expected = Reference(data, crc);
                const auto check = [&](ui32 loss) {
                    for (const auto& [restoreParts, restoreFullData, restoreParityParts] : modes) {
                        TDataPartSet set;
                        set.FullDataSize = size;
                        set.PartsMask = AllParts ^ loss;
                        set.Parts.resize(10);
                        for (ui32 i = 0; i < 10; ++i) {
                            set.Parts[i].ResetToWhole(TRope(loss >> i & 1
                                ? TString(expected[i].size(), '\xa5') : expected[i]));
                        }
                        type.RestoreData(crc, set, restoreParts, restoreFullData, restoreParityParts);
                        if (restoreFullData) {
                            UNIT_ASSERT_VALUES_EQUAL_C(set.FullDataFragment.OwnedString.ConvertToString(), data,
                                "size# " << size << " loss# " << loss
                                << " restoreParts# " << restoreParts << " restoreFullData# " << restoreFullData
                                << " restoreParityParts# " << restoreParityParts);
                        }
                        CheckParts(set, expected,
                            (AllParts ^ loss) | (restoreParts ? (restoreParityParts ? AllParts : 255) : 0));
                    }
                };
                check(0);
                for (ui32 l1 = 0; l1 < 10; ++l1) {
                    for (ui32 l2 = 0; l2 <= l1; ++l2) {
                        check((1u << l1) | (1u << l2));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(CanonicalSelectedOutputsAndWhole) {
        for (ui32 size : Sizes) {
            const auto data = Data(size);
            for (auto crc : {NoCrc, WholeCrc}) {
                const auto expected = Reference(data, crc);
                const auto check = [&](ui32 loss) {
                    ui32 output = loss;
                    do {
                        for (bool needWhole : {false, true}) {
                            if (!output && !needWhole) {
                                continue;
                            }
                            TRopeParts parts;
                            for (ui32 i = 0; i < 10; ++i) {
                                if (!(loss >> i & 1)) {
                                    parts[i] = Fragmented(expected[i]);
                                }
                            }
                            TRope whole(TString("previous contents"));
                            ErasureRestore(crc, Species, size, needWhole ? &whole : nullptr, parts, output);
                            if (needWhole) {
                                UNIT_ASSERT_VALUES_EQUAL(whole.ConvertToString(), data);
                            }
                            for (ui32 i = 0; i < 10; ++i) {
                                if ((output | (AllParts ^ loss)) >> i & 1) {
                                    UNIT_ASSERT_VALUES_EQUAL_C(parts[i].ConvertToString(), expected[i],
                                        "size# " << size << " loss# " << loss << " output# " << output << " part# " << i);
                                } else {
                                    UNIT_ASSERT(parts[i].empty());
                                }
                            }
                        }
                        if (!output) {
                            break;
                        }
                        output = (output - 1) & loss;
                    } while (true);
                };
                check(0);
                for (ui32 l1 = 0; l1 < 10; ++l1) {
                    for (ui32 l2 = 0; l2 <= l1; ++l2) {
                        check((1u << l1) | (1u << l2));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(IncrementalSplit) {
        const TErasureType type(Species);
        for (ui32 size : {640 * 1024 - 1, 640 * 1024, 640 * 1024 + 1, 640 * 2050 + 79}) {
            const auto data = Data(size);
            for (auto crc : {NoCrc, WholeCrc}) {
                const auto expected = Reference(data, crc);
                TRope input(data);
                TDataPartSet parts;
                ui32 calls = 0;
                do {
                    const auto previous = parts.CurBlockIdx;
                    type.IncrementalSplitData(crc, input, parts);
                    UNIT_ASSERT(parts.CurBlockIdx > previous);
                    UNIT_ASSERT(parts.CurBlockIdx - previous <= 1024);
                    ++calls;
                    UNIT_ASSERT(calls <= 3);
                } while (!parts.IsSplitDone());
                UNIT_ASSERT_VALUES_EQUAL(calls, (size / 640 + 1023) / 1024);
                CheckParts(parts, expected);
                UNIT_ASSERT_VALUES_EQUAL(input.ConvertToString(), data);
            }
        }
    }

    Y_UNIT_TEST(AlignedFragmentsAllLosses) {
        for (ui32 size : {640u * 5, 640u * 5 + 17}) {
            const auto expected = Reference(Data(size), NoCrc);
            const ui32 partSize = expected[0].size();
            for (ui32 offset = 0; offset < partSize; offset += 80) {
                for (ui32 length : {80u, partSize - offset}) {
                    for (ui32 l1 = 0; l1 < 10; ++l1) {
                        for (ui32 l2 = 0; l2 <= l1; ++l2) {
                            const ui32 loss = (1u << l1) | (1u << l2);
                            TRopeParts parts;
                            for (ui32 i = 0; i < 10; ++i) {
                                if (!(loss >> i & 1)) {
                                    parts[i] = Fragmented(expected[i].substr(offset, length));
                                }
                            }
                            ErasureRestore(NoCrc, Species, size, nullptr, parts, loss, offset, true);
                            for (ui32 i = 0; i < 10; ++i) {
                                UNIT_ASSERT_VALUES_EQUAL_C(parts[i].ConvertToString(), expected[i].substr(offset, length),
                                    "size# " << size << " offset# " << offset << " length# " << length << " loss# " << loss << " part# " << i);
                            }
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(RangeMapping) {
        const TErasureType type(Species);
        for (ui32 size : Sizes) {
            const auto data = Data(size);
            const auto expected = Reference(data, NoCrc);
            for (ui32 begin = 0; begin < size; ++begin) {
                const ui32 end = Min(size, begin + 173);
                TBlockSplitRange range;
                type.BlockSplitRange(NoCrc, size, begin, end, &range);
                TString restored;
                for (ui32 i = range.BeginPartIdx; i < range.EndPartIdx; ++i) {
                    const auto& part = range.PartRanges[i];
                    UNIT_ASSERT_VALUES_EQUAL(part.AlignedBegin % 80, 0);
                    UNIT_ASSERT_VALUES_EQUAL(part.AlignedEnd % 80, 0);
                    UNIT_ASSERT(part.AlignedBegin <= part.Begin && part.AlignedEnd >= part.End);
                    restored += expected[i].substr(part.Begin, part.End - part.Begin);
                }
                UNIT_ASSERT_VALUES_EQUAL(restored, data.substr(begin, end - begin));
            }
        }
    }
}
} // namespace NKikimr
