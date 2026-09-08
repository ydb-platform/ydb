#include "erasure.h"

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <array>
#include <vector>

#if defined(_unix_)
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace NKikimr;

namespace {

constexpr auto Species = TErasureType::Erasure8Plus2Block;
constexpr auto NoCrc = TErasureType::CrcModeNone;
constexpr auto WholeCrc = TErasureType::CrcModeWholePart;
constexpr ui32 AllParts = (1u << 10) - 1;
using TParts = std::array<TRope, 10>;
using TBytes = std::array<TString, 10>;

// This oracle deliberately depends on neither ISA-L nor the production layout helpers.
ui8 Multiply(ui8 a, ui8 b) {
    ui8 result = 0;
    for (; b; b >>= 1) {
        if (b & 1) {
            result ^= a;
        }
        a = (a << 1) ^ ((a & 0x80) ? 0x1d : 0);
    }
    return result;
}

ui32 CrcOracle(TStringBuf bytes) {
    ui32 crc = ~ui32{0};
    for (const unsigned char byte : bytes) {
        crc ^= byte;
        for (ui32 bit = 0; bit != 8; ++bit) {
            crc = (crc >> 1) ^ ((crc & 1) ? 0x82f63b78u : 0);
        }
    }
    return ~crc;
}

TString Corpus(size_t size) {
    TString data = TString::Uninitialized(size);
    ui32 state = 0x9479ae31;
    for (char& byte : data) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        byte = state;
    }
    return data;
}

TBytes EncodeOracle(TStringBuf whole, TErasureType::ECrcMode crcMode) {
    TBytes parts;
    const size_t partSize = (whole.size() + 255) / 256 * 32;
    const size_t columns = whole.size() / 32;
    size_t pos = 0;
    for (ui32 i = 0; i != 10; ++i) {
        parts[i] = TString(partSize, '\0');
        if (i < 8) {
            const size_t used = (columns / 8 + (i < columns % 8)) * 32
                + (i == 7 ? whole.size() % 32 : 0);
            if (used) {
                memcpy(parts[i].begin(), whole.data() + pos, used);
            }
            pos += used;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(pos, whole.size());
    for (size_t offset = 0; offset != partSize; ++offset) {
        ui8 p0 = 0, p1 = 0;
        for (ui32 i = 0; i != 8; ++i) {
            const ui8 byte = parts[i][offset];
            p0 ^= byte;
            p1 ^= Multiply(byte, 1u << i);
        }
        parts[8][offset] = p0;
        parts[9][offset] = p1;
    }
    if (crcMode == WholeCrc) {
        for (TString& part : parts) {
            const ui32 crc = CrcOracle(part);
            for (ui32 shift = 0; shift != 32; shift += 8) {
                part.push_back(crc >> shift);
            }
        }
    }
    return parts;
}

TString Hex(TStringBuf value) {
    constexpr char digits[] = "0123456789abcdef";
    TString result;
    result.reserve(value.size() * 2);
    for (const unsigned char byte : value) {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 15]);
    }
    return result;
}

TRope Fragment(TStringBuf data) {
    TRope result;
    constexpr size_t chunks[] = {1, 15, 17, 63, 255, 33};
    for (size_t pos = 0, index = 0; pos < data.size(); ++index) {
        const size_t count = std::min(chunks[index % std::size(chunks)], data.size() - pos);
        auto chunk = TRcBuf::Uninitialized(count, 3, 7);
        memcpy(chunk.GetDataMut(), data.data() + pos, count);
        result.Insert(result.End(), std::move(chunk));
        pos += count;
    }
    return result;
}

void CheckBytes(TStringBuf actual, TStringBuf expected) {
    // Avoid the unittest framework's quadratic text diff for corrupted MiB blobs.
    UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
    if (actual != expected) {
        const auto mismatch = std::mismatch(actual.begin(), actual.end(), expected.begin());
        UNIT_FAIL(TStringBuilder() << "byte mismatch at offset# " << (mismatch.first - actual.begin())
            << " size# " << actual.size() << " actual# " << unsigned(ui8(*mismatch.first))
            << " expected# " << unsigned(ui8(*mismatch.second)));
    }
}

void CheckParts(const TParts& actual, const TBytes& expected) {
    for (ui32 i = 0; i != 10; ++i) {
        CheckBytes(actual[i].ConvertToString(), expected[i]);
    }
}

TParts MakeSurvivors(const TBytes& bytes, ui32 missing, bool fragmented = false) {
    TParts parts;
    for (ui32 i = 0; i != 10; ++i) {
        if (!(missing & (1u << i))) {
            parts[i] = fragmented ? Fragment(bytes[i]) : TRope(bytes[i]);
        }
    }
    return parts;
}

std::vector<ui32> LossMasks() {
    std::vector<ui32> masks{0};
    for (ui32 i = 0; i != 10; ++i) {
        masks.push_back(1u << i);
        for (ui32 j = i + 1; j != 10; ++j) {
            masks.push_back((1u << i) | (1u << j));
        }
    }
    return masks;
}

class TCountingAllocator final : public IRcBufAllocator {
public:
    size_t Calls = 0;
    size_t MaxAllocation = 0;

    TRcBuf AllocRcBuf(size_t size, size_t headRoom, size_t tailRoom) noexcept override {
        ++Calls;
        MaxAllocation = std::max(MaxAllocation, size);
        return TRcBuf::Uninitialized(size, headRoom, tailRoom);
    }

    TRcBuf AllocPageAlignedRcBuf(size_t size, size_t tailRoom) noexcept override {
        ++Calls;
        MaxAllocation = std::max(MaxAllocation, size);
        return GetDefaultRcBufAllocator()->AllocPageAlignedRcBuf(size, tailRoom);
    }
};

#if defined(_unix_)
template<class TCallable>
void AssertAborts(TCallable&& action) {
    const pid_t pid = fork();
    UNIT_ASSERT(pid >= 0);
    if (!pid) {
        const struct rlimit noCore{0, 0};
        setrlimit(RLIMIT_CORE, &noCore);
        signal(SIGABRT, SIG_DFL);
        if (!freopen("/dev/null", "w", stderr)) {
            _exit(100);
        }
        action();
        _exit(0);
    }
    int status = 0;
    pid_t result;
    do {
        result = waitpid(pid, &status, 0);
    } while (result == -1 && errno == EINTR);
    UNIT_ASSERT_VALUES_EQUAL(result, pid);
    UNIT_ASSERT_C(WIFSIGNALED(status), "expected SIGABRT, wait status# " << status);
    UNIT_ASSERT_VALUES_EQUAL(WTERMSIG(status), SIGABRT);
}
#endif

} // anonymous namespace

Y_UNIT_TEST_SUITE(ErasureBlock82) {
    Y_UNIT_TEST(RegistryAndGeometry) {
        const TErasureType type(Species);
        UNIT_ASSERT_VALUES_EQUAL(Species, 19);
        UNIT_ASSERT_VALUES_EQUAL(TErasureType::ErasureSpeciesCount, 20);
        UNIT_ASSERT_VALUES_EQUAL(type.ToString(), "block-8-2");
        UNIT_ASSERT_VALUES_EQUAL(TErasureType::ErasureSpeciesToStr(Species), "8Plus2Block");
        UNIT_ASSERT_VALUES_EQUAL(TErasureType::ErasureSpeciesByName("block-8-2"), Species);
        UNIT_ASSERT_EQUAL(type.ErasureFamily(), TErasureType::ErasureParityBlock);
        UNIT_ASSERT_VALUES_EQUAL(type.DataParts(), 8);
        UNIT_ASSERT_VALUES_EQUAL(type.ParityParts(), 2);
        UNIT_ASSERT_VALUES_EQUAL(type.TotalPartCount(), 10);
        UNIT_ASSERT_VALUES_EQUAL(type.MinimalRestorablePartCount(), 8);
        UNIT_ASSERT_VALUES_EQUAL(type.Prime(), 11);
        UNIT_ASSERT_VALUES_EQUAL(type.ColumnSize(), 32);
        UNIT_ASSERT_VALUES_EQUAL(type.MinimalBlockSize(), 256);
        UNIT_ASSERT(type.ColumnSize() != (type.Prime() - 1) * sizeof(ui64));

        for (ui32 size : {0, 1, 31, 32, 33, 63, 64, 65, 255, 256, 257, 511, 512, 513,
                700, 1000, 4096, 10 * 1024 * 1024}) {
            const ui64 userSize = (size + 255) / 256 * 32;
            UNIT_ASSERT_VALUES_EQUAL(type.PartUserSize(size), userSize);
            UNIT_ASSERT_VALUES_EQUAL(type.PartSize(NoCrc, size), userSize);
            UNIT_ASSERT_VALUES_EQUAL(type.PartSize(WholeCrc, size), userSize + 4);
        }
        for (ui32 size : {1, 31, 32, 33, 63, 64, 65, 255, 256, 257}) {
            for (auto crc : {NoCrc, WholeCrc}) {
                const ui32 suffix = crc == WholeCrc ? 4 : 0;
                UNIT_ASSERT_VALUES_EQUAL(type.SuggestDataSize(crc, size + suffix, true), size / 32 * 256);
                UNIT_ASSERT_VALUES_EQUAL(type.SuggestDataSize(crc, size + suffix, false), (size + 31) / 32 * 256);
            }
        }
    }

    Y_UNIT_TEST(OldSpeciesGeometryIsUnchanged) {
        // These values are frozen from the registry preceding species 19.
        constexpr ui32 columns[] = {1, 1, 8, 8, 32, 16, 32, 16, 1, 1, 32, 32, 16, 16, 16, 16, 16, 16, 1};
        constexpr ui32 counts[] = {1, 1, 3, 3, 4, 3, 4, 3, 1, 1, 4, 4, 3, 3, 2, 2, 2, 2, 1};
        for (ui32 species = 0; species != std::size(columns); ++species) {
            const TErasureType type{TErasureType::EErasureSpecies(species)};
            const ui32 block = columns[species] * counts[species];
            UNIT_ASSERT_VALUES_EQUAL_C(type.ColumnSize(), columns[species], species);
            UNIT_ASSERT_VALUES_EQUAL_C(type.MinimalBlockSize(), block, species);
            for (ui32 size : {0, 1, 7, 8, 15, 16, 31, 32, 33, 127, 128, 129, 700, 1000}) {
                const ui64 user = (size + block - 1) / block * columns[species];
                UNIT_ASSERT_VALUES_EQUAL_C(type.PartUserSize(size), user, species);
                UNIT_ASSERT_VALUES_EQUAL_C(type.PartSize(NoCrc, size), user, species);
                UNIT_ASSERT_VALUES_EQUAL_C(type.PartSize(WholeCrc, size),
                    user + ((size || counts[species] != 1) ? 4 : 0), species);
            }
        }
    }

    Y_UNIT_TEST(PersistentGoldenBytesAndCrc) {
        TString whole;
        for (ui32 i = 0; i != 33; ++i) {
            whole.push_back(i);
        }
        constexpr TStringBuf zeros = "0000000000000000000000000000000000000000000000000000000000000000";
        const std::array<TStringBuf, 10> golden = {
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            zeros, zeros, zeros, zeros, zeros, zeros,
            "2000000000000000000000000000000000000000000000000000000000000000",
            "200102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "cd0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        };
        const std::array<TStringBuf, 10> suffixes = {
            "4e79dd46", "aa36918a", "aa36918a", "aa36918a", "aa36918a", "aa36918a",
            "aa36918a", "1d6fce27", "f92082eb", "a154cd84"
        };
        for (auto crc : {NoCrc, WholeCrc}) {
            TParts parts;
            UNIT_ASSERT(ErasureSplit(crc, Species, TRope(whole), parts, nullptr, GetDefaultRcBufAllocator()));
            for (ui32 i = 0; i != 10; ++i) {
                const TString expected = TString(golden[i]) + (crc == WholeCrc ? TString(suffixes[i]) : TString());
                UNIT_ASSERT_VALUES_EQUAL_C(Hex(parts[i].ConvertToString()), expected, i);
                UNIT_ASSERT(CheckCrcAtTheEnd(crc, parts[i]));
            }
            CheckParts(parts, EncodeOracle(whole, crc));
        }

        whole.clear();
        for (ui32 i = 0; i != 8; ++i) {
            whole += TString(32, 1u << i);
        }
        TParts parts;
        UNIT_ASSERT(ErasureSplit(NoCrc, Species, TRope(whole), parts, nullptr, GetDefaultRcBufAllocator()));
        UNIT_ASSERT_VALUES_EQUAL(parts[8].ConvertToString(), TString(32, '\xff'));
        UNIT_ASSERT_VALUES_EQUAL(parts[9].ConvertToString(), TString(32, '\xe2'));
    }

    Y_UNIT_TEST(SplitBoundaryCorpusMatchesIndependentOracle) {
        for (ui32 size : {0, 1, 31, 32, 33, 63, 64, 65, 255, 256, 257, 400, 511, 512, 513,
                700, 1000, 4096, 65537, 10 * 1024 * 1024}) {
            const TString data = Corpus(size);
            for (auto crc : {NoCrc, WholeCrc}) {
                TParts parts;
                UNIT_ASSERT(ErasureSplit(crc, Species, TRope(data), parts, nullptr, GetDefaultRcBufAllocator()));
                CheckParts(parts, EncodeOracle(data, crc));
                TRope restored;
                ErasureRestore(crc, Species, size, &restored, parts, 0);
                CheckBytes(restored.ConvertToString(), data);
            }
        }
    }

    Y_UNIT_TEST(WholeAndPartRangeMapping) {
        const TErasureType type(Species);
        for (ui32 size : {1, 31, 32, 33, 255, 256, 257, 400, 700, 1000}) {
            ui64 start = 0;
            for (ui32 part = 0; part != 8; ++part) {
                const ui64 used = (size / 256 + (part < size / 32 % 8)) * 32 + (part == 7 ? size % 32 : 0);
                UNIT_ASSERT_VALUES_EQUAL(type.BlockSplitPartUsedSize(size, part), used);
                UNIT_ASSERT_VALUES_EQUAL(type.BlockSplitWholeOffset(size, part, 0), start);
                for (ui64 offset = 0; offset != used; ++offset) {
                    ui64 local = Max<ui64>();
                    UNIT_ASSERT_VALUES_EQUAL(type.BlockSplitPartIndex(start + offset, size, local), part);
                    UNIT_ASSERT_VALUES_EQUAL(local, offset);
                    UNIT_ASSERT_VALUES_EQUAL(type.BlockSplitWholeOffset(size, part, offset), start + offset);
                }
                start += used;
            }
            UNIT_ASSERT_VALUES_EQUAL(start, size);
        }
        TBlockSplitRange range;
        type.BlockSplitRange(NoCrc, 700, 90, 100, &range);
        UNIT_ASSERT_VALUES_EQUAL(range.BeginPartIdx, 0);
        UNIT_ASSERT_VALUES_EQUAL(range.EndPartIdx, 2);
        UNIT_ASSERT_VALUES_EQUAL(range.PartRanges[0].Begin, 90);
        UNIT_ASSERT_VALUES_EQUAL(range.PartRanges[0].End, 96);
        UNIT_ASSERT_VALUES_EQUAL(range.PartRanges[0].AlignedBegin, 64);
        UNIT_ASSERT_VALUES_EQUAL(range.PartRanges[0].AlignedEnd, 96);
        UNIT_ASSERT_VALUES_EQUAL(range.PartRanges[1].Begin, 0);
        UNIT_ASSERT_VALUES_EQUAL(range.PartRanges[1].End, 4);
        UNIT_ASSERT_VALUES_EQUAL(range.PartRanges[1].AlignedEnd, 32);
        type.BlockSplitRange(WholeCrc, 700, 0, 700, &range);
        for (ui32 i : {5, 6}) {
            UNIT_ASSERT_VALUES_EQUAL(range.PartRanges[i].End, 64);
            UNIT_ASSERT_VALUES_EQUAL(range.PartRanges[i].AlignedEnd, 96);
        }
        ui64 shift = 0, count = 0;
        type.AlignPartialDataRequest(97, 3, 700, shift, count);
        UNIT_ASSERT_VALUES_EQUAL(shift, 0);
        UNIT_ASSERT_VALUES_EQUAL(count, 32);
    }

    Y_UNIT_TEST(AllLossAndSelectedOutputCombinations) {
        const auto masks = LossMasks();
        UNIT_ASSERT_VALUES_EQUAL(masks.size(), 56);
        for (ui32 size : {1, 33, 257, 700, 1000, 4097}) {
            const TString data = Corpus(size);
            for (auto crc : {NoCrc, WholeCrc}) {
                const auto encoded = EncodeOracle(data, crc);
                ui32 nonemptyPlans = 0;
                for (ui32 missing : masks) {
                    for (ui32 output = missing;; output = (output - 1) & missing) {
                        nonemptyPlans += output != 0;
                        for (bool needWhole : {false, true}) {
                            auto parts = MakeSurvivors(encoded, missing, true);
                            std::array<const char*, 10> originalPointers{};
                            for (ui32 i = 0; i != 10; ++i) {
                                if (parts[i]) {
                                    originalPointers[i] = parts[i].Begin().ContiguousData();
                                }
                            }
                            TRope whole;
                            // Also request present outputs: they must remain unchanged.
                            const ui32 requested = output | (AllParts & ~missing);
                            ErasureRestore(crc, Species, size, needWhole ? &whole : nullptr, parts, requested);
                            for (ui32 i = 0; i != 10; ++i) {
                                if ((missing & ~output) & (1u << i)) {
                                    UNIT_ASSERT_C(!parts[i], "temporary output materialized: missing# " << missing
                                        << " output# " << output << " part# " << i);
                                } else {
                                    UNIT_ASSERT_VALUES_EQUAL_C(parts[i].ConvertToString(), encoded[i],
                                        "missing# " << missing << " output# " << output << " part# " << i);
                                    if (originalPointers[i]) {
                                        UNIT_ASSERT_EQUAL(parts[i].Begin().ContiguousData(), originalPointers[i]);
                                    }
                                }
                            }
                            if (needWhole) {
                                UNIT_ASSERT_VALUES_EQUAL(whole.ConvertToString(), data);
                            }
                        }
                        if (!output) {
                            // In particular, whole-only restore uses an exactly zero mask.
                            auto parts = MakeSurvivors(encoded, missing);
                            TRope whole;
                            ErasureRestore(crc, Species, size, &whole, parts, 0);
                            UNIT_ASSERT_VALUES_EQUAL(whole.ConvertToString(), data);
                            for (ui32 i = 0; i != 10; ++i) {
                                if (missing & (1u << i)) {
                                    UNIT_ASSERT(!parts[i]);
                                }
                            }
                            break;
                        }
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(nonemptyPlans, 145);
            }
        }
    }

    Y_UNIT_TEST(FragmentsOfArbitraryLength) {
        const TString data = Corpus(4097);
        for (auto crc : {NoCrc, WholeCrc}) {
            const auto encoded = EncodeOracle(data, crc);
            for (ui32 length : {1, 15, 16, 17, 31, 32, 33, 63, 64, 65}) {
                for (ui32 offset : {0, 1, 31, 32, 63, 127}) {
                    TBytes fragments;
                    for (ui32 i = 0; i != 10; ++i) {
                        fragments[i] = encoded[i].substr(offset, length);
                    }
                    for (ui32 missing : LossMasks()) {
                        for (ui32 output = missing; output; output = (output - 1) & missing) {
                            auto parts = MakeSurvivors(fragments, missing, true);
                            ErasureRestore(crc, Species, data.size(), nullptr, parts, output, offset, true);
                            for (ui32 i = 0; i != 10; ++i) {
                                if ((missing & ~output) & (1u << i)) {
                                    UNIT_ASSERT(!parts[i]);
                                } else {
                                    UNIT_ASSERT_VALUES_EQUAL(parts[i].ConvertToString(), fragments[i]);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(IncrementalAndFragmentedWithCustomAllocator) {
        for (ui32 quantum : {1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 256 * 1024}) {
            const TString data = Corpus(quantum == 256 * 1024 ? 4 * 1024 * 1024 + 37 : 16387);
            const TRope whole = Fragment(data);
            UNIT_ASSERT(!whole.IsContiguous());
            for (auto crc : {NoCrc, WholeCrc}) {
                TCountingAllocator allocator;
                TParts parts;
                auto context = TErasureSplitContext::Init(quantum);
                ui32 calls = 0;
                bool done;
                do {
                    const ui32 previous = context.Offset;
                    done = ErasureSplit(crc, Species, whole, parts, &context, &allocator);
                    UNIT_ASSERT(context.Offset > previous);
                    UNIT_ASSERT(context.Offset - previous <= quantum);
                    UNIT_ASSERT(++calls < 100000);
                } while (!done);
                UNIT_ASSERT(calls > 1);
                UNIT_ASSERT_VALUES_EQUAL(context.Offset, (data.size() + 255) / 256 * 32);
                UNIT_ASSERT(allocator.Calls > 0);
                UNIT_ASSERT(allocator.MaxAllocation < data.size());
                UNIT_ASSERT(!whole.IsContiguous());
                UNIT_ASSERT_EQUAL(parts[0].Begin().ContiguousData(), whole.Begin().ContiguousData());
                CheckBytes(whole.ConvertToString(), data);
                CheckParts(parts, EncodeOracle(data, crc));
            }
        }
    }

    Y_UNIT_TEST(LegacySplitAndRestoreUseSameFormat) {
        const TErasureType type(Species);
        for (ui32 size : {1, 31, 32, 33, 255, 256, 257, 700, 1000, 1024 * 1024 + 37}) {
            const TString data = Corpus(size);
            for (auto crc : {NoCrc, WholeCrc}) {
                const auto expected = EncodeOracle(data, crc);
                TDataPartSet split;
                type.SplitData(crc, data, split);
                UNIT_ASSERT_VALUES_EQUAL(split.PartsMask, AllParts);
                UNIT_ASSERT_VALUES_EQUAL(split.FullDataSize, size);
                UNIT_ASSERT_VALUES_EQUAL(split.Parts.size(), 10);
                for (ui32 i = 0; i != 10; ++i) {
                    CheckBytes(split.Parts[i].OwnedString.ConvertToString(), expected[i]);
                }
                TDataPartSet incremental;
                ui32 calls = 0;
                do {
                    type.IncrementalSplitData(crc, data, incremental);
                    UNIT_ASSERT(++calls < 10000);
                } while (!incremental.IsSplitDone());
                if (size > 1024 * 1024) {
                    UNIT_ASSERT(calls > 1);
                }
                for (ui32 i = 0; i != 10; ++i) {
                    CheckBytes(incremental.Parts[i].OwnedString.ConvertToString(), expected[i]);
                }
                for (ui32 missing : LossMasks()) {
                    TDataPartSet restore;
                    restore.FullDataSize = size;
                    restore.PartsMask = AllParts & ~missing;
                    restore.Parts.resize(10);
                    for (ui32 i = 0; i != 10; ++i) {
                        if (!(missing & (1u << i))) {
                            restore.Parts[i].ReferenceTo(expected[i]);
                        }
                    }
                    TRope whole;
                    type.RestoreData(crc, restore, whole, true, true, true);
                    CheckBytes(whole.ConvertToString(), data);
                    for (ui32 i = 0; i != 10; ++i) {
                        CheckBytes(restore.Parts[i].OwnedString.ConvertToString(), expected[i]);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(LegacyIncrementalRetainsOwnedBuffers) {
        const TErasureType type(Species);
        const TString data = Corpus(1024 * 1024 + 37);
        for (bool fragmented : {false, true}) {
            for (auto crc : {NoCrc, WholeCrc}) {
                TRope whole = fragmented ? Fragment(data) : TRope(data);
                TDataPartSet parts;
                type.IncrementalSplitData(crc, whole, parts);
                UNIT_ASSERT(!parts.IsSplitDone());
                std::array<const char*, 10> firstPointers;
                for (ui32 i = 0; i != 10; ++i) {
                    firstPointers[i] = parts.Parts[i].Bytes;
                }
                ui32 calls = 1;
                do {
                    type.IncrementalSplitData(crc, whole, parts);
                    for (ui32 i = 0; i != 10; ++i) {
                        UNIT_ASSERT_C(parts.Parts[i].Bytes == firstPointers[i],
                            "incremental split reallocated part# " << i << " call# " << calls);
                        UNIT_ASSERT(parts.Parts[i].OwnedString.IsContiguous());
                        UNIT_ASSERT(parts.Parts[i].Bytes == parts.Parts[i].OwnedString.Begin().ContiguousData());
                    }
                    UNIT_ASSERT(++calls < 100);
                } while (!parts.IsSplitDone());
                UNIT_ASSERT(calls > 2);
                const auto expected = EncodeOracle(data, crc);
                for (ui32 i = 0; i != 10; ++i) {
                    CheckBytes(parts.Parts[i].OwnedString.ConvertToString(), expected[i]);
                }
                CheckBytes(whole.ConvertToString(), data);
            }
        }
    }

    Y_UNIT_TEST(LegacyFragmentRestore) {
        const TErasureType type(Species);
        const TString data = Corpus(4097);
        for (auto crc : {NoCrc, WholeCrc}) {
            const auto expected = EncodeOracle(data, crc);
            for (ui32 missing : LossMasks()) {
                TDataPartSet parts;
                parts.FullDataSize = data.size();
                parts.PartsMask = AllParts & ~missing;
                parts.IsFragment = true;
                parts.Parts.resize(10);
                constexpr ui32 offset = 17, length = 33;
                for (ui32 i = 0; i != 10; ++i) {
                    if (!(missing & (1u << i))) {
                        parts.Parts[i].ReferenceTo(expected[i].substr(offset, length), offset, length, expected[i].size());
                    }
                }
                type.RestoreData(crc, parts, true, false, true);
                for (ui32 i = 0; i != 10; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(parts.Parts[i].OwnedString.ConvertToString(), expected[i].substr(offset, length));
                    UNIT_ASSERT_VALUES_EQUAL(parts.Parts[i].Offset, offset);
                    UNIT_ASSERT_VALUES_EQUAL(parts.Parts[i].Size, length);
                    UNIT_ASSERT_VALUES_EQUAL(parts.Parts[i].PartSize, expected[i].size());
                }
            }
        }
    }

#if defined(_unix_)
    Y_UNIT_TEST(InvalidInputsAbortBeforeCodecAccess) {
        const TString data = Corpus(1000);
        const auto expected = EncodeOracle(data, NoCrc);
        AssertAborts([&] {
            auto parts = MakeSurvivors(expected, 7);
            TRope whole;
            ErasureRestore(NoCrc, Species, data.size(), &whole, parts, 7);
        });
        AssertAborts([&] {
            auto parts = MakeSurvivors(expected, 1);
            parts[1] = TRope(expected[1].substr(1));
            ErasureRestore(NoCrc, Species, data.size(), nullptr, parts, 1);
        });
        AssertAborts([&] {
            auto parts = MakeSurvivors(expected, 0);
            ErasureRestore(NoCrc, Species, data.size(), nullptr, parts, 1u << 10);
        });
        AssertAborts([&] {
            auto parts = MakeSurvivors(expected, 1);
            ErasureRestore(NoCrc, Species, data.size(), nullptr, parts, 1, 1, true);
        });
        AssertAborts([&] {
            auto parts = MakeSurvivors(expected, 1);
            TRope whole;
            ErasureRestore(NoCrc, Species, data.size(), &whole, parts, 1, 0, true);
        });
        AssertAborts([&] {
            auto parts = MakeSurvivors(EncodeOracle(data, WholeCrc), 1);
            ErasureRestore(WholeCrc, Species, data.size(), nullptr, parts, 1, 0, true);
        });
        AssertAborts([&] {
            TParts parts;
            auto context = TErasureSplitContext::Init(0);
            ErasureSplit(NoCrc, Species, TRope(data), parts, &context, GetDefaultRcBufAllocator());
        });
        AssertAborts([&] {
            std::array<TRope, 9> parts;
            ErasureSplit(NoCrc, Species, TRope(data), parts, nullptr, GetDefaultRcBufAllocator());
        });
        AssertAborts([&] {
            TParts parts;
            const TRope whole(data);
            auto context = TErasureSplitContext::Init(1);
            ErasureSplit(NoCrc, Species, whole, parts, &context, GetDefaultRcBufAllocator());
            parts[8] = TRope(TString("x"));
            ErasureSplit(NoCrc, Species, whole, parts, &context, GetDefaultRcBufAllocator());
        });
        const auto legacySources = [&] {
            TDataPartSet parts;
            parts.FullDataSize = data.size();
            parts.PartsMask = AllParts & ~1u;
            parts.Parts.resize(10);
            for (ui32 i = 1; i != 10; ++i) {
                parts.Parts[i].ReferenceTo(expected[i]);
            }
            return parts;
        };
        AssertAborts([&] {
            auto parts = legacySources();
            --parts.Parts[1].PartSize;
            TErasureType(Species).RestoreData(NoCrc, parts, true, false, true);
        });
        AssertAborts([&] {
            auto parts = legacySources();
            ++parts.Parts[1].Bytes;
            TErasureType(Species).RestoreData(NoCrc, parts, true, false, true);
        });
    }
#endif
}
