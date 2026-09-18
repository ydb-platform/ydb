#include "blobstorage_groupinfo.h"

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <bit>

using namespace NKikimr;

namespace {
using TPartsData = TBlobStorageGroupInfo::IDataIntegrityChecker::TPartsData;

struct TCheckerFixture {
    TBlobStorageGroupInfo Info;
    TLogoBlobID Id;
    std::array<TRope, MaxTotalPartCount> Parts;
    std::array<TRope, MaxTotalPartCount> OtherParts;

    TCheckerFixture(TErasureType::EErasureSpecies species, TErasureType::ECrcMode crc, ui32 size)
        : Info(species, 1, TBlobStorageGroupType(species).BlobSubgroupSize(), 1)
        , Id(1, 1, 1, 0, size, 0, 0, crc)
    {
        TString data(size, '\0');
        for (ui32 i = 0; i < size; ++i) {
            data[i] = static_cast<char>((i * 113 + i / 7 + 29) & 255);
        }
        const ui32 total = Info.Type.TotalPartCount();
        UNIT_ASSERT(ErasureSplit(crc, Info.Type, TRope(data), std::span<TRope>(Parts.data(), total), nullptr, GetDefaultRcBufAllocator()));
        ui32 random = 0x9e3779b9;
        for (char& c : data) {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            c = static_cast<char>(random);
        }
        UNIT_ASSERT(ErasureSplit(crc, Info.Type, TRope(data), std::span<TRope>(OtherParts.data(), total), nullptr, GetDefaultRcBufAllocator()));
    }

    TPartsData Make(ui32 mask) const {
        TPartsData data;
        data.Parts.resize(Info.Type.TotalPartCount());
        for (ui32 part = 0; part < data.Parts.size(); ++part) {
            if (mask >> part & 1u) {
                data.Parts[part].emplace_back(part, Parts[part]);
            }
        }
        return data;
    }

    auto Check(const TPartsData& data) const {
        return Info.GetTopology().GetDataIntegrityChecker().GetDataState(Id, data, '\n');
    }
};

void CheckCodewords(TErasureType::EErasureSpecies species) {
    for (auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
        for (ui32 size : {0u, 1u, 255u, 256u, 257u, 4097u}) {
            TCheckerFixture f(species, crc, size);
            const ui32 total = f.Info.Type.TotalPartCount();
            const ui32 required = f.Info.Type.DataParts();
            const ui32 full = (1u << total) - 1;
            // All availability sets cover systematic/parity bases, K+1, K and
            // less than K; presence is independent of empty rope payloads.
            for (ui32 mask = 0; mask <= full; ++mask) {
                auto data = f.Make(mask);
                const auto state = f.Check(data);
                UNIT_ASSERT_C(state.IsOk, state.DataInfo);
                if (ui32(std::popcount(mask)) <= required) {
                    UNIT_ASSERT(state.DataInfo.Contains("No independent redundancy"));
                }
                if (mask) {
                    const ui32 part = std::countr_zero(mask);
                    data.Parts[part].emplace_back(total, f.Parts[part]);
                    data.Parts[part].emplace_back(total + 1, f.Parts[part]);
                    UNIT_ASSERT_C(f.Check(data).IsOk, "equal copies must be accepted");
                }
            }
            if (size < 256) {
                continue;
            }
            for (ui32 part = 0; part < total; ++part) {
                UNIT_ASSERT_C(TRope::Compare(f.Parts[part], f.OtherParts[part]),
                    "replacement corpus must differ for part " << part + 1);
                auto data = f.Make(full);
                // Independently encoded bytes retain a valid WholePart CRC:
                // rejection here must come from the parity equation.
                data.Parts[part][0].second = f.OtherParts[part];
                UNIT_ASSERT_C(!f.Check(data).IsOk, "inconsistent codeword part " << part + 1);
                // K+1 observed parts still provide independent redundancy.
                const ui32 omit = (part + 1) % total;
                data.Parts[omit].clear();
                UNIT_ASSERT_C(!f.Check(data).IsOk, "K+1 inconsistent codeword part " << part + 1);

                // With only K independent parts, a different CRC-valid part
                // defines another possible codeword. Do not claim detection.
                const ui32 basisMask = full & ~(1u << ((part + 1) % total)) & ~(1u << ((part + 2) % total));
                data = f.Make(basisMask);
                data.Parts[part][0].second = f.OtherParts[part];
                UNIT_ASSERT(f.Check(data).IsOk);
                UNIT_ASSERT(f.Check(data).DataInfo.Contains("No independent redundancy"));

                data = f.Make(1u << part);
                data.Parts[part].emplace_back(total, f.OtherParts[part]);
                UNIT_ASSERT_C(!f.Check(data).IsOk, "unequal copies below K");

                data = f.Make(full);
                TString bad = f.Parts[part].ConvertToString();
                bad.Detach()[bad.size() / 2] ^= 1;
                data.Parts[part][0].second = TRope(bad);
                UNIT_ASSERT_C(!f.Check(data).IsOk, "corrupted bytes part " << part + 1);
            }
        }
    }
}

void CheckMalformed(TErasureType::EErasureSpecies species) {
    for (auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
        TCheckerFixture f(species, crc, 4097);
        const ui32 total = f.Info.Type.TotalPartCount();
        const ui32 full = (1u << total) - 1;
        auto data = f.Make(full);
        data.Parts.pop_back();
        UNIT_ASSERT(!f.Check(data).IsOk);
        data = f.Make(full);
        data.Parts.emplace_back();
        UNIT_ASSERT(!f.Check(data).IsOk);
        for (ui32 part = 0; part < total; ++part) {
            for (size_t size : {size_t(0), size_t(1), size_t(4), f.Parts[part].size() - 1, f.Parts[part].size() + 1}) {
                data = f.Make(full);
                data.Parts[part][0].second = TRope(TString(size, 'x'));
                UNIT_ASSERT_C(!f.Check(data).IsOk, "invalid size must be a controlled error");
            }
        }
        for (ui32 crcMode : {2u, 3u}) {
            const TLogoBlobID badId(1, 1, 1, 0, 4097, 0, 0, crcMode);
            const auto state = f.Info.GetTopology().GetDataIntegrityChecker().GetDataState(badId, f.Make(full), '\n');
            UNIT_ASSERT(!state.IsOk);
        }
        if (crc == TErasureType::CrcModeWholePart) {
            TCheckerFixture empty(species, crc, 0);
            for (ui32 part = 0; part < total; ++part) {
                UNIT_ASSERT_VALUES_EQUAL(empty.Parts[part].size(), sizeof(ui32));
                for (ui32 mask : {1u << part, full}) {
                    data = empty.Make(mask);
                    TString bad = empty.Parts[part].ConvertToString();
                    bad.Detach()[0] ^= 1;
                    data.Parts[part][0].second = TRope(bad);
                    const auto state = empty.Check(data);
                    UNIT_ASSERT(!state.IsOk);
                    UNIT_ASSERT(state.DataInfo.Contains("invalid CRC"));
                }
                for (size_t size : {0u, 1u, 2u, 3u, 5u}) {
                    data = empty.Make(1u << part);
                    data.Parts[part][0].second = TRope(TString(size, '\0'));
                    UNIT_ASSERT(!empty.Check(data).IsOk);
                }
            }
        }
    }
}
}

Y_UNIT_TEST_SUITE(TParityBlockIntegrity) {
    Y_UNIT_TEST(CodewordsBlock42) { CheckCodewords(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(CodewordsBlock82) { CheckCodewords(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(MalformedBlock42) { CheckMalformed(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(MalformedBlock82) { CheckMalformed(TErasureType::Erasure8Plus2Block); }
}
