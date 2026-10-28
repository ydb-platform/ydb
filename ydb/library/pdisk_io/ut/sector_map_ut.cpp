#include <ydb/library/pdisk_io/sector_map.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/pdisk_io/protos/sector_map.pb.h>
#include <util/folder/tempdir.h>

namespace NKikimr::NPDisk {
namespace {
constexpr ui32 SectorSize = NSectorMap::SECTOR_SIZE;
constexpr std::array Compressions{NSectorMap::ECompression::Lz4,
    NSectorMap::ECompression::Snappy, NSectorMap::ECompression::None};

TString SectorData(ui64 seed) {
    TString result = TString::Uninitialized(SectorSize);
    char* dst = result.Detach();
    for (ui32 i = 0; i < SectorSize; ++i) {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        dst[i] = static_cast<char>(seed);
    }
    return result;
}

void CheckSector(TSectorMap& map, ui32 index, const TString& expected) {
    // Exercise an unaligned destination and check both boundaries of the output slice.
    TString actual(SectorSize + 2, '\x5a');
    UNIT_ASSERT(map.Read(reinterpret_cast<ui8*>(actual.Detach() + 1), SectorSize, ui64(index) * SectorSize));
    UNIT_ASSERT_VALUES_EQUAL(actual.front(), '\x5a');
    UNIT_ASSERT_VALUES_EQUAL(actual.back(), '\x5a');
    UNIT_ASSERT_VALUES_EQUAL(actual.substr(1, SectorSize), expected);
}
}

Y_UNIT_TEST_SUITE(TSectorMapCompression) {
    Y_UNIT_TEST(IndependentSectorsAfterStateReuse) {
        for (const auto compression : Compressions) {
            constexpr ui32 Count = 64;
            TSectorMap map(ui64(Count) * SectorSize, NSectorMap::DM_NONE, compression);
            TVector<TString> expected(Count);
            const TString random = SectorData(42);
            const TString zeros(SectorSize, '\0');
            TString batch;
            // Adjacent identical incompressible blocks expose accidental cross-sector dictionaries.
            for (ui32 i = 0; i < Count; ++i) {
                expected[i] = random;
                batch += random;
            }
            UNIT_ASSERT(map.Write(reinterpret_cast<const ui8*>(batch.data()), batch.size(), 0));
            for (ui32 i = 0; i < Count; ++i) {
                const ui32 index = (i * 37) % Count;
                CheckSector(map, index, expected[index]);
            }
            // Exercise repeated resets, including the compressor's offset rollover, with bounded storage.
            for (ui32 i = 0; i < 18000; ++i) {
                const ui32 index = (i * 37) % Count;
                TString data = i % 3 == 0 ? zeros : i % 3 == 1 ? random : SectorData(i + 1);
                UNIT_ASSERT(map.Write(reinterpret_cast<const ui8*>(data.data()), data.size(), ui64(index) * SectorSize));
                expected[index] = std::move(data);
                if (i % 127 == 0) {
                    for (ui32 j = 0; j < Count; ++j) {
                        const ui32 shuffled = (j * 37) % Count;
                        CheckSector(map, shuffled, expected[shuffled]);
                    }
                }
            }
            for (ui32 i = 0; i < Count; ++i) {
                CheckSector(map, i, expected[i]);
            }
            UNIT_ASSERT_VALUES_EQUAL(map.DataBytes(), ui64(Count) * SectorSize);
            map.Trim(ui64(Count) * SectorSize, 0);
            UNIT_ASSERT_VALUES_EQUAL(map.AllocatedBytes.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(map.DataBytes(), 0);
            UNIT_ASSERT(map.Write(reinterpret_cast<const ui8*>(random.data()), random.size(), 0));
            CheckSector(map, 0, random);

        }
    }

    Y_UNIT_TEST(ReplayVersionsRemainIndependent) {
        for (const auto compression : Compressions) {
            TSectorMap map(2 * SectorSize, NSectorMap::DM_NONE, compression);
            const TString first = SectorData(1), second = SectorData(2), third = SectorData(3);
            UNIT_ASSERT(map.Write(reinterpret_cast<const ui8*>(first.data()), first.size(), 0));
            map.FailureProbabilities.ReadReplayProbability = 1.0;
            UNIT_ASSERT(map.Write(reinterpret_cast<const ui8*>(second.data()), second.size(), 0));
            CheckSector(map, 0, first);
            UNIT_ASSERT(map.Write(reinterpret_cast<const ui8*>(third.data()), third.size(), 0));
            CheckSector(map, 0, second);
            map.FailureProbabilities.ReadReplayProbability = 0.0;
            CheckSector(map, 0, third);
            UNIT_ASSERT(map.Write(reinterpret_cast<const ui8*>(first.data()), first.size(), 0));
            CheckSector(map, 0, first);
            map.Trim(SectorSize, 0);
            UNIT_ASSERT_VALUES_EQUAL(map.AllocatedBytes.load(), 0);

        }
    }
    Y_UNIT_TEST(CompressionSelectedByPath) {
        using E = NSectorMap::ECompression;
        for (const TString& path : {"", "/tmp/pdisk", "SectorMap:test", "SectorMap:test:100",
                "SectorMap:test:100:NVME", "SectorMap:test:100:NVME:lz4"}) {
            UNIT_ASSERT(NSectorMap::TryGetCompressionFromPath(path) == E::Lz4);
        }
        UNIT_ASSERT(NSectorMap::TryGetCompressionFromPath("SectorMap:test:100:NVME:snappy") == E::Snappy);
        UNIT_ASSERT(NSectorMap::TryGetCompressionFromPath("SectorMap:test:100:NONE:none") == E::None);
        UNIT_ASSERT(!NSectorMap::TryGetCompressionFromPath("SectorMap:test:100:NVME:unknown"));
        UNIT_ASSERT(!NSectorMap::TryGetCompressionFromPath("SectorMap:test:100:NVME:"));
        UNIT_ASSERT(!NSectorMap::TryGetCompressionFromPath("SectorMap:test:100:NVME:none:extra"));
    }

    Y_UNIT_TEST(SnapshotsCompatibleAcrossCodecs) {
        TTempDir dir;
        const TString path = TString(dir.Name()) + "/snapshot";
        const TString first = SectorData(123), latest = SectorData(456);
        for (const auto sourceCodec : Compressions) {
            TSectorMap source(3 * SectorSize, NSectorMap::DM_NONE, sourceCodec);
            UNIT_ASSERT(source.Write(reinterpret_cast<const ui8*>(first.data()), SectorSize, 0));
            source.FailureProbabilities.ReadReplayProbability = 1.0;
            UNIT_ASSERT(source.Write(reinterpret_cast<const ui8*>(latest.data()), SectorSize, 0));
            UNIT_ASSERT(source.Write(reinterpret_cast<const ui8*>(first.data()), SectorSize, 2 * SectorSize));
            source.StoreToFile(path);

            // A legacy LZ4-only reader can decode every exported sector.
            NKikimrPDisk::TSectorMapSnapshot snap;
            UNIT_ASSERT(snap.ParseFromString(TFileInput(path).ReadAll()));
            UNIT_ASSERT_VALUES_EQUAL(snap.SectorsSize(), 2);
            for (const auto& sector : snap.GetSectors()) {
                UNIT_ASSERT(sector.GetCompressionType() == NKikimrPDisk::LZ4);
                TString data = TString::Uninitialized(SectorSize);
                UNIT_ASSERT_VALUES_EQUAL(LZ4_decompress_safe(sector.GetData().data(), data.Detach(),
                    sector.GetData().size(), SectorSize), SectorSize);
                UNIT_ASSERT_VALUES_EQUAL(data, sector.GetOffset() == 0 ? latest : first);
            }
            for (const auto targetCodec : Compressions) {
                TSectorMap target(3 * SectorSize, NSectorMap::DM_NONE, targetCodec);
                target.LoadFromFile(path);
                CheckSector(target, 0, latest);
                CheckSector(target, 1, TString(SectorSize, '\x33'));
                CheckSector(target, 2, first);
            }
        }
    }

}
}
