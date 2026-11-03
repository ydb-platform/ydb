#include "block_checksums.h"

#include <library/cpp/testing/unittest/registar.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString MakeBytes(size_t size)
{
    TString bytes(size, '\0');
    for (size_t i = 0; i < bytes.size(); ++i) {
        bytes[i] = static_cast<char>(i * 17 + 3);
    }
    return bytes;
}

TBlockChecksums ChecksumsOf(const TString& bytes)
{
    const TSgList sglist = {TBlockDataRef(bytes.data(), bytes.size())};
    return CalculateBlockChecksums(sglist);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockChecksumsTest)
{
    Y_UNIT_TEST(ShouldMatchXxh3ForOneUnit)
    {
        const TString bytes = MakeBytes(ChecksumUnitSize);
        const TBlockChecksums checksums = ChecksumsOf(bytes);

        UNIT_ASSERT_VALUES_EQUAL(1u, checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(
            XXH3_64bits(bytes.data(), bytes.size()),
            checksums[0]);
    }

    Y_UNIT_TEST(ShouldNotDependOnBufferBoundaries)
    {
        constexpr size_t unitCount = 3;
        const TString bytes = MakeBytes(unitCount * ChecksumUnitSize);
        const TBlockChecksums contiguous = ChecksumsOf(bytes);

        UNIT_ASSERT_VALUES_EQUAL(unitCount, contiguous.size());
        for (size_t i = 0; i < unitCount; ++i) {
            const ui64 expected = XXH3_64bits(
                bytes.data() + i * ChecksumUnitSize,
                ChecksumUnitSize);
            UNIT_ASSERT_VALUES_EQUAL(expected, contiguous[i]);
        }

        // 1000 + 5000 + 6288 = 12288 = 3 * 4 KiB. The cuts fall inside units,
        // so the hash has to stream across buffers. The last unit sits in one
        // buffer and takes the single-call path.
        const TString part0 = bytes.substr(0, 1000);
        const TString part1 = bytes.substr(1000, 5000);
        const TString part2 = bytes.substr(6000, 6288);
        const TSgList fragmented = {
            TBlockDataRef(part0.data(), part0.size()),
            TBlockDataRef(part1.data(), part1.size()),
            TBlockDataRef(part2.data(), part2.size()),
        };

        UNIT_ASSERT_VALUES_EQUAL(
            contiguous,
            CalculateBlockChecksums(fragmented));
    }

    Y_UNIT_TEST(ShouldEmitTwoChecksumsForEightKilobyteBlock)
    {
        const TString bytes = MakeBytes(8_KB);
        const TBlockChecksums checksums = ChecksumsOf(bytes);

        UNIT_ASSERT_VALUES_EQUAL(2u, checksums.size());
        UNIT_ASSERT_VALUES_EQUAL(
            XXH3_64bits(bytes.data(), ChecksumUnitSize),
            checksums[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            XXH3_64bits(bytes.data() + ChecksumUnitSize, ChecksumUnitSize),
            checksums[1]);
    }

    Y_UNIT_TEST(ShouldStreamAcrossSmallBuffers)
    {
        constexpr size_t segmentSize = 512;
        const TString bytes = MakeBytes(2 * ChecksumUnitSize);
        const TBlockChecksums contiguous = ChecksumsOf(bytes);

        TVector<TString> parts;
        for (size_t offset = 0; offset < bytes.size(); offset += segmentSize) {
            parts.push_back(bytes.substr(offset, segmentSize));
        }

        // 512-byte pieces, so each 4 KiB unit spans 8 buffers. The two-arg
        // TBlockDataRef constructor rejects a null pointer, so the empty
        // buffers are default block refs: a null data pointer and size 0.
        // One sits in the middle of the first unit, and one at each end.
        TSgList fragmented;
        fragmented.push_back(TBlockDataRef());
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i == 4) {
                fragmented.push_back(TBlockDataRef());
            }
            fragmented.push_back(
                TBlockDataRef(parts[i].data(), parts[i].size()));
        }
        fragmented.push_back(TBlockDataRef());

        UNIT_ASSERT_VALUES_EQUAL(
            contiguous,
            CalculateBlockChecksums(fragmented));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
