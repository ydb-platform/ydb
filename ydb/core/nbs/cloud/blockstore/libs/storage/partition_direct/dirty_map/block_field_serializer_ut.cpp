#include "block_field_serializer.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockFieldSerializerTest)
{
    Y_UNIT_TEST(ShouldSaveSparseRangesWithRunLengthEncoding)
    {
        TBlockRangeField source;
        source.Add(TBlockRange64::WithLength(16, 25));
        source.Add(TBlockRange64::WithLength(42, 5));

        TBlockFieldProto proto;
        SaveBlockField(source, MaxVChunkBlockCount, &proto);

        UNIT_ASSERT(
            proto.GetEncodingCase() == TBlockFieldProto::kRunLengthEncoding);
        const auto& encoding = proto.GetRunLengthEncoding();
        UNIT_ASSERT_VALUES_EQUAL(4, encoding.size());
        UNIT_ASSERT_VALUES_EQUAL(16, static_cast<ui8>(encoding[0]));
        UNIT_ASSERT_VALUES_EQUAL(25, static_cast<ui8>(encoding[1]));
        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<ui8>(encoding[2]));
        UNIT_ASSERT_VALUES_EQUAL(5, static_cast<ui8>(encoding[3]));

        TBlockRangeField target;
        LoadBlockField(proto, &target);

        UNIT_ASSERT_VALUES_EQUAL(source.Print(), target.Print());
    }

    Y_UNIT_TEST(ShouldEncodeLongRunLengths)
    {
        TBlockRangeField source;
        source.Add(TBlockRange64::WithLength(255, 510));

        TBlockFieldProto proto;
        SaveBlockField(source, MaxVChunkBlockCount, &proto);

        const auto& encoding = proto.GetRunLengthEncoding();
        UNIT_ASSERT_VALUES_EQUAL(5, encoding.size());
        UNIT_ASSERT_VALUES_EQUAL(255, static_cast<ui8>(encoding[0]));
        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<ui8>(encoding[1]));
        UNIT_ASSERT_VALUES_EQUAL(255, static_cast<ui8>(encoding[2]));
        UNIT_ASSERT_VALUES_EQUAL(255, static_cast<ui8>(encoding[3]));
        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<ui8>(encoding[4]));

        TBlockRangeField target;
        LoadBlockField(proto, &target);

        UNIT_ASSERT_VALUES_EQUAL(source.Print(), target.Print());
    }

    Y_UNIT_TEST(ShouldSaveFragmentedRangesWithBitMask)
    {
        TBlockRangeField source;
        for (ui64 blockIndex = 0; blockIndex < MaxVChunkBlockCount;
             blockIndex += 2)
        {
            source.Add(TBlockRange64::WithLength(blockIndex, 1));
        }

        TBlockFieldProto proto;
        SaveBlockField(source, MaxVChunkBlockCount, &proto);

        UNIT_ASSERT(proto.GetEncodingCase() == TBlockFieldProto::kBitMask);
        UNIT_ASSERT_VALUES_EQUAL(
            MaxVChunkBlockCount / 8,
            proto.GetBitMask().size());
        for (const char byte: proto.GetBitMask()) {
            UNIT_ASSERT_VALUES_EQUAL(0x55, static_cast<ui8>(byte));
        }

        TBlockRangeField target;
        LoadBlockField(proto, &target);

        UNIT_ASSERT_VALUES_EQUAL(
            MaxVChunkBlockCount / 2,
            target.GetSegmentCount());
        UNIT_ASSERT_VALUES_EQUAL(
            MaxVChunkBlockCount / 2,
            target.GetBlockCount());
        UNIT_ASSERT(target.Overlaps(TBlockRange64::WithLength(0, 1)));
        UNIT_ASSERT(!target.Overlaps(TBlockRange64::WithLength(1, 1)));
        UNIT_ASSERT(target.Overlaps(
            TBlockRange64::WithLength(MaxVChunkBlockCount - 2, 1)));
    }

    Y_UNIT_TEST(ShouldChooseEncodingAtSegmentThreshold)
    {
        constexpr size_t segmentThreshold =
            (MaxVChunkBlockCount / 8 - MaxVChunkBlockCount / 0xff) / 2;

        TBlockRangeField field;
        for (ui64 segmentIndex = 0; segmentIndex < segmentThreshold;
             ++segmentIndex)
        {
            field.Add(TBlockRange64::WithLength(segmentIndex * 2, 1));
        }

        TBlockFieldProto proto;
        SaveBlockField(field, MaxVChunkBlockCount, &proto);
        UNIT_ASSERT(
            proto.GetEncodingCase() == TBlockFieldProto::kRunLengthEncoding);

        field.Add(TBlockRange64::WithLength(segmentThreshold * 2, 1));
        SaveBlockField(field, MaxVChunkBlockCount, &proto);
        UNIT_ASSERT(proto.GetEncodingCase() == TBlockFieldProto::kBitMask);
    }

    Y_UNIT_TEST(ShouldUseRuntimeBlockCountToChooseEncoding)
    {
        constexpr ui64 blockCount = 512;

        TBlockRangeField field;
        for (ui64 segmentIndex = 0; segmentIndex < 32; ++segmentIndex) {
            field.Add(TBlockRange64::WithLength(segmentIndex * 16, 1));
        }

        TBlockFieldProto proto;
        SaveBlockField(field, blockCount, &proto);

        UNIT_ASSERT(proto.GetEncodingCase() == TBlockFieldProto::kBitMask);
        UNIT_ASSERT(proto.GetBitMask().size() <= blockCount / 8);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
