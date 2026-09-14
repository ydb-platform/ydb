#include "block_field_serializer.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

void AssertFieldsEqual(
    const TBlockRangeField& expected,
    const TBlockRangeField& actual)
{
    UNIT_ASSERT_VALUES_EQUAL(expected.Print(), actual.Print());
    UNIT_ASSERT_VALUES_EQUAL(expected.GetBlockCount(), actual.GetBlockCount());
}

void AssertRestored(
    const TBlockRangeField& source,
    const TBlockFieldProto& proto)
{
    TBlockRangeField restored(CreateArenaAllocator());
    LoadBlockField(proto, &restored);
    AssertFieldsEqual(source, restored);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockFieldSerializerTest)
{
    Y_UNIT_TEST(ShouldSaveAndLoadEmptyField)
    {
        TBlockRangeField source(CreateArenaAllocator());

        TBlockFieldProto proto;
        SaveBlockField(source, &proto);

        UNIT_ASSERT(
            proto.GetEncodingCase() == TBlockFieldProto::ENCODING_NOT_SET);
        AssertRestored(source, proto);
    }

    Y_UNIT_TEST(ShouldSaveSparseRangesWithRunLengthEncoding)
    {
        TBlockRangeField source(CreateArenaAllocator());
        source.Add(TBlockRange16::WithLength(16, 25));
        source.Add(TBlockRange16::WithLength(42, 5));

        TBlockFieldProto proto;
        SaveBlockField(source, &proto);

        UNIT_ASSERT(
            proto.GetEncodingCase() == TBlockFieldProto::kRunLengthEncoding);
        const auto& encoding = proto.GetRunLengthEncoding();
        UNIT_ASSERT_VALUES_EQUAL(4, encoding.size());
        UNIT_ASSERT_VALUES_EQUAL(16, static_cast<ui8>(encoding[0]));
        UNIT_ASSERT_VALUES_EQUAL(25, static_cast<ui8>(encoding[1]));
        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<ui8>(encoding[2]));
        UNIT_ASSERT_VALUES_EQUAL(5, static_cast<ui8>(encoding[3]));

        AssertRestored(source, proto);
    }

    Y_UNIT_TEST(ShouldEncodeLongRunLengths)
    {
        TBlockRangeField source(CreateArenaAllocator());
        source.Add(TBlockRange16::WithLength(255, 510));

        TBlockFieldProto proto;
        SaveBlockField(source, &proto);

        const auto& encoding = proto.GetRunLengthEncoding();
        UNIT_ASSERT_VALUES_EQUAL(5, encoding.size());
        UNIT_ASSERT_VALUES_EQUAL(255, static_cast<ui8>(encoding[0]));
        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<ui8>(encoding[1]));
        UNIT_ASSERT_VALUES_EQUAL(255, static_cast<ui8>(encoding[2]));
        UNIT_ASSERT_VALUES_EQUAL(255, static_cast<ui8>(encoding[3]));
        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<ui8>(encoding[4]));

        AssertRestored(source, proto);
    }

    Y_UNIT_TEST(ShouldSaveFragmentedRangesWithBitMask)
    {
        TBlockRangeField source(CreateArenaAllocator());
        for (ui64 blockIndex = 0; blockIndex < MaxVChunkBlockCount;
             blockIndex += 2)
        {
            source.Add(TBlockRange16::WithLength(blockIndex, 1));
        }

        TBlockFieldProto proto;
        SaveBlockField(source, &proto);

        UNIT_ASSERT(proto.GetEncodingCase() == TBlockFieldProto::kBitMask);
        UNIT_ASSERT_VALUES_EQUAL(
            MaxVChunkBlockCount / 8,
            proto.GetBitMask().size());
        for (const char byte: proto.GetBitMask()) {
            UNIT_ASSERT_VALUES_EQUAL(0x55, static_cast<ui8>(byte));
        }

        TBlockRangeField target(CreateArenaAllocator());
        LoadBlockField(proto, &target);

        AssertFieldsEqual(source, target);
        UNIT_ASSERT_VALUES_EQUAL(
            MaxVChunkBlockCount / 2,
            target.GetBlockCount());
        UNIT_ASSERT(target.Overlaps(TBlockRange16::WithLength(0, 1)));
        UNIT_ASSERT(!target.Overlaps(TBlockRange16::WithLength(1, 1)));
        UNIT_ASSERT(target.Overlaps(
            TBlockRange16::WithLength(MaxVChunkBlockCount - 2, 1)));
    }

    Y_UNIT_TEST(ShouldUseCurrentBackendEncoding)
    {
        TBlockRangeField field(CreateArenaAllocator());
        field.Add(TBlockRange16::WithLength(0, 1));
        field.Add(TBlockRange16::WithLength(2, 1));

        TBlockFieldProto proto;
        SaveBlockField(field, &proto);
        UNIT_ASSERT(
            proto.GetEncodingCase() == TBlockFieldProto::kRunLengthEncoding);
        AssertRestored(field, proto);

        TBlockRangeField bitmapField(
            CreateArenaAllocator(),
            MaxVChunkBlockCount,
            TBlockRangeField::EBackend::Bitmask);
        bitmapField.Add(TBlockRange16::WithLength(0, 1));
        bitmapField.Add(TBlockRange16::WithLength(2, 1));
        SaveBlockField(bitmapField, &proto);
        UNIT_ASSERT(proto.GetEncodingCase() == TBlockFieldProto::kBitMask);
        AssertRestored(bitmapField, proto);
    }

    Y_UNIT_TEST(ShouldUseRuntimeBlockCountToChooseEncoding)
    {
        constexpr ui64 blockCount = 2048;

        TBlockRangeField field(
            CreateArenaAllocator(),
            blockCount,
            TBlockRangeField::EBackend::Bitmask);
        for (ui64 segmentIndex = 0; segmentIndex < 32; ++segmentIndex) {
            field.Add(TBlockRange16::WithLength(segmentIndex * 16, 1));
        }

        TBlockFieldProto proto;
        SaveBlockField(field, &proto);

        UNIT_ASSERT(proto.GetEncodingCase() == TBlockFieldProto::kBitMask);
        UNIT_ASSERT(proto.GetBitMask().size() <= blockCount / 8);
        AssertRestored(field, proto);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
