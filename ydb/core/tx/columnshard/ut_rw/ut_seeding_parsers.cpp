#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>
#include <ydb/core/tx/columnshard/normalizer/portion/batch_cursor.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NOlap;

namespace {

// Binary form suitable for TryParseBlobIds (uses AsBinaryString / FromBinary round-trip).
TString MakeBlobIdsBinary() {
    TLogoBlobID id(1, 2, 3, 0, 100, 0);
    return id.AsBinaryString();   // HostToInet-encoded, as stored in the proto
}

// Binary form suitable for TryParseBlobAddress (raw ui64 array, as stored in the Blob column).
TString MakeBlobAddressBinary() {
    TLogoBlobID id(1, 2, 3, 0, 100, 0);
    return TString(reinterpret_cast<const char*>(id.GetRaw()), sizeof(TLogoBlobID));
}

// Build a serialised TIndexPortionBlobsInfo with one valid blob.
TString MakeValidBlobsProto() {
    NKikimrTxColumnShard::TIndexPortionBlobsInfo proto;
    proto.AddBlobIds(MakeBlobIdsBinary());
    return proto.SerializeAsString();
}

}   // anonymous namespace

Y_UNIT_TEST_SUITE(TryParseBlobIds) {
    Y_UNIT_TEST(BadProto) {
        TFakeGroupSelector sel;
        auto result = TColumnChunkLoadContextV2::TryParseBlobIds("not-a-proto\xff\xfe", sel);
        UNIT_ASSERT(!result.IsSuccess());
    }

    Y_UNIT_TEST(TruncatedBlobId) {
        NKikimrTxColumnShard::TIndexPortionBlobsInfo proto;
        proto.AddBlobIds("short");   // fewer than 24 bytes
        TFakeGroupSelector sel;
        auto result = TColumnChunkLoadContextV2::TryParseBlobIds(proto.SerializeAsString(), sel);
        UNIT_ASSERT(!result.IsSuccess());
    }

    Y_UNIT_TEST(EmptyVector) {
        NKikimrTxColumnShard::TIndexPortionBlobsInfo proto;   // no blob ids
        TFakeGroupSelector sel;
        auto result = TColumnChunkLoadContextV2::TryParseBlobIds(proto.SerializeAsString(), sel);
        UNIT_ASSERT(!result.IsSuccess());
    }

    Y_UNIT_TEST(ValidRoundtrip) {
        TFakeGroupSelector sel;
        auto result = TColumnChunkLoadContextV2::TryParseBlobIds(MakeValidBlobsProto(), sel);
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.DetachResult().size(), 1u);
    }
}

Y_UNIT_TEST_SUITE(TryParseBlobAddress) {
    Y_UNIT_TEST(WrongSize) {
        TFakeGroupSelector sel;
        auto result = TIndexChunkLoadContext::TryParseBlobAddress("tooshort", sel);
        UNIT_ASSERT(!result.IsSuccess());
    }

    Y_UNIT_TEST(WrongSizeLonger) {
        TFakeGroupSelector sel;
        TString too_long(TLogoBlobID::BinarySize + 1, '\0');
        auto result = TIndexChunkLoadContext::TryParseBlobAddress(too_long, sel);
        UNIT_ASSERT(!result.IsSuccess());
    }

    Y_UNIT_TEST(ValidRoundtrip) {
        TFakeGroupSelector sel;
        auto result = TIndexChunkLoadContext::TryParseBlobAddress(MakeBlobAddressBinary(), sel);
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT(result.DetachResult().IsValid());
    }
}

Y_UNIT_TEST_SUITE(TBatchCursorStartKey) {
    Y_UNIT_TEST(StartsAtGivenKey) {
        TInternalPathId pathId = TInternalPathId::FromRawValue(42);
        ui64 portionId = 7;
        TBatchCursor cursor(100, { pathId, portionId });
        auto key = cursor.GetNextLoadPortionKey();
        UNIT_ASSERT_VALUES_EQUAL(key.first.GetRawValue(), pathId.GetRawValue());
        UNIT_ASSERT_VALUES_EQUAL(key.second, portionId);
    }

    Y_UNIT_TEST(DefaultStartsAtZero) {
        TBatchCursor cursor(100);
        auto key = cursor.GetNextLoadPortionKey();
        UNIT_ASSERT_VALUES_EQUAL(key.first.GetRawValue(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(key.second, 0u);
    }

    Y_UNIT_TEST(OnPortionLoadedAdvancesKey) {
        TBatchCursor cursor(100);
        TInternalPathId pathId = TInternalPathId::FromRawValue(5);
        cursor.OnPortionLoaded(pathId, 10);
        auto key = cursor.GetNextLoadPortionKey();
        UNIT_ASSERT_VALUES_EQUAL(key.first.GetRawValue(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(key.second, 11u);   // portionId + 1
    }
}
