#include "blob.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>
#include <util/generic/size_literals.h>
#include <util/stream/format.h>


namespace NKikimr::NPQ {
namespace {

Y_UNIT_TEST_SUITE(TPQTestInternal) {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TEST CASES
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


Y_UNIT_TEST(TestPartitionedBlobSimpleTest) {
    THead head;
    THead newHead;

    TPartitionedBlob blob(TPartitionId(0), 0, "sourceId", 1, 1, 10, head, newHead, false, false, 8_MB);
    TClientBlob clientBlob("sourceId", 1, "valuevalue", TMaybe<TPartData>(), TInstant::MilliSeconds(1), TInstant::MilliSeconds(1), 0, "123", "123");
    UNIT_ASSERT(blob.IsInited());
    TString error;
    UNIT_ASSERT(blob.IsNextPart("sourceId", 1, 0, &error));

    blob.Add(std::move(clientBlob));
    UNIT_ASSERT(blob.IsComplete());
    UNIT_ASSERT(blob.GetFormedBlobs().empty());
    UNIT_ASSERT(blob.GetClientBlobs().size() == 1);
}

void Test(bool headCompacted, ui32 parts, ui32 partSize, ui32 leftInHead)
{
    TVector<TClientBlob> all;

    THead head;
    head.Offset = 100;
    TString value(100_KB, 'a');
    head.Batches.push_back(TBatch(head.Offset, 0, TVector<TClientBlob>()));
    for (ui32 i = 0; i < 50; ++i) {
        head.Batches.back().AddBlob(TClientBlob(
            "sourceId" + TString(1,'a' + rand() % 26), i + 1, value, TMaybe<TPartData>(),
            TInstant::MilliSeconds(i + 1),  TInstant::MilliSeconds(i + 1), 1, "", ""
        ));
        if (!headCompacted)
            all.push_back(head.Batches.back().Blobs.back());
    }
    head.Batches.back().Pack();
    UNIT_ASSERT(head.Batches.back().Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed);
    head.Batches.back().Unpack();
    head.Batches.back().Pack();
    TString str;
    head.Batches.back().SerializeTo(str);
    auto header = ExtractHeader(str.c_str(), str.size());
    TBatch batch(header, str.c_str() + header.ByteSize() + sizeof(ui16));
    batch.Unpack();

    head.PackedSize = head.Batches.back().GetPackedSize();
    UNIT_ASSERT(head.Batches.back().GetUnpackedSize() + GetMaxHeaderSize() >= head.Batches.back().GetPackedSize());
    THead newHead;
    newHead.Offset = head.GetNextOffset();
    newHead.Batches.push_back(TBatch(newHead.Offset, 0, TVector<TClientBlob>()));
    for (ui32 i = 0; i < 10; ++i) {
        newHead.Batches.back().AddBlob(TClientBlob(
            "sourceId2", i + 1, value, TMaybe<TPartData>(),
            TInstant::MilliSeconds(i + 1000), TInstant::MilliSeconds(i + 1000), 1, "", ""
        ));
        all.push_back(newHead.Batches.back().Blobs.back()); //newHead always glued
    }
    newHead.PackedSize = newHead.Batches.back().GetUnpackedSize();
    TString value2(partSize, 'b');
    ui32 maxBlobSize = 8 << 20;
    TPartitionedBlob blob(TPartitionId(0), newHead.GetNextOffset(), "sourceId3", 1, parts, parts * value2.size(), head, newHead, headCompacted, false, maxBlobSize);

    TVector<TPartitionedBlob::TFormedBlobInfo> formed;

    TString error;
    for (ui32 i = 0; i < parts; ++i) {
        UNIT_ASSERT(!blob.IsComplete());
        UNIT_ASSERT(blob.IsNextPart("sourceId3", 1, i, &error));
        TMaybe<TPartData> partData = TPartData(i, parts, value2.size());
        TClientBlob clientBlob(
            "soruceId3", 1, value2, std::move(partData),
            TInstant::MilliSeconds(1), TInstant::MilliSeconds(1), 1, "", ""
        );
        all.push_back(clientBlob);
        auto res = blob.Add(std::move(clientBlob));
        if (res && !res->Value.empty())
            formed.emplace_back(*res);
    }
    UNIT_ASSERT(blob.IsComplete());
    UNIT_ASSERT(formed.size() == blob.GetFormedBlobs().size());
    for (ui32 i = 0; i < formed.size(); ++i) {
        UNIT_ASSERT(formed[i].Key == blob.GetFormedBlobs()[i].OldKey);
        UNIT_ASSERT(formed[i].Value.size() == blob.GetFormedBlobs()[i].Size);
        UNIT_ASSERT(formed[i].Value.size() <= 8_MB);
        UNIT_ASSERT(formed[i].Value.size() > 6_MB);
    }
    TVector<TClientBlob> real;
    ui32 nextOffset = headCompacted ? newHead.Offset : head.Offset;
    for (auto& p : formed) {
        const char* data = p.Value.c_str();
        const char* end = data + p.Value.size();
        ui64 offset = p.Key.GetOffset();
        UNIT_ASSERT(offset == nextOffset);
        while(data < end) {
            auto header = ExtractHeader(data, end - data);
            UNIT_ASSERT(header.GetOffset() == nextOffset);
            nextOffset += header.GetCount();
            data += header.ByteSize() + sizeof(ui16);
            TBatch batch(header, data);
            data += header.GetPayloadSize();
            batch.Unpack();
            for (auto& b: batch.Blobs) {
                real.push_back(b);
            }
        }
    }
    ui32 s = 0;
    ui32 c = 0;

    if (formed.empty()) { //nothing compacted - newHead must be here

        if (!headCompacted) {
            for (auto& p : head.Batches) {
                p.Unpack();
                for (const auto& b : p.Blobs)
                    real.push_back(b);
            }
        }

        for (auto& p : newHead.Batches) {
            p.Unpack();
            for (const auto& b : p.Blobs)
                real.push_back(b);
        }
    }

    for (const auto& p : blob.GetClientBlobs()) {
        real.push_back(p);
        c++;
        s += p.GetBlobSize();
    }

    UNIT_ASSERT(c == leftInHead);
    UNIT_ASSERT(s + GetMaxHeaderSize() <= maxBlobSize);
    UNIT_ASSERT(real.size() == all.size());
    for (ui32 i = 0; i < all.size(); ++i) {
        UNIT_ASSERT(all[i].SourceId == real[i].SourceId);
        UNIT_ASSERT(all[i].SeqNo == real[i].SeqNo);
        UNIT_ASSERT(all[i].Data == real[i].Data);
        UNIT_ASSERT(all[i].PartData.Defined() == real[i].PartData.Defined());
        if (all[i].PartData.Defined()) {
            UNIT_ASSERT(all[i].PartData->PartNo == real[i].PartData->PartNo);
            UNIT_ASSERT(all[i].PartData->TotalParts == real[i].PartData->TotalParts);
            UNIT_ASSERT(all[i].PartData->TotalSize == real[i].PartData->TotalSize);
        }

    }
}

Y_UNIT_TEST(TestPartitionedBigTest) {

    Test(true, 100, 400_KB, 3);
    Test(false, 100, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 16); //serialized size of client blob is 512_KB - 100
    Test(false, 101, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 1); //serialized size of client blob is 512_KB - 100
    Test(false, 1, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 1); //serialized size of client blob is 512_KB - 100
    Test(true, 1, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 1); //serialized size of client blob is 512_KB - 100
    Test(true, 101, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 7); //serialized size of client blob is 512_KB - 100
}

Y_UNIT_TEST(TestBatchPacking) {
    TString value(10, 'a');
    TBatch batch;
    for (ui32 i = 0; i < 100; ++i) {
        batch.AddBlob(TClientBlob(
            "sourceId1", i + 1, value, TMaybe<TPartData>(),
            TInstant::MilliSeconds(1), TInstant::MilliSeconds(1), 0, "", ""
        ));
    }
    batch.Pack();
    TBuffer b = batch.PackedData;
    UNIT_ASSERT(batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed);
    batch.Unpack();
    batch.Pack();
    UNIT_ASSERT(batch.PackedData == b);
    TString str;
    batch.SerializeTo(str);
    auto header = ExtractHeader(str.c_str(), str.size());
    TBatch batch2(header, str.c_str() + header.ByteSize() + sizeof(ui16));
    batch2.Unpack();
    Y_ABORT_UNLESS(batch2.Blobs.size() == 100);

    TBatch batch3;
    batch3.AddBlob(TClientBlob(
        "sourceId", 999'999'999'999'999ll, "abacaba", TPartData{33, 66, 4'000'000'000u},
        TInstant::MilliSeconds(999'999'999'999ll), TInstant::MilliSeconds(1000), 0, "", ""
    ));
    batch3.Pack();
    UNIT_ASSERT(batch3.Header.GetFormat() == NKikimrPQ::TBatchHeader::EUncompressed);
    batch3.Unpack();
    Y_ABORT_UNLESS(batch3.Blobs.size() == 1);
}

const TString ToHex(const TString& value) {
    return TStringBuilder() << HexText(TBasicStringBuf(value));
}

Y_UNIT_TEST(TestKeyRange) {
    char expected_[] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18};
    TString expected(expected_, sizeof(expected_));

    NYql::TWide<ui64> v (0x0102030405060708ull, 0x1112131415161718ull);

    TString result = AsKeyBound(v);

    UNIT_ASSERT_STRINGS_EQUAL(ToHex(result), ToHex(expected));

    NYql::NDecimal::TUint128 v2 = 0x0102030405060708ull;
    v2 <<= sizeof(ui64) << 3;
    v2 |= 0x1112131415161718ull;
    result = AsKeyBound(v2);

    UNIT_ASSERT_STRINGS_EQUAL(ToHex(result), ToHex(expected));
} // Y_UNIT_TEST(TestKeyRange)

Y_UNIT_TEST(TestAsInt) {
    {
        ui8 v = 0x73;
        ui8 r8 = AsInt<ui8>(AsKeyBound(v));
        UNIT_ASSERT_VALUES_EQUAL(v, r8);

        ui64 r64 = AsInt<ui64>(AsKeyBound(v));
        ui64 v64 = v;
        v64 <<= 56;
        UNIT_ASSERT_VALUES_EQUAL(v64, r64);

        NYql::NDecimal::TUint128 r128 = AsInt<NYql::NDecimal::TUint128>(AsKeyBound(v));
        NYql::NDecimal::TUint128 v128 = v;
        v128 <<= 120;
        UNIT_ASSERT_EQUAL(v128, r128);
    }

    {
        ui16 v = 0x1234;

        ui8 v8 = 0x12;
        ui8 r8 = AsInt<ui8>(AsKeyBound(v));
        UNIT_ASSERT_VALUES_EQUAL(v8, r8);

        ui16 r16 = AsInt<ui16>(AsKeyBound(v));
        UNIT_ASSERT_VALUES_EQUAL(v, r16);

        ui64 r64 = AsInt<ui64>(AsKeyBound(v));
        ui64 v64 = v;
        v64 <<= 48;
        UNIT_ASSERT_VALUES_EQUAL(v64, r64);

        NYql::NDecimal::TUint128 r128 = AsInt<NYql::NDecimal::TUint128>(AsKeyBound(v));
        NYql::NDecimal::TUint128 v128 = v;
        v128 <<= 112;
        UNIT_ASSERT_EQUAL(v128, r128);
    }

    {
        NYql::NDecimal::TUint128 v128 = 0x0102030405060708ull;
        v128 <<= 64;
        v128 += 0x0910111213141516ull;
        NYql::NDecimal::TUint128 r128 = AsInt<NYql::NDecimal::TUint128>(AsKeyBound(v128));
        UNIT_ASSERT_EQUAL(v128, r128);
    }
}

Y_UNIT_TEST(TestAsIntWide) {
    {
        ui32 v = 0x00001234;
        NYql::TWide<ui16> r = AsInt<NYql::TWide<ui16>>(AsKeyBound(v));
        ui32 r32 = ui32(r);
        UNIT_ASSERT_VALUES_EQUAL_C(v, r32, TStringBuilder() << NPQ::ToHex(v) << " != " << NPQ::ToHex(r32));
    }
    {
        ui32 v = 0x12345678;
        NYql::TWide<ui16> r = AsInt<NYql::TWide<ui16>>(AsKeyBound(v));
        ui32 r32 = ui32(r);
        UNIT_ASSERT_VALUES_EQUAL_C(v, r32, TStringBuilder() << NPQ::ToHex(v) << " != " << NPQ::ToHex(r32));
    }
}

Y_UNIT_TEST(TestToHex) {
    ui64 v = 0x0102030405060708;
    TString r = NPQ::ToHex<ui64>(v);
    UNIT_ASSERT_VALUES_EQUAL(r, "0x0102030405060708");
}

Y_UNIT_TEST(StoreKeys) {
    TKey keyOld(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5, false);
    UNIT_ASSERT_VALUES_EQUAL(keyOld.ToString(), "d0000000009_00000000000000000008_00007_0000000006_00005");

    TKey keyNew(TKeyPrefix::TypeData, TPartitionId{5, TWriteId{0, 1}, 9}, 8, 7, 6, 5, false);
    UNIT_ASSERT_VALUES_EQUAL(keyNew.ToString(), "D0000000009_00000000000000000008_00007_0000000006_00005");

    keyNew.SetType(TKeyPrefix::TypeInfo);
    UNIT_ASSERT_VALUES_EQUAL(keyNew.ToString(), "M0000000009_00000000000000000008_00007_0000000006_00005");
}

Y_UNIT_TEST(RestoreKeys) {
    {
        TKey key("X0000000001_00000000000000000002_00003_0000000004_00005");
        UNIT_ASSERT(key.GetType() == TKeyPrefix::TypeTmpData);
        UNIT_ASSERT_VALUES_EQUAL(key.GetPartition().InternalPartitionId, 1);
    }
    {
        TKey key("i0000000001_00000000000000000002_00003_0000000004_00005");
        UNIT_ASSERT(key.GetType() == TKeyPrefix::TypeMeta);
        UNIT_ASSERT_VALUES_EQUAL(key.GetPartition().InternalPartitionId, 1);
    }
}

} //Y_UNIT_TEST_SUITE


} // TInternalsTest
} // namespace NKikimr::NPQ
