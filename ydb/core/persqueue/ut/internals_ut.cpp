#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <util/stream/format.h>


namespace NKikimr::NPQ {
namespace {

Y_UNIT_TEST_SUITE(TPQTestInternal) {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TEST CASES
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    // key for Body
    auto keyOld = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    UNIT_ASSERT_VALUES_EQUAL(keyOld.ToString(), "d0000000009_00000000000000000008_00007_0000000006_00005");

    auto keyNew = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{5, TWriteId{0, 1}, 9}, 8, 7, 6, 5);
    UNIT_ASSERT_VALUES_EQUAL(keyNew.ToString(), "D0000000009_00000000000000000008_00007_0000000006_00005");

    keyNew.SetType(TKeyPrefix::TypeInfo);
    UNIT_ASSERT_VALUES_EQUAL(keyNew.ToString(), "M0000000009_00000000000000000008_00007_0000000006_00005");

    // key for Head
    auto keyHead = TKey::ForHead(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    UNIT_ASSERT_VALUES_EQUAL(keyHead.ToString(), "d0000000009_00000000000000000008_00007_0000000006_00005|");

    keyHead = TKey::FromKey(keyHead, TKeyPrefix::TypeData, TPartitionId{10}, 11);
    UNIT_ASSERT_VALUES_EQUAL(keyHead.ToString(), "d0000000010_00000000000000000011_00007_0000000006_00005|");

    // key for FastWrite
    auto keyFastWrite = TKey::ForFastWrite(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    UNIT_ASSERT_VALUES_EQUAL(keyFastWrite.ToString(), "d0000000009_00000000000000000008_00007_0000000006_00005?");

    keyFastWrite = TKey::FromKey(keyFastWrite, TKeyPrefix::TypeData, TPartitionId{12}, 13);
    UNIT_ASSERT_VALUES_EQUAL(keyFastWrite.ToString(), "d0000000012_00000000000000000013_00007_0000000006_00005?");
}

Y_UNIT_TEST(RestoreKeys) {
    // the key from the string
    {
        auto key = TKey::FromString("X0000000001_00000000000000000002_00003_0000000004_00005");
        UNIT_ASSERT(key.GetType() == TKeyPrefix::TypeTmpData);
        UNIT_ASSERT_VALUES_EQUAL(key.GetPartition().InternalPartitionId, 1);
        UNIT_ASSERT_VALUES_EQUAL(key.GetOffset(), 2);
        UNIT_ASSERT_VALUES_EQUAL(key.GetPartNo(), 3);
        UNIT_ASSERT_VALUES_EQUAL(key.GetCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(key.GetInternalPartsCount(), 5);
        UNIT_ASSERT(!key.HasSuffix());
    }

    // blob type
    {
        auto key = TKey::FromString("i0000000001_00000000000000000002_00003_0000000004_00005");
        UNIT_ASSERT(key.GetType() == TKeyPrefix::TypeMeta);
    }

    // the `partitionId` is being replaced
    {
        auto key = TKey::FromString("d0000000002_00000000000000000013_00007_0000000006_00005", TPartitionId{3});
        UNIT_ASSERT_VALUES_EQUAL(key.GetPartition().InternalPartitionId, 3);
        UNIT_ASSERT(!key.HasSuffix());
    }

    // key for FastWrite
    {
        auto key = TKey::FromString("d0000000002_00000000000000000013_00007_0000000006_00005?", TPartitionId{4});
        UNIT_ASSERT_VALUES_EQUAL(key.GetPartition().InternalPartitionId, 4);
        UNIT_ASSERT(key.HasSuffix());
    }

    // key for head
    {
        auto key = TKey::FromString("d0000000002_00000000000000000013_00007_0000000006_00005|", TPartitionId{8});
        UNIT_ASSERT_VALUES_EQUAL(key.GetPartition().InternalPartitionId, 8);
        UNIT_ASSERT(key.HasSuffix());
        UNIT_ASSERT(!key.GetOffsetDelta().Defined());
    }
}

Y_UNIT_TEST(StoreKeysWithOffsetDelta) {
    auto key = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    key.SetOffsetDelta(42);
    UNIT_ASSERT(key.HasOffsetDelta());
    UNIT_ASSERT_VALUES_EQUAL(*key.GetOffsetDelta(), 42u);
    UNIT_ASSERT_VALUES_EQUAL(key.ToString(), "d0000000009_00000000000000000008_00007_0000000006_00005_0000000042");

    auto keyHead = TKey::ForHead(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    keyHead.SetOffsetDelta(3);
    UNIT_ASSERT_VALUES_EQUAL(keyHead.ToString(), "d0000000009_00000000000000000008_00007_0000000006_00005_0000000003|");

    key.SetOffsetDelta(Nothing());
    UNIT_ASSERT(!key.HasOffsetDelta());
    UNIT_ASSERT_VALUES_EQUAL(key.ToString(), "d0000000009_00000000000000000008_00007_0000000006_00005");
}

Y_UNIT_TEST(RestoreKeysWithOffsetDelta) {
    {
        auto key = TKey::FromString("d0000000002_00000000000000000013_00007_0000000006_00005_0000000042", TPartitionId{4});
        UNIT_ASSERT(key.HasOffsetDelta());
        UNIT_ASSERT_VALUES_EQUAL(*key.GetOffsetDelta(), 42u);
        UNIT_ASSERT(!key.HasSuffix());
    }

    {
        auto key = TKey::FromString("d0000000002_00000000000000000013_00007_0000000006_00005_0000000003?", TPartitionId{4});
        UNIT_ASSERT(key.HasOffsetDelta());
        UNIT_ASSERT_VALUES_EQUAL(*key.GetOffsetDelta(), 3u);
        UNIT_ASSERT(key.IsFastWrite());
    }

    {
        auto key = TKey::FromString("d0000000002_00000000000000000013_00007_0000000006_00005", TPartitionId{4});
        UNIT_ASSERT(!key.GetOffsetDelta().Defined());
    }
}

Y_UNIT_TEST(LegacyKeysBackwardCompatible) {
    // Keys in DS without OffsetDelta (body size == KeySize()) must keep parsing after format extension.
    const TString legacyBody = "d0000000009_00000000000000000008_00007_0000000006_00005";
    const TString legacyHead = legacyBody + "|";
    const TString legacyFastWrite = legacyBody + "?";

    UNIT_ASSERT(!TKey::FromString(legacyBody).HasOffsetDelta());
    UNIT_ASSERT(!TKey::FromString(legacyHead).HasOffsetDelta());
    UNIT_ASSERT(!TKey::FromString(legacyFastWrite).HasOffsetDelta());

    const auto key = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    UNIT_ASSERT_VALUES_EQUAL(key.ToString(), legacyBody);
    UNIT_ASSERT_EQUAL(TKey::FromString(key.ToString()), key);

    const auto fromLegacy = TKey::FromKey(key, TKeyPrefix::TypeData, TPartitionId{10}, 11);
    UNIT_ASSERT(!fromLegacy.GetOffsetDelta().Defined());
    UNIT_ASSERT_VALUES_EQUAL(fromLegacy.ToString(), "d0000000010_00000000000000000011_00007_0000000006_00005");

    auto withDelta = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    withDelta.SetOffsetDelta(99);
    const auto fromWithDelta = TKey::FromKey(withDelta, TKeyPrefix::TypeData, TPartitionId{10}, 11);
    UNIT_ASSERT_VALUES_EQUAL(*fromWithDelta.GetOffsetDelta(), 99u);
    UNIT_ASSERT_VALUES_EQUAL(fromWithDelta.ToString(), "d0000000010_00000000000000000011_00007_0000000006_00005_0000000099");
}

} //Y_UNIT_TEST_SUITE


} // namespace
} // namespace NKikimr::NPQ
