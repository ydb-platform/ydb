#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/public/write_id.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

namespace {

TPartitionId ServicePartition(ui32 original = 5, ui32 internal = 9) {
    return TPartitionId(original, TWriteId(1, 2), internal);
}

} // namespace

Y_UNIT_TEST_SUITE(TKeyPrefixTest) {

Y_UNIT_TEST(DefaultAndMarked) {
    TKeyPrefix empty;
    UNIT_ASSERT_EQUAL(empty.GetType(), TKeyPrefix::TypeNone);
    UNIT_ASSERT_VALUES_EQUAL(empty.GetPartition().InternalPartitionId, 0u);
    UNIT_ASSERT(!empty.Marked(TKeyPrefix::MarkUser));
    UNIT_ASSERT_VALUES_EQUAL(empty.ToString().size(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(TKeyPrefix::MarkPosition(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(TKeyPrefix::MarkedSize(), 12u);

    empty.Clear();
    UNIT_ASSERT(!empty.Marked(TKeyPrefix::MarkSourceId));

    TKeyPrefix marked(TKeyPrefix::TypeInfo, TPartitionId(7), TKeyPrefix::MarkSourceId);
    const TKeyPrefix& markedRef = marked;
    UNIT_ASSERT_EQUAL(markedRef.GetType(), TKeyPrefix::TypeInfo);
    UNIT_ASSERT(marked.Marked(TKeyPrefix::MarkSourceId));
    UNIT_ASSERT(!marked.Marked(TKeyPrefix::MarkUser));
    UNIT_ASSERT(!marked.IsServicePartition());
}

Y_UNIT_TEST(ServiceTypes) {
    const TPartitionId part = ServicePartition();
    for (auto type : {
             TKeyPrefix::TypeData,
             TKeyPrefix::TypeTmpData,
             TKeyPrefix::TypeInfo,
             TKeyPrefix::TypeMeta,
             TKeyPrefix::TypeTxMeta,
         })
    {
        TKeyPrefix prefix(type, part);
        UNIT_ASSERT(prefix.IsServicePartition());
        UNIT_ASSERT_EQUAL(prefix.GetType(), type);
        prefix.SetType(type);
        UNIT_ASSERT_EQUAL(prefix.GetType(), type);
    }

    TKeyPrefix none(TKeyPrefix::TypeNone, part);
    UNIT_ASSERT_EQUAL(none.GetType(), TKeyPrefix::TypeNone);
    none.SetType(TKeyPrefix::TypeNone);
    UNIT_ASSERT_EQUAL(none.GetType(), TKeyPrefix::TypeNone);
}

Y_UNIT_TEST(NonServiceSetType) {
    TKeyPrefix prefix(TKeyPrefix::TypeData, TPartitionId(3));
    UNIT_ASSERT_EQUAL(prefix.GetType(), TKeyPrefix::TypeData);
    prefix.SetType(TKeyPrefix::TypeMeta);
    UNIT_ASSERT_EQUAL(prefix.GetType(), TKeyPrefix::TypeMeta);
    prefix.SetType(TKeyPrefix::TypeTxMeta);
    UNIT_ASSERT_EQUAL(prefix.GetType(), TKeyPrefix::TypeTxMeta);
    prefix.SetType(TKeyPrefix::TypeTmpData);
    UNIT_ASSERT_EQUAL(prefix.GetType(), TKeyPrefix::TypeTmpData);
    prefix.SetType(TKeyPrefix::TypeInfo);
    UNIT_ASSERT_EQUAL(prefix.GetType(), TKeyPrefix::TypeInfo);
}

Y_UNIT_TEST(MakeKeyPrefixRange) {
    const TPartitionId part(10);
    auto [from, to] = MakeKeyPrefixRange(TKeyPrefix::TypeData, part);
    UNIT_ASSERT_EQUAL(from.GetType(), TKeyPrefix::TypeData);
    UNIT_ASSERT_VALUES_EQUAL(from.GetPartition().InternalPartitionId, 10u);
    UNIT_ASSERT_VALUES_EQUAL(to.GetPartition().InternalPartitionId, 11u);
    UNIT_ASSERT_VALUES_EQUAL(to.GetPartition().OriginalPartitionId, 10u);
}

Y_UNIT_TEST(MakeKeyPrefixRangeService) {
    const TPartitionId part = ServicePartition(4, 8);
    auto [from, to] = MakeKeyPrefixRange(TKeyPrefix::TypeData, part);
    UNIT_ASSERT(from.IsServicePartition());
    UNIT_ASSERT_VALUES_EQUAL(to.GetPartition().InternalPartitionId, 9u);
    UNIT_ASSERT(to.GetPartition().WriteId.Defined());
}

} // Y_UNIT_TEST_SUITE(TKeyPrefixTest)

Y_UNIT_TEST_SUITE(TKeyTest) {

Y_UNIT_TEST(StoreAndRestoreBodyHeadFastWrite) {
    auto body = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    UNIT_ASSERT_VALUES_EQUAL(body.ToString(), "d0000000009_00000000000000000008_00007_0000000006_00005");
    UNIT_ASSERT(!body.HasSuffix());
    UNIT_ASSERT(!body.IsHead());
    UNIT_ASSERT(!body.IsFastWrite());
    UNIT_ASSERT(!body.HasOffsetDelta());
    UNIT_ASSERT(TKey::IsValidSerializedSize(body.ToString().size()));

    auto head = TKey::ForHead(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    UNIT_ASSERT(head.IsHead());
    UNIT_ASSERT(head.HasSuffix());
    UNIT_ASSERT(head.GetSuffix() == TKey::ESuffix::Head);

    auto fast = TKey::ForFastWrite(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    UNIT_ASSERT(fast.IsFastWrite());
    UNIT_ASSERT(fast.GetSuffix() == TKey::ESuffix::FastWrite);

    auto copied = TKey(head);
    UNIT_ASSERT(copied == head);
    UNIT_ASSERT(!(copied < head));
    UNIT_ASSERT(!(head < copied));
}

Y_UNIT_TEST(FromStringAndPartitionOverride) {
    auto key = TKey::FromString("X0000000001_00000000000000000002_00003_0000000004_00005");
    UNIT_ASSERT_EQUAL(key.GetType(), TKeyPrefix::TypeTmpData);
    UNIT_ASSERT_VALUES_EQUAL(key.GetPartition().InternalPartitionId, 1u);
    UNIT_ASSERT_VALUES_EQUAL(key.GetOffset(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(key.GetPartNo(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(key.GetCount(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(key.GetInternalPartsCount(), 5u);

    auto replaced = TKey::FromString(key.ToString(), TPartitionId{3});
    UNIT_ASSERT_VALUES_EQUAL(replaced.GetPartition().InternalPartitionId, 3u);
    UNIT_ASSERT_VALUES_EQUAL(replaced.GetOffset(), 2u);
}

Y_UNIT_TEST(AllTypesFromString) {
    UNIT_ASSERT_EQUAL(TKey::FromString("i0000000001_00000000000000000002_00003_0000000004_00005").GetType(), TKeyPrefix::TypeMeta);
    UNIT_ASSERT_EQUAL(TKey::FromString("I0000000001_00000000000000000002_00003_0000000004_00005").GetType(), TKeyPrefix::TypeTxMeta);
    UNIT_ASSERT_EQUAL(TKey::FromString("m0000000001_00000000000000000002_00003_0000000004_00005").GetType(), TKeyPrefix::TypeInfo);
    UNIT_ASSERT_EQUAL(TKey::FromString("J0000000001_00000000000000000002_00003_0000000004_00005").GetType(), TKeyPrefix::TypeMeta);
    UNIT_ASSERT_EQUAL(TKey::FromString("K0000000001_00000000000000000002_00003_0000000004_00005").GetType(), TKeyPrefix::TypeTxMeta);
    UNIT_ASSERT_EQUAL(TKey::FromString("M0000000001_00000000000000000002_00003_0000000004_00005").GetType(), TKeyPrefix::TypeInfo);
    UNIT_ASSERT_EQUAL(TKey::FromString("D0000000001_00000000000000000002_00003_0000000004_00005").GetType(), TKeyPrefix::TypeData);
    UNIT_ASSERT_EQUAL(TKey::FromString("x0000000001_00000000000000000002_00003_0000000004_00005").GetType(), TKeyPrefix::TypeTmpData);

    auto serviceType = TKey::FromString("M0000000001_00000000000000000002_00003_0000000004_00005");
    UNIT_ASSERT(!serviceType.IsServicePartition());
    serviceType.SetType(TKeyPrefix::TypeData);
    UNIT_ASSERT_EQUAL(serviceType.GetType(), TKeyPrefix::TypeData);
}

Y_UNIT_TEST(Setters) {
    auto key = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{1}, 2, 3, 4, 5);
    key.SetOffset(20);
    key.SetPartNo(30);
    key.SetCount(40);
    key.SetInternalPartsCount(50);
    UNIT_ASSERT_VALUES_EQUAL(key.GetOffset(), 20u);
    UNIT_ASSERT_VALUES_EQUAL(key.GetPartNo(), 30u);
    UNIT_ASSERT_VALUES_EQUAL(key.GetCount(), 40u);
    UNIT_ASSERT_VALUES_EQUAL(key.GetInternalPartsCount(), 50u);

    key.SetFastWrite();
    UNIT_ASSERT(key.IsFastWrite());
    key.SetBody();
    UNIT_ASSERT(!key.HasSuffix());
}

Y_UNIT_TEST(OffsetDelta) {
    auto key = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5, 42);
    UNIT_ASSERT(key.HasOffsetDelta());
    UNIT_ASSERT_VALUES_EQUAL(*key.GetOffsetDelta(), 42u);
    UNIT_ASSERT(TKey::IsValidSerializedSize(key.ToString().size()));

    auto head = TKey::ForHead(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5, 3);
    UNIT_ASSERT(head.IsHead());
    UNIT_ASSERT_VALUES_EQUAL(*head.GetOffsetDelta(), 3u);
    UNIT_ASSERT(TKey::IsValidSerializedSize(head.ToString().size()));

    auto fast = TKey::ForFastWrite(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5, 9);
    UNIT_ASSERT(fast.IsFastWrite());
    UNIT_ASSERT_VALUES_EQUAL(*fast.GetOffsetDelta(), 9u);

    key.SetOffsetDelta(ui64{99});
    UNIT_ASSERT_VALUES_EQUAL(*key.GetOffsetDelta(), 99u);
    key.SetOffsetDelta(Nothing());
    UNIT_ASSERT(!key.HasOffsetDelta());

    auto headNoDelta = TKey::ForHead(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5);
    UNIT_ASSERT(headNoDelta.IsHead());
    UNIT_ASSERT(!headNoDelta.HasOffsetDelta());
    headNoDelta.SetOffsetDelta(7);
    UNIT_ASSERT(headNoDelta.IsHead());
    UNIT_ASSERT_VALUES_EQUAL(*headNoDelta.GetOffsetDelta(), 7u);
    headNoDelta.SetOffsetDelta(Nothing());
    UNIT_ASSERT(headNoDelta.IsHead());
    UNIT_ASSERT(!headNoDelta.HasOffsetDelta());

    auto restored = TKey::FromString("d0000000002_00000000000000000013_00007_0000000006_00005_0000000042");
    UNIT_ASSERT_VALUES_EQUAL(*restored.GetOffsetDelta(), 42u);
    auto restoredFast = TKey::FromString("d0000000002_00000000000000000013_00007_0000000006_00005_0000000003?");
    UNIT_ASSERT(restoredFast.IsFastWrite());
    UNIT_ASSERT_VALUES_EQUAL(*restoredFast.GetOffsetDelta(), 3u);
}

Y_UNIT_TEST(FromKeyPreservesFields) {
    auto src = TKey::ForHead(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5, 11);
    auto dst = TKey::FromKey(src, TKeyPrefix::TypeInfo, TPartitionId{10}, 12);
    UNIT_ASSERT_EQUAL(dst.GetType(), TKeyPrefix::TypeInfo);
    UNIT_ASSERT_VALUES_EQUAL(dst.GetPartition().InternalPartitionId, 10u);
    UNIT_ASSERT_VALUES_EQUAL(dst.GetOffset(), 12u);
    UNIT_ASSERT_VALUES_EQUAL(dst.GetPartNo(), 7u);
    UNIT_ASSERT_VALUES_EQUAL(dst.GetCount(), 6u);
    UNIT_ASSERT_VALUES_EQUAL(dst.GetInternalPartsCount(), 5u);
    UNIT_ASSERT(dst.IsHead());
    UNIT_ASSERT_VALUES_EQUAL(*dst.GetOffsetDelta(), 11u);
}

Y_UNIT_TEST(Ordering) {
    auto a = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{1}, 10, 1, 1, 0);
    auto b = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{2}, 1, 0, 1, 0);
    UNIT_ASSERT(a < b);

    auto c = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{1}, 11, 0, 1, 0);
    UNIT_ASSERT(a < c);

    auto d = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{1}, 10, 2, 1, 0);
    UNIT_ASSERT(a < d);
    UNIT_ASSERT(!(d < a));
    UNIT_ASSERT(!(a < a));
}

Y_UNIT_TEST(LegacyEmptySuffix) {
    TString raw = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId{9}, 8, 7, 6, 5).ToString();
    raw.append('\0');
    UNIT_ASSERT(TKey::IsValidSerializedSize(raw.size()));
    auto key = TKey::FromString(raw);
    UNIT_ASSERT_VALUES_EQUAL(key.GetOffset(), 8u);
    UNIT_ASSERT(!key.HasSuffix());
    UNIT_ASSERT(!key.HasOffsetDelta());
}

Y_UNIT_TEST(ServiceBodyKey) {
    auto key = TKey::ForBody(TKeyPrefix::TypeData, ServicePartition(), 8, 7, 6, 5);
    UNIT_ASSERT(key.IsServicePartition());
    UNIT_ASSERT_EQUAL(key.GetType(), TKeyPrefix::TypeData);
    UNIT_ASSERT(key.ToString().StartsWith("D"));
}

Y_UNIT_TEST(GetTxKey) {
    UNIT_ASSERT_VALUES_EQUAL(GetTxKey(7), "tx_00000000000000000007");
    UNIT_ASSERT_VALUES_EQUAL(GetTxKey(7, 3), "tx_00000000000000000007_0000000003");
}

Y_UNIT_TEST(DefaultKey) {
    TKey key;
    UNIT_ASSERT_EQUAL(key.GetType(), TKeyPrefix::TypeNone);
    UNIT_ASSERT_VALUES_EQUAL(key.GetOffset(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(key.GetCount(), 0u);
    UNIT_ASSERT(!key.HasSuffix());
    UNIT_ASSERT(!key.HasOffsetDelta());
    TKey copy(key);
    UNIT_ASSERT(copy == key);
    UNIT_ASSERT(TKey::IsValidSerializedSize(TKey::KeySize()));
    UNIT_ASSERT(TKey::IsValidSerializedSize(TKey::KeySize() + 1));
    UNIT_ASSERT(TKey::IsValidSerializedSize(TKey::KeySizeWithOffsetDelta()));
    UNIT_ASSERT(TKey::IsValidSerializedSize(TKey::KeySizeWithOffsetDelta() + 1));
    UNIT_ASSERT(!TKey::IsValidSerializedSize(0));
}

} // Y_UNIT_TEST_SUITE(TKeyTest)

} // namespace NKikimr::NPQ
