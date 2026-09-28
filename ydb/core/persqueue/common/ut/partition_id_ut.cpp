#include <ydb/core/persqueue/common/partition_id.h>
#include <ydb/core/persqueue/public/write_id.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPartitionIdTest) {

Y_UNIT_TEST(DefaultAndExplicitConstructors) {
    TPartitionId empty;
    UNIT_ASSERT_VALUES_EQUAL(empty.OriginalPartitionId, 0u);
    UNIT_ASSERT_VALUES_EQUAL(empty.InternalPartitionId, 0u);
    UNIT_ASSERT(!empty.WriteId.Defined());
    UNIT_ASSERT(!empty.IsSupportivePartition());
    UNIT_ASSERT_VALUES_EQUAL(empty.ToString(), "0");

    TPartitionId simple(42);
    UNIT_ASSERT_VALUES_EQUAL(simple.OriginalPartitionId, 42u);
    UNIT_ASSERT_VALUES_EQUAL(simple.InternalPartitionId, 42u);
    UNIT_ASSERT(!simple.IsSupportivePartition());
    UNIT_ASSERT_VALUES_EQUAL(simple.ToString(), "42");
}

Y_UNIT_TEST(SupportivePartition) {
    TWriteId writeId(1, 2);
    TPartitionId id(10, writeId, 11);
    UNIT_ASSERT(id.IsSupportivePartition());
    UNIT_ASSERT_VALUES_EQUAL(id.OriginalPartitionId, 10u);
    UNIT_ASSERT_VALUES_EQUAL(id.InternalPartitionId, 11u);
    UNIT_ASSERT(id.ToString().Contains("10"));
    UNIT_ASSERT(id.ToString().Contains("11"));

    TStringStream ss;
    id.ToStream(ss);
    UNIT_ASSERT_VALUES_EQUAL(ss.Str(), id.ToString());

    TStringStream out;
    Out<TPartitionId>(out, id);
    UNIT_ASSERT_VALUES_EQUAL(out.Str(), id.ToString());
}

Y_UNIT_TEST(CompareAndHash) {
    TPartitionId a(1);
    TPartitionId b(2);
    TPartitionId a2(1);
    UNIT_ASSERT(a < b);
    UNIT_ASSERT(a == a2);
    UNIT_ASSERT(a != b);
    UNIT_ASSERT(a <=> b < 0);
    UNIT_ASSERT(a <=> a2 == 0);

    UNIT_ASSERT_VALUES_EQUAL(a.GetHash(), a2.GetHash());
    UNIT_ASSERT_VALUES_EQUAL(THash<TPartitionId>()(a), a.GetHash());
    UNIT_ASSERT_VALUES_EQUAL(std::hash<TPartitionId>()(a), a.GetHash());

    TPartitionId withWrite(1, TWriteId(3, 4), 5);
    UNIT_ASSERT(a != withWrite);
    UNIT_ASSERT(a < withWrite);
    UNIT_ASSERT(!(withWrite < a));
    UNIT_ASSERT(withWrite.GetHash() != a.GetHash());
}

} // Y_UNIT_TEST_SUITE(TPartitionIdTest)

} // namespace NKikimr::NPQ
