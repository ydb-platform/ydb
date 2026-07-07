#include <ydb/core/kafka_proxy/kafka_producer_instance_id.h>
#include <ydb/core/persqueue/public/pqdata_transaction_compat.h>
#include <ydb/core/persqueue/public/write_id.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>

namespace NKikimr::NPQ {
namespace {

NKikimrPQ::TWriteId MakeLegacyTopicWriteId(ui64 nodeId, ui64 keyId)
{
    NKikimrPQ::TWriteId writeId;
    writeId.SetNodeId(nodeId);
    writeId.SetKeyId(keyId);
    return writeId;
}

NKikimrPQ::TWriteId MakeLegacyKafkaWriteId(i64 id, i32 epoch)
{
    NKikimrPQ::TWriteId writeId;
    writeId.SetKafkaTransaction(true);
    writeId.MutableKafkaProducerInstanceId()->SetId(id);
    writeId.MutableKafkaProducerInstanceId()->SetEpoch(epoch);
    return writeId;
}

Y_UNIT_TEST_SUITE(TWriteIdCompat) {

Y_UNIT_TEST(UpgradeFromLegacyTopic) {
    auto writeId = MakeLegacyTopicWriteId(10, 20);
    UNIT_ASSERT(!HasCanonical(writeId));

    UpgradeFromLegacy(writeId);

    UNIT_ASSERT(HasCanonical(writeId));
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetTopicApi().GetNodeId(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetTopicApi().GetKeyId(), 20u);
}

Y_UNIT_TEST(UpgradeFromLegacyKafka) {
    auto writeId = MakeLegacyKafkaWriteId(42, 3);
    UNIT_ASSERT(!HasCanonical(writeId));

    UpgradeFromLegacy(writeId);

    UNIT_ASSERT(HasCanonical(writeId));
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetKafkaApi().GetKafkaProducerInstanceId().GetId(), 42);
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetKafkaApi().GetKafkaProducerInstanceId().GetEpoch(), 3);
}

Y_UNIT_TEST(DowngradeToLegacyKeepsCanonical) {
    TWriteId writeId(1, 2);
    auto proto = writeId.GetProto();

    DowngradeToLegacy(proto);

    UNIT_ASSERT(HasCanonical(proto));
    UNIT_ASSERT_VALUES_EQUAL(proto.GetNodeId(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetKeyId(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetTopicApi().GetNodeId(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetTopicApi().GetKeyId(), 2u);
}

Y_UNIT_TEST(SetWriteIdWritesCanonicalAndLegacy) {
    TWriteId writeId(NKafka::TProducerInstanceId{7, 8});
    NKikimrPQ::TDataTransaction tx;
    SetWriteId(tx, writeId);

    const auto& proto = tx.GetWriteId();
    UNIT_ASSERT(HasCanonical(proto));
    UNIT_ASSERT(proto.GetKafkaTransaction());
    UNIT_ASSERT_VALUES_EQUAL(proto.GetKafkaProducerInstanceId().GetId(), 7);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetKafkaProducerInstanceId().GetEpoch(), 8);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetKafkaApi().GetKafkaProducerInstanceId().GetId(), 7);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetKafkaApi().GetKafkaProducerInstanceId().GetEpoch(), 8);
}

Y_UNIT_TEST(GetWriteIdRoundTripFromLegacyProto) {
    NKikimrPQ::TDataTransaction tx;
    *tx.MutableWriteId() = MakeLegacyTopicWriteId(3, 4);

    const TWriteId writeId = GetWriteId(tx);

    UNIT_ASSERT(writeId.IsTopicApiTransaction());
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetNodeId(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetKeyId(), 4u);
}

Y_UNIT_TEST(SetWriteIdRoundTrip) {
    const TWriteId original(5, 6);
    NKikimrPQ::TTransaction tx;
    SetWriteId(tx, original);

    UNIT_ASSERT_VALUES_EQUAL(GetWriteId(tx), original);
}

Y_UNIT_TEST(ProtoSerializeDeserializeRoundTrip) {
    const TWriteId original(NKafka::TProducerInstanceId{11, 12});
    NKikimrPQ::TWriteId proto = original.GetProto();
    const TString bytes = proto.SerializeAsString();

    NKikimrPQ::TWriteId parsed;
    UNIT_ASSERT(parsed.ParseFromString(bytes));

    UNIT_ASSERT_VALUES_EQUAL(TWriteId(std::move(parsed)), original);
}

} // Y_UNIT_TEST_SUITE(TWriteIdCompat)

Y_UNIT_TEST_SUITE(TWriteIdEquality) {

Y_UNIT_TEST(KafkaWriteIdsAreDistinct) {
    const TWriteId first(NKafka::TProducerInstanceId{1, 0});
    const TWriteId second(NKafka::TProducerInstanceId{2, 0});

    UNIT_ASSERT(first != second);
    UNIT_ASSERT_VALUES_UNEQUAL(first.GetHash(), second.GetHash());
}

Y_UNIT_TEST(HashMapStoresDistinctKafkaWriteIds) {
    const TWriteId first(NKafka::TProducerInstanceId{1, 0});
    const TWriteId second(NKafka::TProducerInstanceId{2, 0});

    THashMap<TWriteId, ui32> txWrites;
    txWrites[first] = 1;
    txWrites[second] = 2;

    UNIT_ASSERT_VALUES_EQUAL(txWrites.size(), 2u);
    UNIT_ASSERT(txWrites.contains(first));
    UNIT_ASSERT(txWrites.contains(second));
}

Y_UNIT_TEST(HashMatchesEquality) {
    const TWriteId left(9, 10);
    const TWriteId right(9, 10);
    const TWriteId other(9, 11);

    UNIT_ASSERT_VALUES_EQUAL(left, right);
    UNIT_ASSERT_VALUES_EQUAL(left.GetHash(), right.GetHash());
    UNIT_ASSERT(left != other);
    UNIT_ASSERT_VALUES_UNEQUAL(left.GetHash(), other.GetHash());
}

} // Y_UNIT_TEST_SUITE(TWriteIdEquality)

} // namespace
} // namespace NKikimr::NPQ
