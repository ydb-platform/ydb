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

NKikimrPQ::TWriteId MakeDeferredWriteId(ui64 intPublicationId, const TString& extPublicationId)
{
    NKikimrPQ::TWriteId writeId;
    auto* deferred = writeId.MutableDeferredPublicationApi();
    deferred->SetIntPublicationId(intPublicationId);
    deferred->SetExtPublicationId(extPublicationId);
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

Y_UNIT_TEST(DowngradeToLegacyTopicClearsKafkaLegacy) {
    auto writeId = MakeLegacyKafkaWriteId(7, 8);
    UpgradeFromLegacy(writeId);
    writeId.ClearId();
    auto* topicApi = writeId.MutableTopicApi();
    topicApi->SetNodeId(1);
    topicApi->SetKeyId(2);

    DowngradeToLegacy(writeId);

    UNIT_ASSERT(!writeId.GetKafkaTransaction());
    UNIT_ASSERT(!writeId.HasKafkaProducerInstanceId());
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetNodeId(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetKeyId(), 2u);
}

Y_UNIT_TEST(DowngradeToLegacyKafkaClearsTopicLegacy) {
    auto writeId = MakeLegacyTopicWriteId(10, 20);
    UpgradeFromLegacy(writeId);
    writeId.ClearId();
    auto* producerId = writeId.MutableKafkaApi()->MutableKafkaProducerInstanceId();
    producerId->SetId(7);
    producerId->SetEpoch(8);

    DowngradeToLegacy(writeId);

    UNIT_ASSERT(writeId.GetKafkaTransaction());
    UNIT_ASSERT(!writeId.HasNodeId());
    UNIT_ASSERT(!writeId.HasKeyId());
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetKafkaProducerInstanceId().GetId(), 7);
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetKafkaProducerInstanceId().GetEpoch(), 8);
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

Y_UNIT_TEST(SetWriteIdDeferredRoundTrip) {
    const TWriteId original(MakeDeferredWriteId(42, "ext-42"));
    NKikimrPQ::TDataTransaction tx;
    SetWriteId(tx, original);

    const TWriteId restored = GetWriteId(tx);
    UNIT_ASSERT(restored.IsDeferredPublicationApiTransaction());
    UNIT_ASSERT_VALUES_EQUAL(restored, original);

    const auto& proto = tx.GetWriteId();
    UNIT_ASSERT(HasCanonical(proto));
    UNIT_ASSERT(proto.HasDeferredPublicationApi());
    UNIT_ASSERT_VALUES_EQUAL(proto.GetDeferredPublicationApi().GetIntPublicationId(), 42u);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetDeferredPublicationApi().GetExtPublicationId(), "ext-42");
}

Y_UNIT_TEST(DowngradeToLegacyKeepsDeferredCanonical) {
    auto writeId = MakeDeferredWriteId(7, "ext-7");
    UNIT_ASSERT(HasCanonical(writeId));

    DowngradeToLegacy(writeId);

    UNIT_ASSERT(HasCanonical(writeId));
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetDeferredPublicationApi().GetIntPublicationId(), 7u);
    UNIT_ASSERT_VALUES_EQUAL(writeId.GetDeferredPublicationApi().GetExtPublicationId(), "ext-7");
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

Y_UNIT_TEST(DeferredWriteIdsAreDistinct) {
    const TWriteId first(MakeDeferredWriteId(1, "ext-a"));
    const TWriteId second(MakeDeferredWriteId(2, "ext-b"));

    UNIT_ASSERT(first != second);
    UNIT_ASSERT_VALUES_UNEQUAL(first.GetHash(), second.GetHash());
}

} // Y_UNIT_TEST_SUITE(TWriteIdEquality)

} // namespace
} // namespace NKikimr::NPQ
