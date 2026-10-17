#include <ydb/core/persqueue/common/heartbeat.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(THeartbeatTest) {

Y_UNIT_TEST(ParseAndSerializeRoundTrip) {
    NKikimrPQ::THeartbeat proto;
    proto.SetStep(11);
    proto.SetTxId(22);
    proto.SetData("hb-data");

    const THeartbeat parsed = THeartbeat::Parse(proto);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Version.Step, 11u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Version.TxId, 22u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Data, "hb-data");

    NKikimrPQ::THeartbeat out;
    parsed.Serialize(out);
    UNIT_ASSERT_VALUES_EQUAL(out.GetStep(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(out.GetTxId(), 22u);
    UNIT_ASSERT_VALUES_EQUAL(out.GetData(), "hb-data");
}

Y_UNIT_TEST(EmptyDefaults) {
    THeartbeat hb;
    hb.Version = TRowVersion(0, 0);
    NKikimrPQ::THeartbeat proto;
    hb.Serialize(proto);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetStep(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetTxId(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetData(), "");

    const THeartbeat parsed = THeartbeat::Parse(proto);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Version.Step, 0u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Data, "");
}

} // Y_UNIT_TEST_SUITE(THeartbeatTest)

} // namespace NKikimr::NPQ
