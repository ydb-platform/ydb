#include <ydb/core/persqueue/common/schema_change.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TSchemaChangeInfoTest) {

Y_UNIT_TEST(ParseAndSerializeRoundTrip) {
    NKikimrPQ::TSchemaChangeInfo proto;
    proto.SetStep(7);
    proto.SetTxId(9);
    proto.SetData("schema");

    const TSchemaChangeInfo parsed = TSchemaChangeInfo::Parse(proto);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Version.Step, 7u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Version.TxId, 9u);
    UNIT_ASSERT_VALUES_EQUAL(parsed.Data, "schema");

    NKikimrPQ::TSchemaChangeInfo out;
    parsed.Serialize(out);
    UNIT_ASSERT_VALUES_EQUAL(out.GetStep(), 7u);
    UNIT_ASSERT_VALUES_EQUAL(out.GetTxId(), 9u);
    UNIT_ASSERT_VALUES_EQUAL(out.GetData(), "schema");
}

Y_UNIT_TEST(IsSchemaChangeVersionReleased) {
    const TRowVersion version(10, 1);
    const TRowVersion lastEmitted(5, 0);
    const TRowVersion committed(10, 1);
    UNIT_ASSERT(IsSchemaChangeVersionReleased(version, lastEmitted, committed));
    UNIT_ASSERT(IsSchemaChangeVersionReleased(version, TRowVersion(10, 1), TRowVersion(0, 0)));
    UNIT_ASSERT(!IsSchemaChangeVersionReleased(version, lastEmitted, TRowVersion(9, 99)));
    UNIT_ASSERT(IsSchemaChangeVersionReleased(version, version, version));
}

Y_UNIT_TEST(SelectSchemaChangeForAck) {
    TSchemaChangeInfo proposed{
        .Version = TRowVersion(2, 0),
        .Data = "new",
    };
    UNIT_ASSERT_VALUES_EQUAL(SelectSchemaChangeForAck(proposed, Nothing()).Data, "new");

    TSchemaChangeInfo older{
        .Version = TRowVersion(1, 0),
        .Data = "old",
    };
    UNIT_ASSERT_VALUES_EQUAL(SelectSchemaChangeForAck(proposed, older).Data, "new");

    TSchemaChangeInfo newer{
        .Version = TRowVersion(3, 0),
        .Data = "newer",
    };
    const TSchemaChangeInfo selected = SelectSchemaChangeForAck(proposed, newer);
    UNIT_ASSERT_VALUES_EQUAL(selected.Data, "newer");
    UNIT_ASSERT_VALUES_EQUAL(selected.Version.Step, 3u);
}

} // Y_UNIT_TEST_SUITE(TSchemaChangeInfoTest)

} // namespace NKikimr::NPQ
