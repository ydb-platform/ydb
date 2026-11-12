#include <ydb/core/kqp/common/simple/session_id.h>
#include <ydb/library/actors/core/actorid.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpSessionId) {
    Y_UNIT_TEST(AcceptsSessionIdFromProducer) {
        for (ui32 nodeId : {1u, 52910u, NActors::TActorId::MaxNodeId}) {
            const TString id = TStringBuilder() << "ydb://session/3?node_id=" << nodeId
                << "&id=" << CGIEscapeRet(Base64Encode(CreateGuidAsString()));
            UNIT_ASSERT(ValidateSessionId(id));
            UNIT_ASSERT_VALUES_EQUAL(*ValidateSessionId(id), nodeId);
        }
    }

    Y_UNIT_TEST(AcceptsExistingSessionId) {
        const auto nodeId = ValidateSessionId(
            "ydb://session/3?node_id=52910&id=MWFmNGYwYTAtYzJkN2RhOWEtZmFkMDhlMTUtZjU0ZDE0OTA%3D");
        UNIT_ASSERT(nodeId);
        UNIT_ASSERT_VALUES_EQUAL(*nodeId, 52910);
    }

    Y_UNIT_TEST(AcceptsReorderedParameters) {
        UNIT_ASSERT(ValidateSessionId("ydb://session/3?id=MS0yLTMtNA%3D%3D&node_id=1"));
    }

    Y_UNIT_TEST(RejectsNodeIdOutsideActorAddressRange) {
        const TString id = TStringBuilder() << "ydb://session/3?node_id=" << NActors::TActorId::MaxNodeId + 1
            << "&id=MS0yLTMtNA%3D%3D";
        UNIT_ASSERT(!ValidateSessionId(id));
    }

    Y_UNIT_TEST(RejectsMalformedIds) {
        for (TStringBuf id : {
            "",
            "session-id",
            "ydb://session/3?node_id=1",
            "ydb://session/3?id=MS0yLTMtNA%3D%3D",
            "ydb://session/3?node_id=0&id=MS0yLTMtNA%3D%3D",
            "ydb://session/3?node_id=-1&id=MS0yLTMtNA%3D%3D",
            "ydb://session/3?node_id=4294967296&id=MS0yLTMtNA%3D%3D",
            "ydb://session/3?node_id=one&id=MS0yLTMtNA%3D%3D",
            "ydb://session/3?node_id=1&node_id=2&id=MS0yLTMtNA%3D%3D",
            "ydb://session/3?node_id=1&id=MS0yLTMtNA%3D%3D&id=MS0yLTMtNA%3D%3D",
            "ydb://session/3?node_id=1&id=MS0yLTMtNA%3D%3D&extra=1",
            "ydb://session/3?node_id=1&id=",
            "ydb://session/3?node_id=1&id=not_base64!",
            "ydb://session/3?node_id=1&id=bm90LWEtZ3VpZA%3D%3D",
            "ydb://session/3?node_id=1&id=MS0yLTMtNA%",
            "ydb://session/3?node_id=1&id=MS0yLTMtNA%3",
            "ydb://session/3?node_id=1&id=MS0yLTMtNA%GG",
            "ydb://session/3?node_id=1&id=MS0yLTMtNA%3D%3D#fragment",
            "ydb://session/3?node_id=1&id=MS0yLTMtNA%3D%3D#",
            "ydb://user@session/3?node_id=1&id=MS0yLTMtNA%3D%3D",
            "ydb://session:2135/3?node_id=1&id=MS0yLTMtNA%3D%3D",
            "https://session/3?node_id=1&id=MS0yLTMtNA%3D%3D",
            "ydb://operation/3?node_id=1&id=MS0yLTMtNA%3D%3D",
            "ydb://session/1?node_id=1&id=MS0yLTMtNA%3D%3D",
            "ydb://session/3/extra?node_id=1&id=MS0yLTMtNA%3D%3D",
            "ydb://session/extra/../3?node_id=1&id=MS0yLTMtNA%3D%3D",
            " ydb://session/3?node_id=1&id=MS0yLTMtNA%3D%3D",
            "ydb://session/3?node_id=1&id=MS0yLTMtNA%3D%3D\n",
        }) {
            UNIT_ASSERT_C(!ValidateSessionId(id), id);
        }
    }
}

} // namespace NKikimr::NKqp
