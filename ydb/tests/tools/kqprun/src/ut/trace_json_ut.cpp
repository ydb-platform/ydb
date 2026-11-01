#include <ydb/tests/tools/kqprun/src/trace_json.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKqpRun {
namespace {

using TSpan = opentelemetry::proto::trace::v1::Span;
using TStatus = opentelemetry::proto::trace::v1::Status;

NJson::TJsonValue Serialize(const TSpan& span) {
    NJson::TJsonValue json;
    NJson::ReadJsonTree(SpanToOtlpJson(span), &json, true);
    return json;
}

Y_UNIT_TEST_SUITE(TKqpRunOtlpJson) {
    Y_UNIT_TEST(HexIdsAndNumericEnums) {
        TSpan span;
        span.set_trace_id(HexDecode("00112233445566778899AABBCCDDEEFF"));
        span.set_span_id(HexDecode("0123456789ABCDEF"));
        span.set_parent_span_id(HexDecode("FEDCBA9876543210"));
        span.set_kind(TSpan::SPAN_KIND_SERVER);
        span.mutable_status()->set_code(TStatus::STATUS_CODE_ERROR);
        span.mutable_status()->set_message("failed\n\"query\"");

        const auto json = Serialize(span);
        UNIT_ASSERT_VALUES_EQUAL(json["traceId"].GetString(), "00112233445566778899AABBCCDDEEFF");
        UNIT_ASSERT_VALUES_EQUAL(json["spanId"].GetString(), "0123456789ABCDEF");
        UNIT_ASSERT_VALUES_EQUAL(json["parentSpanId"].GetString(), "FEDCBA9876543210");
        UNIT_ASSERT(json["kind"].IsInteger());
        UNIT_ASSERT_VALUES_EQUAL(json["kind"].GetInteger(), 2);
        UNIT_ASSERT(json["status"]["code"].IsInteger());
        UNIT_ASSERT_VALUES_EQUAL(json["status"]["code"].GetInteger(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["status"]["message"].GetString(), "failed\n\"query\"");
        UNIT_ASSERT(!json.Has("trace_id"));
    }

    Y_UNIT_TEST(LinkIdsAndBytesAttributes) {
        TSpan span;
        auto* link = span.add_links();
        link->set_trace_id(HexDecode("FFEEDDCCBBAA99887766554433221100"));
        link->set_span_id(HexDecode("0011223344556677"));
        link->set_flags(1);
        auto* attribute = link->add_attributes();
        attribute->set_key("traceId");
        attribute->mutable_value()->set_bytes_value(HexDecode("00FF"));
        auto* event = span.add_events();
        event->set_name("event");
        attribute = event->add_attributes();
        attribute->set_key("spanId");
        attribute->mutable_value()->set_bytes_value(HexDecode("FF00"));

        const auto json = Serialize(span);
        const auto& jsonLink = json["links"][0];
        UNIT_ASSERT_VALUES_EQUAL(jsonLink["traceId"].GetString(), "FFEEDDCCBBAA99887766554433221100");
        UNIT_ASSERT_VALUES_EQUAL(jsonLink["spanId"].GetString(), "0011223344556677");
        UNIT_ASSERT_VALUES_EQUAL(jsonLink["flags"].GetInteger(), 1);
        UNIT_ASSERT_VALUES_EQUAL(jsonLink["attributes"][0]["value"]["bytesValue"].GetString(), "AP8=");
        UNIT_ASSERT_VALUES_EQUAL(json["events"][0]["attributes"][0]["value"]["bytesValue"].GetString(), "/wA=");
    }

    Y_UNIT_TEST(PreservesInt64Strings) {
        TSpan span;
        span.set_start_time_unix_nano(1740000000000000001ULL);
        span.set_end_time_unix_nano(1740000000000000002ULL);
        auto* attribute = span.add_attributes();
        attribute->set_key("count");
        attribute->mutable_value()->set_int_value(9223372036854775807LL);

        const auto json = Serialize(span);
        UNIT_ASSERT_VALUES_EQUAL(json["startTimeUnixNano"].GetString(), "1740000000000000001");
        UNIT_ASSERT_VALUES_EQUAL(json["endTimeUnixNano"].GetString(), "1740000000000000002");
        UNIT_ASSERT_VALUES_EQUAL(json["attributes"][0]["value"]["intValue"].GetString(), "9223372036854775807");
    }

    Y_UNIT_TEST(PreservesDoublePrecision) {
        TSpan span;
        constexpr double value = 1.2345678901234567;
        auto* attribute = span.add_attributes();
        attribute->set_key("fraction");
        attribute->mutable_value()->set_double_value(value);

        const auto json = Serialize(span);
        UNIT_ASSERT_VALUES_EQUAL(json["attributes"][0]["value"]["doubleValue"].GetDouble(), value);
    }

    Y_UNIT_TEST(OmitsUnsetParentAndLinks) {
        TSpan span;
        span.set_trace_id(HexDecode("00112233445566778899AABBCCDDEEFF"));
        span.set_span_id(HexDecode("0123456789ABCDEF"));
        span.mutable_status()->set_code(TStatus::STATUS_CODE_OK);

        const auto json = Serialize(span);
        UNIT_ASSERT(!json.Has("parentSpanId"));
        UNIT_ASSERT(!json.Has("links"));
        UNIT_ASSERT_VALUES_EQUAL(json["status"]["code"].GetInteger(), 1);
    }
}

}   // namespace
}   // namespace NKqpRun
