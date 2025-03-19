#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NOperationId {

Y_UNIT_TEST_SUITE(OperationIdTest) {
    const std::string PreparedQueryId = "9d629c27-2c3036b3-4b180476-64435bca";

    Y_UNIT_TEST(ConvertKindOnly) {
        Ydb::TOperationId proto;
        proto.set_kind(Ydb::TOperationId::OPERATION_DDL);
        auto str = ProtoToString(proto);
        UNIT_ASSERT_EQUAL(str, "ydb://operation/1");
        auto newProto = TOperationId(str);
        UNIT_ASSERT_EQUAL(newProto.GetProto().kind(), proto.kind());
        UNIT_ASSERT_EQUAL(newProto.GetProto().data_size(), 0);
    }

    Y_UNIT_TEST(PreparedQueryIdCompatibleFormatter) {
        Ydb::TOperationId opId;
        opId.set_kind(Ydb::TOperationId::PREPARED_QUERY_ID);
        AddOptionalValue(opId, "id", PreparedQueryId);
        auto result = ProtoToString(opId);
        UNIT_ASSERT_VALUES_EQUAL(FormatPreparedQueryIdCompat(PreparedQueryId), result);
    }

    Y_UNIT_TEST(PreparedQueryIdDecode) {
        const auto queryId = FormatPreparedQueryIdCompat(PreparedQueryId);
        std::string decodedString;
        bool decoded = DecodePreparedQueryIdCompat(queryId, decodedString);
        UNIT_ASSERT(decoded);
        UNIT_ASSERT_VALUES_EQUAL(PreparedQueryId, decodedString);
    }

    Y_UNIT_TEST(PreparedQueryIdDecodeRawString) {
        std::string decodedString;
        bool decoded = DecodePreparedQueryIdCompat(PreparedQueryId, decodedString);
        UNIT_ASSERT(!decoded);
        UNIT_ASSERT(decodedString.empty());
    }

    Y_UNIT_TEST(PreparedQueryIdDecodeInvalidString) {
        std::string decodedString;
        UNIT_ASSERT_EXCEPTION(
            DecodePreparedQueryIdCompat(std::string("ydb://preparedqueryid/4?id="), decodedString), yexception);
        UNIT_ASSERT(decodedString.empty());
    }

    Y_UNIT_TEST(FormatPrefixShorter) {
        UNIT_ASSERT(std::string("ydb://preparedqueryid/4?id=").size() < PreparedQueryId.size());
    }

#if 0
    Y_UNIT_TEST(PreparedQueryIdCompatibleFormatterPerf) {
        ui64 x = 0;
        for (int i = 0; i < 10000000; i++) {
            auto result = FormatPreparedQueryIdCompat(PreparedQueryId);
            x += result.size();
        }
        std::cerr << x << std::endl;
    }

    Y_UNIT_TEST(PreparedQueryIdDecodePerf) {
        ui64 x = 0;
        for (int i = 0; i < 10000000; i++) {
            const auto queryId = FormatPreparedQueryIdCompat(PreparedQueryId);
            std::string decodedString;
            bool decoded = DecodePreparedQueryIdCompat(queryId, decodedString);
            UNIT_ASSERT(decoded);
            UNIT_ASSERT_VALUES_EQUAL(PreparedQueryId, decodedString);
            x += decodedString.size();
        }
        std::cerr << x << std::endl;
    }

    Y_UNIT_TEST(PreparedQueryIdOldFormatterPerf) {
        ui64 x = 0;
        for (int i = 0; i < 10000000; i++) {
            Ydb::TOperationId opId;
            opId.SetKind(Ydb::TOperationId::PREPARED_QUERY_ID);
            AddOptionalValue(opId, "id", PreparedQueryId);
            auto result = ProtoToString(opId);
            x += result.size();
        }
        std::cerr << x << std::endl;
    }
#endif
    Y_UNIT_TEST(ConvertKindAndValues) {
        Ydb::TOperationId proto;
        proto.set_kind(Ydb::TOperationId::OPERATION_DDL);
        {
            auto data = proto.add_data();
            data->set_key("key1");
            data->set_value("value1");
        }
        {
            auto data = proto.add_data();
            data->set_key("txId");
            data->set_value("42");
        }
        auto str = ProtoToString(proto);
        UNIT_ASSERT_EQUAL(str, "ydb://operation/1?key1=value1&txId=42");
        auto newProto = TOperationId(str);
        UNIT_ASSERT_EQUAL(newProto.GetProto().kind(), proto.kind());
        UNIT_ASSERT_EQUAL(newProto.GetProto().data_size(), 2);
        {
            auto data = newProto.GetProto().data(0);
            UNIT_ASSERT_EQUAL(data.key(), "key1");
            UNIT_ASSERT_EQUAL(data.value(), "value1");
        }
        {
            auto data = newProto.GetProto().data(1);
            UNIT_ASSERT_EQUAL(data.key(), "txId");
            UNIT_ASSERT_EQUAL(data.value(), "42");
        }
    }
}

} // namespace NOperationId
} // namespace NKikimr
