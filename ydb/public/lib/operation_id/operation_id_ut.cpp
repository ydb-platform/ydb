#include "operation_id.h"

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NOperationId {

Y_UNIT_TEST_SUITE(OperationIdTest) {
    const TString PreparedQueryId = "9d629c27-2c3036b3-4b180476-64435bca";

    Y_UNIT_TEST(ConvertKindOnly) {
        Ydb::TOperationId proto;
        proto.SetKind(Ydb::TOperationId::OPERATION_DDL);
        auto str = ProtoToString(proto);
        UNIT_ASSERT_EQUAL(str, "ydb://operation/1");
        auto newProto = TOperationId(str);
        UNIT_ASSERT_EQUAL(newProto.GetKind(), proto.GetKind());
        UNIT_ASSERT_EQUAL(newProto.DataSize(), 0);
    }

    Y_UNIT_TEST(PreparedQueryIdCompatibleFormatter) {
        Ydb::TOperationId opId;
        opId.SetKind(Ydb::TOperationId::PREPARED_QUERY_ID);
        AddOptionalValue(opId, "id", PreparedQueryId);
        auto result = ProtoToString(opId);
        UNIT_ASSERT_VALUES_EQUAL(FormatPreparedQueryIdCompat(PreparedQueryId), result);
    }

    Y_UNIT_TEST(PreparedQueryIdDecode) {
        const auto queryId = FormatPreparedQueryIdCompat(PreparedQueryId);
        TString decodedString;
        bool decoded = DecodePreparedQueryIdCompat(queryId, decodedString);
        UNIT_ASSERT(decoded);
        UNIT_ASSERT_VALUES_EQUAL(PreparedQueryId, decodedString);
    }

    Y_UNIT_TEST(PreparedQueryIdDecodeRawString) {
        TString decodedString;
        bool decoded = DecodePreparedQueryIdCompat(PreparedQueryId, decodedString);
        UNIT_ASSERT(!decoded);
        UNIT_ASSERT(decodedString.empty());
    }

    Y_UNIT_TEST(PreparedQueryIdDecodeInvalidString) {
        TString decodedString;
        UNIT_ASSERT_EXCEPTION(
            DecodePreparedQueryIdCompat(TString("ydb://preparedqueryid/4?id="), decodedString), yexception);
        UNIT_ASSERT(decodedString.empty());
    }

    Y_UNIT_TEST(FormatPrefixShorter) {
        UNIT_ASSERT(TString("ydb://preparedqueryid/4?id=").size() < PreparedQueryId.size());
    }

#if 0
    Y_UNIT_TEST(PreparedQueryIdCompatibleFormatterPerf) {
        ui64 x = 0;
        for (int i = 0; i < 10000000; i++) {
            auto result = FormatPreparedQueryIdCompat(PreparedQueryId);
            x += result.size();
        }
        Cerr << x << Endl;
    }

    Y_UNIT_TEST(PreparedQueryIdDecodePerf) {
        ui64 x = 0;
        for (int i = 0; i < 10000000; i++) {
            const auto queryId = FormatPreparedQueryIdCompat(PreparedQueryId);
            TString decodedString;
            bool decoded = DecodePreparedQueryIdCompat(queryId, decodedString);
            UNIT_ASSERT(decoded);
            UNIT_ASSERT_VALUES_EQUAL(PreparedQueryId, decodedString);
            x += decodedString.size();
        }
        Cerr << x << Endl;
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
        Cerr << x << Endl;
    }
#endif
    Y_UNIT_TEST(ConvertKindAndValues) {
        Ydb::TOperationId proto;
        proto.SetKind(Ydb::TOperationId::OPERATION_DDL);
        {
            auto data = proto.AddData();
            data->SetKey("key1");
            data->SetValue("value1");
        }
        {
            auto data = proto.AddData();
            data->SetKey("txId");
            data->SetValue("42");
        }
        auto str = ProtoToString(proto);
        UNIT_ASSERT_EQUAL(str, "ydb://operation/1?key1=value1&txId=42");
        auto newProto = TOperationId(str);
        UNIT_ASSERT_EQUAL(newProto.GetKind(), proto.GetKind());
        UNIT_ASSERT_EQUAL(newProto.DataSize(), 2);
        {
            auto data = newProto.GetData(0);
            UNIT_ASSERT_EQUAL(data.GetKey(), "key1");
            UNIT_ASSERT_EQUAL(data.GetValue(), "value1");
        }
        {
            auto data = newProto.GetData(1);
            UNIT_ASSERT_EQUAL(data.GetKey(), "txId");
            UNIT_ASSERT_EQUAL(data.GetValue(), "42");
        }
    }
}

} // namespace NOperationId
} // namespace NKikimr
