#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <util/generic/yexception.h>

#include <gtest/gtest.h>

namespace NYdb::NOperationId {

const std::string PreparedQueryId = "9d629c27-2c3036b3-4b180476-64435bca";

TEST(OperationIdTest, ConvertKindOnly) {
    Ydb::TOperationId proto;
    proto.set_kind(Ydb::TOperationId::OPERATION_DDL);
    auto str = NKikimr::NOperationId::ProtoToString(proto);
    ASSERT_EQ(str, "ydb://operation/1");
    auto newProto = NKikimr::NOperationId::TOperationId(str);
    ASSERT_EQ(newProto.GetProto().kind(), proto.kind());
    ASSERT_EQ(newProto.GetProto().data_size(), 0);
}

TEST(OperationIdTest, PreparedQueryIdCompatibleFormatter) {
    Ydb::TOperationId opId;
    opId.set_kind(Ydb::TOperationId::PREPARED_QUERY_ID);
    NKikimr::NOperationId::AddOptionalValue(opId, "id", PreparedQueryId);
    auto result = NKikimr::NOperationId::ProtoToString(opId);
    ASSERT_EQ(NKikimr::NOperationId::FormatPreparedQueryIdCompat(PreparedQueryId), result);
}

TEST(OperationIdTest, PreparedQueryIdDecode) {
    const auto queryId = NKikimr::NOperationId::FormatPreparedQueryIdCompat(PreparedQueryId);
    std::string decodedString;
    bool decoded = NKikimr::NOperationId::DecodePreparedQueryIdCompat(queryId, decodedString);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(PreparedQueryId, decodedString);
}

TEST(OperationIdTest, PreparedQueryIdDecodeRawString) {
    std::string decodedString;
    bool decoded = NKikimr::NOperationId::DecodePreparedQueryIdCompat(PreparedQueryId, decodedString);
    ASSERT_FALSE(decoded);
    ASSERT_TRUE(decodedString.empty());
}

TEST(OperationIdTest, PreparedQueryIdDecodeInvalidString) {
    std::string decodedString;
    ASSERT_THROW(
        NKikimr::NOperationId::DecodePreparedQueryIdCompat("ydb://preparedqueryid/4?id=", decodedString), yexception);
    ASSERT_TRUE(decodedString.empty());
}

TEST(OperationIdTest, FormatPrefixShorter) {
    ASSERT_TRUE(std::strlen("ydb://preparedqueryid/4?id=") < PreparedQueryId.size());
}

#if 0
TEST(OperationIdTest, PreparedQueryIdCompatibleFormatterPerf) {
    ui64 x = 0;
    for (int i = 0; i < 10000000; i++) {
        auto result = NKikimr::NOperationId::FormatPreparedQueryIdCompat(PreparedQueryId);
        x += result.size();
    }
    std::cerr << x << std::endl;
}

TEST(OperationIdTest, PreparedQueryIdDecodePerf) {
    ui64 x = 0;
    for (int i = 0; i < 10000000; i++) {
        const auto queryId = NKikimr::NOperationId::FormatPreparedQueryIdCompat(PreparedQueryId);
        std::string decodedString;
        bool decoded = NKikimr::NOperationId::DecodePreparedQueryIdCompat(queryId, decodedString);
        ASSERT_TRUE(decoded);
        ASSERT_EQ(PreparedQueryId, decodedString);
        x += decodedString.size();
    }
    std::cerr << x << std::endl;
}

TEST(OperationIdTest, PreparedQueryIdOldFormatterPerf) {
    ui64 x = 0;
    for (int i = 0; i < 10000000; i++) {
        Ydb::TOperationId opId;
        opId.SetKind(Ydb::TOperationId::PREPARED_QUERY_ID);
        NKikimr::NOperationId::AddOptionalValue(opId, "id", PreparedQueryId);
        auto result = NKikimr::NOperationId::ProtoToString(opId);
        x += result.size();
    }
    std::cerr << x << std::endl;
}
#endif

TEST(OperationIdTest, ConvertKindAndValues) {
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
    auto str = NKikimr::NOperationId::ProtoToString(proto);
    ASSERT_EQ(str, "ydb://operation/1?key1=value1&txId=42");
    auto newProto = NKikimr::NOperationId::TOperationId(str);
    ASSERT_EQ(newProto.GetProto().kind(), proto.kind());
    ASSERT_EQ(newProto.GetProto().data_size(), 2);
    {
        auto data = newProto.GetProto().data(0);
        ASSERT_EQ(data.key(), "key1");
        ASSERT_EQ(data.value(), "value1");
    }
    {
        auto data = newProto.GetProto().data(1);
        ASSERT_EQ(data.key(), "txId");
        ASSERT_EQ(data.value(), "42");
    }
}

TEST(OperationIdTest, InvalidOperationId) {
    ASSERT_THROW(NKikimr::NOperationId::TOperationId("ydb://preparedqueryid"), yexception);
}

}
