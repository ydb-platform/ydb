
#include <ydb/public/lib/value/value.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <google/protobuf/text_format.h>

TEST(TValue, FieldsCount) {
    ASSERT_EQ(NKikimrMiniKQL::TValue::GetDescriptor()->field_count(), 18) << "update the NKikimr::NClient::TValue wrapper to support new fields";
}

static NKikimrMiniKQL::TResult ConstructResult(TStringBuf data) {
    NKikimrMiniKQL::TResult result;
    Y_ABORT_UNLESS(google::protobuf::TextFormat::ParseFromString(data, &result));
    return result;
}

static void TestIsNull(TStringBuf data, bool expected) {
    const auto result = ConstructResult(data);
    const auto value = NKikimr::NClient::TValue::Create(result);
    EXPECT_EQ(value.IsNull(), expected) << TString(data).Quote();
    EXPECT_EQ(value.GetValue().ByteSize() == 0, expected) << TString(data).Quote();
}

TEST(TValue, IsNull) {
    TestIsNull("", true);
    TestIsNull("Value {}", true);
    TestIsNull("Value { Int32: 50; }", false);
    TestIsNull("Value { Uint32: 100; }", false);
    TestIsNull("Value { Int64: 1000; }", false);
    TestIsNull("Value { Uint64: 2000; }", false);
    TestIsNull("Value { Float: 1.5; }", false);
    TestIsNull("Value { Double: 2.5; }", false);
    TestIsNull("Value { Bool: false; }", false);
    TestIsNull("Value { Bytes: \"hello\"; }", false);
    TestIsNull("Value { Text: \"world\"; }", false);
    TestIsNull("Value { Optional: {}; }", false);
    TestIsNull("Value { Optional: { Int32: 50}; }", false);
    TestIsNull("Value { Optional: { Uint32: 100}; }", false);
    TestIsNull("Value { Optional: { Int64: 1000}; }", false);
    TestIsNull("Value { Optional: { Uint64: 2000}; }", false);
    TestIsNull("Value { Optional: { Float: 1.5}; }", false);
    TestIsNull("Value { Optional: { Double: 2.5}; }", false);
    TestIsNull("Value { Optional: { Bool: true}; }", false);
    TestIsNull("Value { Optional: { Bytes: \"hello\"}; }", false);
    TestIsNull("Value { Optional: { Text: \"world\"}; }", false);
    TestIsNull("Value { List: [] }", true);
    TestIsNull("Value { List: [{Int32: 50}] }", false);
    TestIsNull("Value { List: [{Int32: 50}, {Int32: 60}] }", false);
    TestIsNull("Value { Tuple: [] }", true);
    TestIsNull("Value { Tuple: [{Int32: 50}] }", false);
    TestIsNull("Value { Tuple: [{Int32: 50}, {Uint32: 100}] }", false);
    TestIsNull("Value { Struct: [] }", true);
    TestIsNull("Value { Struct: [{Int32: 50}] }", false);
    TestIsNull("Value { Struct: [{Int32: 50}, {Uint32: 100}] }", false);
    TestIsNull("Value { Dict: [] }", true);
    TestIsNull("Value { Dict: [{Key: {Int32: 1}, Payload: {Text: \"one\"}}] }", false);
    TestIsNull("Value { Dict: [{Key: {Int32: 1}, Payload: {Text: \"one\"}}, {Key: {Int32: 2}, Payload: {Text: \"two\"}}] }", false);
}
