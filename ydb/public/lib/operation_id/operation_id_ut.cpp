#include "operation_id.h" 
 
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
 
namespace NKikimr { 
namespace NOperationId {
 
Y_UNIT_TEST_SUITE(OperationIdTest) {
    Y_UNIT_TEST(ConvertKindOnly) {
        Ydb::TOperationId proto;
        proto.SetKind(Ydb::TOperationId::OPERATION_DDL);
        auto str = ProtoToString(proto); 
        UNIT_ASSERT_EQUAL(str, "ydb://operation/1"); 
        auto newProto = TOperationId(str); 
        UNIT_ASSERT_EQUAL(newProto.GetKind(), proto.GetKind()); 
        UNIT_ASSERT_EQUAL(newProto.DataSize(), 0); 
    } 
 
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
