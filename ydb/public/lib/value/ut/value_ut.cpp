
#include <ydb/public/lib/value/value.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

TEST(TValue, FieldsCount) {
    ASSERT_EQ(NKikimrMiniKQL::TValue::GetDescriptor()->field_count(), 18) << "update the NKikimr::NClient::TValue wrapper to support new fields";
}
