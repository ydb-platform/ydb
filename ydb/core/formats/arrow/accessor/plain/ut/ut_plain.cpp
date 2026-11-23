#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(PlainArrayAccessor) {
    using namespace NKikimr::NArrow::NAccessor;

    Y_UNIT_TEST(PlainAccessorsNoNulls) {
        auto value = std::make_shared<TTrivialArray>(std::make_shared<arrow::UInt8Scalar>(0));
        UNIT_ASSERT(value->HasNulls() == false);
        auto chunked = std::make_shared<TTrivialChunkedArray>(value->GetChunkedArray());
        UNIT_ASSERT(chunked->HasNulls() == false);
    }

    Y_UNIT_TEST(PlainAccessorsWithNulls) {
        auto value = std::make_shared<TTrivialArray>(arrow::MakeNullScalar(arrow::uint8()));
        UNIT_ASSERT(value->HasNulls() == true);
        auto chunked = std::make_shared<TTrivialChunkedArray>(value->GetChunkedArray());
        UNIT_ASSERT(chunked->HasNulls() == true);
    }
}; // Y_UNIT_TEST_SUITE(PlainArrayAccessor)
