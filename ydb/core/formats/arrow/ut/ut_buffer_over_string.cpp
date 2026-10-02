#include <ydb/core/formats/arrow/serializer/stream.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/buffer.h>

Y_UNIT_TEST_SUITE(TBufferOverString) {
    Y_UNIT_TEST(KeepsMemoryAlive) {
        // NOTE: TString has both COW and non-COW builds; this test should pass in both.

        std::shared_ptr<arrow::Buffer> buffer =
            std::make_shared<NKikimr::NArrow::NSerialization::TBufferOverString>(TString(8192, 'A'));

        UNIT_ASSERT(buffer);
        UNIT_ASSERT_VALUES_EQUAL(buffer->size(), 8192);
        for (int64_t i = 0; i < buffer->size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(static_cast<char>(buffer->data()[i]), 'A');
        }
    }
}
