#include "fragmented_buffer.h"
#include "lz4_data_generator.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TFragmentedBufferTest) {
    Y_UNIT_TEST(TestWriteRead) {
        const char *data2 = "234";
        TFragmentedBuffer fb;
        fb.Write(1, data2, 3);
        char buffer[4];
        buffer[sizeof(buffer) - 1] = 0;
        fb.Read(1, buffer, 3);
        UNIT_ASSERT_VALUES_EQUAL(buffer, data2);
    }

    Y_UNIT_TEST(TestOverwriteRead) {
        const char *data2 = "234";
        const char *data3 = "456";
        TFragmentedBuffer fb;
        fb.Write(1, data2, 3);
        fb.Write(1, data3, 3);
        char buffer[4];
        buffer[sizeof(buffer) - 1] = 0;
        fb.Read(1, buffer, 3);
        UNIT_ASSERT_VALUES_EQUAL(buffer, data3);
    }

    Y_UNIT_TEST(TestIntersectedWriteRead) {
        const char *data2 = "234";
        const char *data3 = "456";
        TFragmentedBuffer fb;
        fb.Write(1, data2, 3);
        fb.Write(3, data3, 3);
        char buffer[6];
        buffer[sizeof(buffer) - 1] = 0;
        fb.Read(1, buffer, 5);
        UNIT_ASSERT_VALUES_EQUAL(buffer, "23456");
    }

    Y_UNIT_TEST(TestIntersectedWriteRead2) {
        const char *data2 = "234";
        const char *data3 = "456";
        TFragmentedBuffer fb;
        fb.Write(3, data3, 3);
        fb.Write(1, data2, 3);
        char buffer[6];
        buffer[sizeof(buffer) - 1] = 0;
        fb.Read(1, buffer, 5);
        UNIT_ASSERT_VALUES_EQUAL(buffer, "23456");
    }

    Y_UNIT_TEST(TestIntersectedWriteRead3) {
        const char *data2 = "234";
        const char *data3 = "456";
        const char *data4 = "678";
        TFragmentedBuffer fb;
        fb.Write(5, data4, 3);
        fb.Write(1, data2, 3);
        fb.Write(3, data3, 3);
        char buffer[8];
        buffer[sizeof(buffer) - 1] = 0;
        fb.Read(1, buffer, 7);
        UNIT_ASSERT_VALUES_EQUAL(buffer, "2345678");
    }

    Y_UNIT_TEST(Test3WriteRead) {
        const char *data2 = "234";
        const char *data3v2 = "5";
        const char *data4 = "678";
        TFragmentedBuffer fb;
        fb.Write(4, data3v2, 1);
        fb.Write(5, data4, 3);
        fb.Write(1, data2, 3);
        char buffer[8];
        buffer[sizeof(buffer) - 1] = 0;
        fb.Read(1, buffer, 7);
        UNIT_ASSERT_VALUES_EQUAL(buffer, "2345678");
    }

    Y_UNIT_TEST(Test5WriteRead) {
        const char *data1 = "1";
        const char *data2 = "234";
        const char *data3 = "456";
        const char *data4 = "678";
        const char *data5 = "9";
        TFragmentedBuffer fb;
        fb.Write(5, data4, 3);
        fb.Write(1, data2, 3);
        fb.Write(3, data3, 3);
        fb.Write(0, data1, 1);
        fb.Write(8, data5, 1);
        char buffer[10];
        buffer[sizeof(buffer) - 1] = 0;
        fb.Read(0, buffer, 9);
        UNIT_ASSERT_VALUES_EQUAL(buffer, "123456789");

        char buffer2[6];
        buffer2[sizeof(buffer2) - 1] = 0;
        fb.Read(2, buffer2, 5);
        UNIT_ASSERT_VALUES_EQUAL(buffer2, "34567");

    }

    Y_UNIT_TEST(TestIsNotMonolith) {
        const char *data2 = "234";
        const char *data3v2 = "5";
        const char *data4 = "678";
        TFragmentedBuffer fb;
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), false);
        fb.Write(3, data3v2, 1);
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), false);
        fb.Write(4, data4, 3);
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), false);
        fb.Write(0, data2, 3);
        char buffer[8];
        buffer[sizeof(buffer) - 1] = 0;
        fb.Read(0, buffer, 7);
        UNIT_ASSERT_VALUES_EQUAL(buffer, "2345678");
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), true);
    }

    Y_UNIT_TEST(TestGetMonolith) {
        const char *data2 = "234";
        TFragmentedBuffer fb;
        fb.Write(0, data2, 3);
        char buffer[4];
        buffer[sizeof(buffer) - 1] = 0;
        fb.Read(0, buffer, 3);
        UNIT_ASSERT_VALUES_EQUAL(buffer, data2);
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), true);
        TRope res = fb.GetMonolith();
        UNIT_ASSERT_VALUES_EQUAL(res.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(memcmp(res.GetContiguousSpan().data(), data2, 3), 0);
    }

    Y_UNIT_TEST(TestSetMonolith) {
        TRope inData(TString("123"));
        TFragmentedBuffer fb;
        fb.SetMonolith(TRope(inData));
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), true);
        TRope res = fb.GetMonolith();
        UNIT_ASSERT_VALUES_EQUAL(inData.ConvertToString(), res.ConvertToString());
    }

    Y_UNIT_TEST(TestReplaceWithSetMonolith) {
        TRope inData(TString("123"));
        const char *data3v2 = "5";
        const char *data4 = "678";
        TFragmentedBuffer fb;
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), false);
        fb.Write(3, data3v2, 1);
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), false);
        fb.Write(4, data4, 3);
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), false);
        fb.SetMonolith(TRope(inData));
        UNIT_ASSERT_VALUES_EQUAL(fb.IsMonolith(), true);
        TRope res = fb.GetMonolith();
        UNIT_ASSERT_VALUES_EQUAL(inData.ConvertToString(), res.ConvertToString());
    }

    Y_UNIT_TEST(CopyFrom) {
        TFragmentedBuffer buffer;
        buffer.Write(0, "HELLO", 5);
        buffer.Write(10, "WORLD", 5);
        TFragmentedBuffer copy;
        copy.CopyFrom(buffer, {0, 5});
        buffer.Write(5, "BRAVE", 5);
        copy.CopyFrom(buffer, {5, 15});
        UNIT_ASSERT_VALUES_EQUAL(copy.Read(0, 5).ConvertToString(), "HELLO");
        UNIT_ASSERT_VALUES_EQUAL(copy.Read(10, 5).ConvertToString(), "WORLD");
        UNIT_ASSERT_VALUES_EQUAL(copy.Read(12, 3).ConvertToString(), "RLD");
        copy.CopyFrom(buffer, {0, 15});
        UNIT_ASSERT_VALUES_EQUAL(copy.Read(0, 15).ConvertToString(), "HELLOBRAVEWORLD");
    }

    Y_UNIT_TEST(ReadWriteRandom) {
        const size_t maxLength = 10000;
        TString reference(maxLength, 0);
        TFragmentedBuffer buffer;
        TIntervalSet<i32> written;

        auto ropify = [](TString buffer) {
            TRope result;
            size_t offset = 0;
            while (offset < buffer.size()) {
                size_t length = 1 + RandomNumber<size_t>(buffer.size() - offset);
                result.Insert(result.End(), TRcBuf::Copy(buffer.data() + offset, length));
                offset += length;
            }
            return result;
        };

        for (ui32 iter = 0; iter < 10000; ++iter) {
            size_t writeOffset = RandomNumber<size_t>(maxLength);
            size_t writeLength = 1 + RandomNumber<size_t>(Min<size_t>(100, maxLength - writeOffset));
            TString data = FastGenDataForLZ4(writeLength, iter);

            memcpy(reference.Detach() + writeOffset, data.data(), writeLength);
            written.Add(writeOffset, writeOffset + writeLength);

            buffer.Write(writeOffset, ropify(data));

            size_t index = RandomNumber<size_t>(written.Size());
            auto it = written.begin();
            for (size_t i = 0; i < index; ++i) {
                ++it;
            }
            auto [begin, end] = *it;
            UNIT_ASSERT(begin < end);
            begin += RandomNumber<size_t>(end - begin);
            ui32 size = 1 + RandomNumber<size_t>(end - begin);

            UNIT_ASSERT_EQUAL(buffer.Read(begin, size).ConvertToString(), reference.substr(begin, size));
        }
    }
}

} // NKikimr
