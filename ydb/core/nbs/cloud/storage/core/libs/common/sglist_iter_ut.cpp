#include "sglist_iter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSgListIterTest)
{
    Y_UNIT_TEST(ShouldRead)
    {
        TVector<char> buffer(128, 0);

        TSgList sgList;
        {
            TSgListIter iter(sgList);
            size_t read = iter.Copy(buffer.data(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(read, 0);
            UNIT_ASSERT_VALUES_EQUAL(buffer[0], 0);
        }

        TVector<TString> data = {"ab", "cd", "efg", "h"};
        for (const auto& str: data) {
            sgList.emplace_back(str.data(), str.size());
        }

        {
            TSgListIter iter(sgList);
            size_t read = iter.Copy(buffer.data(), 3);
            UNIT_ASSERT_VALUES_EQUAL(read, 3);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(buffer.data(), 3), "abc");

            read = iter.Copy(buffer.data() + 3, 3);
            UNIT_ASSERT_VALUES_EQUAL(read, 3);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(buffer.data() + 3, 3), "def");

            read = iter.Copy(buffer.data() + 6, 3);
            UNIT_ASSERT_VALUES_EQUAL(read, 2);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(buffer.data() + 6, 2), "gh");

            read = iter.Copy(buffer.data(), 3);
            UNIT_ASSERT_VALUES_EQUAL(read, 0);
        }

        {
            TSgListIter iter(sgList);
            size_t read = iter.Copy(buffer.data(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(read, 8);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(buffer.data(), read),
                                     "abcdefgh");

            read = iter.Copy(buffer.data(), buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(read, 0);
        }
    }
}

}   // namespace NYdb::NBS
