
#include <library/cpp/testing/unittest/registar.h>
#include <util/folder/path.h>

Y_UNIT_TEST_SUITE(YdbCliPathTests) {

    Y_UNIT_TEST(TestPath) {
        TFsPath root("root");

        auto child = root.Child("child");
        UNIT_ASSERT_VALUES_EQUAL(child.GetPath(), "root/child"); // Should fail on Windows
    }
}