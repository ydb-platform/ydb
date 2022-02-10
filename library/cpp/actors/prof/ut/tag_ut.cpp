#include "tag.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NProfiling;

class TAtomTagsTest: public TTestBase {
private:
    UNIT_TEST_SUITE(TAtomTagsTest);
    UNIT_TEST(Test_MakeTag);
    UNIT_TEST(Test_Make2Tags);
    UNIT_TEST(Test_MakeTagTwice);

    UNIT_TEST(Test_MakeAndGetTag);

    UNIT_TEST(Test_MakeVector);
    UNIT_TEST_SUITE_END();

public:
    void Test_MakeTag();
    void Test_Make2Tags();
    void Test_MakeTagTwice();
    void Test_MakeAndGetTag();
    void Test_MakeVector();
};

UNIT_TEST_SUITE_REGISTRATION(TAtomTagsTest);

void TAtomTagsTest::Test_MakeTag() {
    ui32 tag = MakeTag("a tag");
    UNIT_ASSERT(tag != 0);
}

void TAtomTagsTest::Test_Make2Tags() {
    ui32 tag1 = MakeTag("a tag 1");
    ui32 tag2 = MakeTag("a tag 2");
    UNIT_ASSERT(tag1 != 0);
    UNIT_ASSERT(tag2 != 0);
    UNIT_ASSERT(tag1 != tag2);
}

void TAtomTagsTest::Test_MakeTagTwice() {
    ui32 tag1 = MakeTag("a tag twice");
    ui32 tag2 = MakeTag("a tag twice");
    UNIT_ASSERT(tag1 != 0);
    UNIT_ASSERT(tag1 == tag2);
}

void TAtomTagsTest::Test_MakeAndGetTag() {
    const char* makeStr = "tag to get";
    ui32 tag = MakeTag(makeStr);
    const char* tagStr = GetTag(tag);
    UNIT_ASSERT_STRINGS_EQUAL(makeStr, tagStr);
}

void TAtomTagsTest::Test_MakeVector() {
    TVector<const char*> strs = {
        "vector tag 0",
        "vector tag 1",
        "vector tag 3",
        "vector tag 4"};
    ui32 baseTag = MakeTags(strs);
    UNIT_ASSERT(baseTag != 0);
    for (ui32 i = 0; i < strs.size(); ++i) {
        const char* str = GetTag(baseTag + i);
        UNIT_ASSERT_STRINGS_EQUAL(str, strs[i]);
    }
}
