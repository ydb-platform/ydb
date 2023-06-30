#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include "utils.h"

TEST_TF(TypeStripTags, StripTags) {
    auto t = f.List(f.Tagged(f.Void(), "Tag"));

    ASSERT_EQ(t->StripTags().Get(), t.Get());
    ASSERT_EQ(f.Tagged(t, "Tag")->StripTags().Get(), t.Get());
    ASSERT_EQ(f.Tagged(f.Tagged(t, "Tag"), "Tag2")->StripTags().Get(), t.Get());
}

TEST_TF(TypeStripTags, StripOptionals) {
    auto t = f.Tagged(f.Optional(f.Void()), "Tag");

    ASSERT_EQ(t->StripOptionals().Get(), t.Get());
    ASSERT_EQ(f.Optional(t)->StripOptionals().Get(), t.Get());
    ASSERT_EQ(f.Optional(f.Optional(t))->StripOptionals().Get(), t.Get());
}

TEST_TF(TypeStripTags, StripTagsAndOptionals) {
    auto t = f.Void();

    ASSERT_EQ(t->StripTagsAndOptionals().Get(), t.Get());
    ASSERT_EQ(f.Optional(t)->StripTagsAndOptionals().Get(), t.Get());
    ASSERT_EQ(f.Optional(f.Optional(t))->StripTagsAndOptionals().Get(), t.Get());
    ASSERT_EQ(f.Optional(f.Tagged(t, "Tag"))->StripTagsAndOptionals().Get(), t.Get());
    ASSERT_EQ(f.Optional(f.Optional(f.Optional(t)))->StripTagsAndOptionals().Get(), t.Get());
}
