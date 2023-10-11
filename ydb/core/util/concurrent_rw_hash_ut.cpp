#include <concurrent_rw_hash.h>

#include <library/cpp/testing/unittest/gtest.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

TEST(TConcurrentRWHashTest, TEmptyGetTest) {
    TConcurrentRWHashMap<TString, ui32> h;

    EXPECT_FALSE(h.Has("key"));

    ui32 res = 100;
    EXPECT_FALSE(h.Get("key", res));
    EXPECT_EQ(res, 100);

    // Can't check h.Get("key") here because it has Y_ABORT_UNLESS inside o_O
}

TEST(TConcurrentRWHashTest, TInsertTest) {
    TConcurrentRWHashMap<TString, ui32> h;

    h.Insert("key1", 1);
    h.Insert("key2", 2);

    EXPECT_EQ(h.Get("key1"), 1);
    EXPECT_EQ(h.Get("key2"), 2);

    ui32 res = 100;
    EXPECT_TRUE(h.Has("key1"));
    EXPECT_TRUE(h.Get("key1", res));
    EXPECT_EQ(res, 1);

    EXPECT_TRUE(h.Has("key2"));
    EXPECT_TRUE(h.Get("key2", res));
    EXPECT_EQ(res, 2);

    EXPECT_FALSE(h.Has("key3"));
    EXPECT_FALSE(h.Get("key3", res));
    EXPECT_EQ(res, 2);
}

TEST(TConcurrentRWHashTest, TInsertIfAbsentTest) {
    TConcurrentRWHashMap<TString, ui32> h;

    ui32 res;
    EXPECT_FALSE(h.Has("key"));
    EXPECT_FALSE(h.Get("key", res));

    EXPECT_EQ(h.InsertIfAbsent("key", 1), static_cast<ui32>(1));
    EXPECT_EQ(h.Get("key"), 1);

    EXPECT_EQ(h.InsertIfAbsent("key", 2), static_cast<ui32>(1));
    EXPECT_EQ(h.Get("key"), 1);
}

TEST(TConcurrentRWHashTest, TInsertIfAbsentTestFunc) {
    TConcurrentRWHashMap<TString, ui32> h;

    bool initialized = false;
    auto f = [&initialized]() {
        initialized = true;
        return static_cast<ui32>(1);
    };

    ui32 res = 0;
    EXPECT_FALSE(h.Get("key", res));
    EXPECT_EQ(res, 0);

    EXPECT_EQ(h.InsertIfAbsentWithInit("key", f), 1);
    EXPECT_EQ(h.Get("key"), 1);
    EXPECT_TRUE(initialized);

    initialized = false;
    EXPECT_EQ(h.InsertIfAbsentWithInit("key", f), 1);
    EXPECT_EQ(h.Get("key"), 1);
    EXPECT_FALSE(initialized);

    h.Insert("key", 2);
    EXPECT_EQ(h.InsertIfAbsentWithInit("key", f), 2);
    EXPECT_EQ(h.Get("key"), 2);
    EXPECT_FALSE(initialized);
}

TEST(TConcurrentRWHashTest, TRemoveTest) {
    TConcurrentRWHashMap<TString, ui32> h;

    EXPECT_FALSE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));

    h.Insert("key1", 1);
    EXPECT_TRUE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));
    EXPECT_EQ(h.Get("key1"), 1);

    EXPECT_EQ(h.Remove("key1"), 1);
    EXPECT_FALSE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));

    h.Insert("key1", 1);
    h.Insert("key2", 2);

    EXPECT_TRUE(h.Has("key1"));
    EXPECT_TRUE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));
    EXPECT_EQ(h.Get("key1"), 1);
    EXPECT_EQ(h.Get("key2"), 2);

    EXPECT_EQ(h.Remove("key2"), 2);
    EXPECT_TRUE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));
    EXPECT_EQ(h.Get("key1"), 1);
}

TEST(TConcurrentRWHashTest, TEraseTest) {
    TConcurrentRWHashMap<TString, ui32> h;

    EXPECT_FALSE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));

    h.Insert("key1", 1);
    EXPECT_TRUE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));
    EXPECT_EQ(h.Get("key1"), 1);

    EXPECT_TRUE(h.Erase("key1"));
    EXPECT_FALSE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));

    h.Insert("key1", 1);
    h.Insert("key2", 2);

    EXPECT_TRUE(h.Has("key1"));
    EXPECT_TRUE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));
    EXPECT_EQ(h.Get("key1"), 1);
    EXPECT_EQ(h.Get("key2"), 2);

    EXPECT_TRUE(h.Erase("key2"));
    EXPECT_TRUE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));
    EXPECT_EQ(h.Get("key1"), 1);

    EXPECT_FALSE(h.Erase("key2"));
}

