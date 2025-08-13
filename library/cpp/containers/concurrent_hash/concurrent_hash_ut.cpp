#include "concurrent_hash.h"

#include <library/cpp/testing/unittest/gtest.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/string/cast.h>

TEST(TConcurrentHashTest, TEmptyGetTest) {
    TConcurrentHashMap<TString, ui32> h;

    EXPECT_FALSE(h.Has("key"));

    ui32 res = 100;
    EXPECT_FALSE(h.Get("key", res));
    EXPECT_EQ(res, 100);

    // Can't check h.Get("key") here because it has Y_ABORT_UNLESS inside o_O
}

TEST(TConcurrentHashTest, TInsertTest) {
    TConcurrentHashMap<TString, ui32> h;

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

TEST(TConcurrentHashTest, TInsertIfAbsentTest) {
    TConcurrentHashMap<TString, ui32> h;

    ui32 res;
    EXPECT_FALSE(h.Has("key"));
    EXPECT_FALSE(h.Get("key", res));

    EXPECT_EQ(h.InsertIfAbsent("key", 1), static_cast<ui32>(1));
    EXPECT_EQ(h.Get("key"), 1);

    EXPECT_EQ(h.InsertIfAbsent("key", 2), static_cast<ui32>(1));
    EXPECT_EQ(h.Get("key"), 1);
}

TEST(TConcurrentHashTest, TInsertIfAbsentTestFunc) {
    TConcurrentHashMap<TString, ui32> h;

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


TEST(TConcurrentHashTest, TEmplaceIfAbsentTest) {
    struct TBadConstructor{};

    // InsertIfAbsent cannot be uses for noncopyable and nonmovable types (e.g. atomics or structs with atomic members)
    struct TFoo : public TNonCopyable {
        explicit TFoo(int value)
            : Value(value)
        {}

        explicit TFoo(TBadConstructor) {
            ythrow yexception{} << "THis constructor must not be called";
        }

        int Value;
    };

    TConcurrentHashMap<TString, TFoo> h;

    EXPECT_FALSE(h.Has("key"));

    EXPECT_EQ(h.EmplaceIfAbsent("key", 123).Value, 123);
    EXPECT_TRUE(h.Has("key"));

    // If the key already exists, the value must not be constructed
    EXPECT_EQ(h.EmplaceIfAbsent("key", TBadConstructor{}).Value, 123);
}

TEST(TConcurrentHashTest, TRemoveTest) {
    TConcurrentHashMap<TString, ui32> h;

    EXPECT_FALSE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));

    h.Insert("key1", 1);
    EXPECT_TRUE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));
    EXPECT_EQ(h.Get("key1"), 1);

    h.Remove("key1");
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

    h.Remove("key2");
    EXPECT_TRUE(h.Has("key1"));
    EXPECT_FALSE(h.Has("key2"));
    EXPECT_FALSE(h.Has("key3"));
    EXPECT_EQ(h.Get("key1"), 1);
}

TEST(TConcurrentHashTest, TTryRemoveTest) {
    TConcurrentHashMap<TString, ui32> h;

    EXPECT_FALSE(h.Has("key"));

    ui32 res;
    EXPECT_FALSE(h.TryRemove("key", res));

    h.Insert("key", 1);
    EXPECT_TRUE(h.Has("key"));
    EXPECT_TRUE(h.TryRemove("key", res));
    EXPECT_EQ(res, 1);
    EXPECT_FALSE(h.TryRemove("key", res));
}

TEST(TConcurrentHashTest, TExchangeTest) {
    struct TValue: TThrRefBase {
        TValue(int v)
            : Value(v)
        {
        }

        int Value;
    };

    using TValuePtr = TIntrusivePtr<TValue>;

    TConcurrentHashMap<int, TValuePtr> h;

    TValuePtr v = MakeIntrusive<TValue>(123);
    h.Exchange(1, v);
    EXPECT_EQ(v, nullptr);

    v = MakeIntrusive<TValue>(456);
    h.Exchange(1, v);
    EXPECT_EQ(v->RefCount(), 1);
    EXPECT_EQ(v->Value, 123);
    EXPECT_EQ(h.Get(1)->Value, 456);
}

TEST(TConcurrentHashTest, TGetBucketTest) {
    TConcurrentHashMap<TString, ui32> h;

    for (int i = 0; i < 100; ++i) {
        TString key = ToString(i);
        auto& bucket1 = h.GetBucketForKey(key);
        auto& bucket2 = h.GetBucketForKey(TStringBuf(key));
        EXPECT_EQ(&bucket1, &bucket2);
    }
}
