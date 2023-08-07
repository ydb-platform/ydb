#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

TEST(TypeFactory, AdoptPoolToPool) {
    auto f1 = NTi::PoolFactory();
    auto f2 = NTi::PoolFactory();

    auto t = f1->Optional(f1->List(f1->String()));
    auto ta = f2->Adopt(t);

    ASSERT_NE(t.Get(), ta.Get());
    ASSERT_NE(t->GetItemTypeRaw(), ta->GetItemTypeRaw());
    ASSERT_EQ(t->GetItemTypeRaw()->AsListRaw()->GetItemTypeRaw(), ta->GetItemTypeRaw()->AsListRaw()->GetItemTypeRaw());

    f1 = nullptr;
    f2 = nullptr;
    t = nullptr;

    // `ta` is still alive

    ASSERT_TRUE(ta->IsOptional());
    ASSERT_TRUE(ta->GetItemTypeRaw()->IsList());
}

TEST(TypeFactory, AdoptPoolToSamePool) {
    auto f = NTi::PoolFactory();

    auto t = f->Optional(f->List(f->String()));
    auto ta = f->Adopt(t);

    ASSERT_EQ(t.Get(), ta.Get());
}

TEST(TypeFactory, AdoptHeapToHeap) {
    auto f = NTi::HeapFactory();

    auto t = f->Optional(f->List(f->String()));
    auto ta = f->Adopt(t);

    ASSERT_EQ(t.Get(), ta.Get());
}

TEST(TypeFactory, AdoptHeapToPool) {
    auto f1 = NTi::HeapFactory();
    auto f2 = NTi::PoolFactory();

    auto t = f1->Optional(f1->List(f1->String()));
    auto ta = f2->Adopt(t);

    ASSERT_NE(t.Get(), ta.Get());
    ASSERT_NE(t->GetItemTypeRaw(), ta->GetItemTypeRaw());
    ASSERT_EQ(t->GetItemTypeRaw()->AsListRaw()->GetItemTypeRaw(), ta->GetItemTypeRaw()->AsListRaw()->GetItemTypeRaw());

    f1 = nullptr;
    f2 = nullptr;
    t = nullptr;

    // `ta` is still alive

    ASSERT_TRUE(ta->IsOptional());
    ASSERT_TRUE(ta->GetItemTypeRaw()->IsList());
}

TEST(TypeFactory, AdoptPoolToHeap) {
    auto f1 = NTi::PoolFactory();
    auto f2 = NTi::HeapFactory();

    auto t = f1->Optional(f1->List(f1->String()));
    auto ta = f2->Adopt(t);

    ASSERT_NE(t.Get(), ta.Get());
    ASSERT_NE(t->GetItemTypeRaw(), ta->GetItemTypeRaw());
    ASSERT_EQ(t->GetItemTypeRaw()->AsListRaw()->GetItemTypeRaw(), ta->GetItemTypeRaw()->AsListRaw()->GetItemTypeRaw());

    f1 = nullptr;
    f2 = nullptr;
    t = nullptr;

    // `ta` is still alive

    ASSERT_TRUE(ta->IsOptional());
    ASSERT_TRUE(ta->GetItemTypeRaw()->IsList());
}

TEST(TypeFactory, AdoptStaticToPool) {
    auto f = NTi::PoolFactory();

    auto t = NTi::Void();
    auto ta = f->Adopt(t);

    ASSERT_EQ(t.Get(), ta.Get());
}

TEST(TypeFactory, AdoptStaticToHeap) {
    auto f = NTi::HeapFactory();

    auto t = NTi::Void();
    auto ta = f->Adopt(t);
    ASSERT_EQ(t.Get(), ta.Get());
}

TEST(TypeFactory, Dedup) {
    {
        auto f = NTi::PoolFactory(/* deduplicate = */ false);

        auto a = f->OptionalRaw(f->StringRaw());
        auto b = f->OptionalRaw(f->StringRaw());

        ASSERT_NE(a, b);
    }

    {
        auto f = NTi::PoolFactory(/* deduplicate = */ true);

        auto a = f->OptionalRaw(f->StringRaw());
        auto b = f->OptionalRaw(f->StringRaw());

        ASSERT_EQ(a, b);
    }
}
