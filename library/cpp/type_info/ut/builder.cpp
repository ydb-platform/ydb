#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include "utils.h"

class Builder: public NTesting::TTest {
public:
    void SetUp() override {
        F = NTi::PoolFactory(false);
    }

    void TearDown() override {
        F.Reset();
    }

    NTi::IPoolTypeFactoryPtr F;
};

TEST_F(Builder, TaggedBuilder) {
    auto builder = NTi::TTaggedBuilderRaw(*F);

    ASSERT_FALSE(builder.CanBuild());
    ASSERT_FALSE(builder.HasTag());
    ASSERT_FALSE(builder.HasItem());

    UNIT_ASSERT_EQUAL(builder.GetTag(), Nothing());
    UNIT_ASSERT_EQUAL(builder.GetItem(), Nothing());

    {
        auto tag = TString("Url");
        builder.SetTag(tag);
    }

    ASSERT_FALSE(builder.CanBuild());
    ASSERT_TRUE(builder.HasTag());
    ASSERT_FALSE(builder.HasItem());

    UNIT_ASSERT_EQUAL(builder.GetTag(), MakeMaybe<TStringBuf>("Url"));
    UNIT_ASSERT_EQUAL(builder.GetItem(), Nothing());

    {
        auto ty = NTi::String();
        builder.SetItem(ty);
    }

    ASSERT_TRUE(builder.CanBuild());
    ASSERT_TRUE(builder.HasTag());
    ASSERT_TRUE(builder.HasItem());

    UNIT_ASSERT_EQUAL(builder.GetTag(), MakeMaybe<TStringBuf>("Url"));
    UNIT_ASSERT_EQUAL(builder.GetItem(), MakeMaybe<const NTi::TType*>(NTi::TStringType::InstanceRaw()));

    {
        auto tagged = builder.Build();
        ASSERT_STRICT_EQ(tagged, NTi::Tagged(NTi::String(), "Url"));
    }

    ASSERT_TRUE(builder.CanBuild());

    builder.Reset();

    ASSERT_FALSE(builder.CanBuild());
    ASSERT_FALSE(builder.HasTag());
    ASSERT_FALSE(builder.HasItem());

    UNIT_ASSERT_EQUAL(builder.GetTag(), Nothing());
    UNIT_ASSERT_EQUAL(builder.GetItem(), Nothing());

    builder.SetTag("T");
    builder.SetItem(NTi::String());

    ASSERT_TRUE(builder.CanBuild());
    ASSERT_TRUE(builder.HasTag());
    ASSERT_TRUE(builder.HasItem());

    UNIT_ASSERT_EQUAL(builder.GetTag(), MakeMaybe<TStringBuf>("T"));
    UNIT_ASSERT_EQUAL(builder.GetItem(), MakeMaybe<const NTi::TType*>(NTi::TStringType::InstanceRaw()));

    builder.DiscardItem();

    ASSERT_FALSE(builder.CanBuild());
    ASSERT_TRUE(builder.HasTag());
    ASSERT_FALSE(builder.HasItem());

    UNIT_ASSERT_EQUAL(builder.GetTag(), MakeMaybe<TStringBuf>("T"));
    UNIT_ASSERT_EQUAL(builder.GetItem(), Nothing());

    builder.DiscardTag();

    builder.SetTag("T");
    builder.SetItem(NTi::String());

    ASSERT_TRUE(builder.CanBuild());
    ASSERT_TRUE(builder.HasTag());
    ASSERT_TRUE(builder.HasItem());
}

TEST_F(Builder, TaggedBuilderChaining) {
    auto builder = NTi::TTaggedBuilderRaw(*F)
                       .SetTag("Uuu")
                       .SetItem(NTi::Optional(NTi::String()));

    ASSERT_STRICT_EQ(builder.Build(), NTi::Tagged(NTi::Optional(NTi::String()), "Uuu"));

    builder = std::move(builder)
                  .DiscardTag()
                  .SetTag("Urls");

    ASSERT_STRICT_EQ(builder.Build(), NTi::Tagged(NTi::Optional(NTi::String()), "Urls"));

    builder = std::move(builder)
                  .DiscardItem()
                  .SetItem(F->ListRaw(F->StringRaw()));

    ASSERT_STRICT_EQ(builder.Build(), NTi::Tagged(NTi::List(NTi::String()), "Urls"));

    auto type = std::move(builder)
                    .Reset()
                    .SetTag("Url")
                    .SetItem(F->StringRaw())
                    .Build();

    ASSERT_STRICT_EQ(type, NTi::Tagged(NTi::String(), "Url"));
}
