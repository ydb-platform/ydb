#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/forwarding_consumer.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/yson/yson_builder.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NYson {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStringBuilderTest, Simple)
{
    TYsonStringBuilder builder;
    builder->OnStringScalar("some_scalar");
    ASSERT_EQ(builder.Flush().ToString(), TString{"\1\x16some_scalar"});
    ASSERT_TRUE(builder.IsEmpty());
}

TEST(TYsonStringBuilderTest, Reusing)
{
    TYsonStringBuilder builder;
    builder->OnStringScalar("some_scalar1");
    ASSERT_EQ(builder.Flush().ToString(), TString{"\1\x18some_scalar1"});
    ASSERT_TRUE(builder.IsEmpty());

    builder->OnStringScalar("some_scalar2");
    ASSERT_EQ(builder.Flush().ToString(), TString{"\1\x18some_scalar2"});
    ASSERT_TRUE(builder.IsEmpty());
}

TEST(TYsonStringBuilderTest, Checkpoints)
{
    TYsonStringBuilder builder;
    builder->OnStringScalar("some_scalar");

    auto checkpoint = builder.CreateCheckpoint();
    builder.CreateCheckpoint();
    builder.RestoreCheckpoint(checkpoint);
    builder.RestoreCheckpoint(checkpoint);

    ASSERT_EQ(builder.Flush().ToString(), TString{"\1\x16some_scalar"});
    ASSERT_TRUE(builder.IsEmpty());
}

TEST(TYsonStringBuilderTest, MapCheckpoints)
{
    TYsonStringBuilder builder(EYsonFormat::Text);

    builder->OnBeginMap();

    builder->OnKeyedItem("key1");
    builder->OnEntity();

    auto checkpoint = builder.CreateCheckpoint();
    builder->OnKeyedItem("key2");
    builder->OnEntity();
    builder.RestoreCheckpoint(checkpoint);

    builder->OnEndMap();

    ASSERT_EQ(builder.Flush().ToString(), TString{R"({"key1"=#;})"});
}

////////////////////////////////////////////////////////////////////////////////

struct TDirectForwardingYsonConsumer
    : public TForwardingYsonConsumer
{
    TDirectForwardingYsonConsumer(IYsonConsumer* consumer)
    {
        Forward(consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonBuilderTest, Forwarding)
{
    TYsonStringBuilder stringBuilder(EYsonFormat::Text);
    TYsonBuilder<TDirectForwardingYsonConsumer> builder(stringBuilder);

    builder->OnBeginMap();
    auto checkpoint = builder.CreateCheckpoint();
    builder->OnKeyedItem("key");
    builder->OnEntity();
    builder.RestoreCheckpoint(checkpoint);
    builder->OnEndMap();

    ASSERT_EQ(stringBuilder.Flush().ToString(), TString{R"({})"});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
