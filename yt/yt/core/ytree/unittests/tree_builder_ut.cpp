#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/yson_consumer_mock.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/tree_visitor.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;

using ::testing::Sequence;
using ::testing::InSequence;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TTreeBuilderTest: public ::testing::Test
{
public:
    StrictMock<TMockYsonConsumer> Mock;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTreeBuilderTest, EmptyMap)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnEndMap());

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();
    builder->OnEndMap();
    auto root = builder->EndTree();

    VisitTree(root, &Mock, false);
}

TEST_F(TTreeBuilderTest, NestedMaps)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("a"));
        EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("b"));
            EXPECT_CALL(Mock, OnBeginMap());
            EXPECT_CALL(Mock, OnKeyedItem("c"));
            EXPECT_CALL(Mock, OnInt64Scalar(42));
            EXPECT_CALL(Mock, OnEndMap());
        EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnEndMap());

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
    builder->OnBeginMap();
    builder->OnKeyedItem("a");
        builder->OnBeginMap();
        builder->OnKeyedItem("b");
            builder->OnBeginMap();
            builder->OnKeyedItem("c");
            builder->OnInt64Scalar(42);
            builder->OnEndMap();
        builder->OnEndMap();
    builder->OnEndMap();
    auto root = builder->EndTree();

    VisitTree(root, &Mock, false);
}

TEST_F(TTreeBuilderTest, MapWithAttributes)
{
    // These are partial order chains.
    // If you feel confused a pen and a paper will help you and save your brain from exploding.
    // Have fun!
    Sequence s1, s2, s3, s4, s5, s6;

    EXPECT_CALL(Mock, OnBeginAttributes()).InSequence(s1, s2, s3, s4);
        EXPECT_CALL(Mock, OnKeyedItem("acl")).InSequence(s1, s2);
        EXPECT_CALL(Mock, OnBeginMap()).InSequence(s1, s2);
            EXPECT_CALL(Mock, OnKeyedItem("read")).InSequence(s1);
            EXPECT_CALL(Mock, OnBeginList()).InSequence(s1);
                EXPECT_CALL(Mock, OnListItem()).InSequence(s1);
                EXPECT_CALL(Mock, OnStringScalar("*")).InSequence(s1);
            EXPECT_CALL(Mock, OnEndList()).InSequence(s1);

            EXPECT_CALL(Mock, OnKeyedItem("write")).InSequence(s2);
            EXPECT_CALL(Mock, OnBeginList()).InSequence(s2);
                EXPECT_CALL(Mock, OnListItem()).InSequence(s2);
                EXPECT_CALL(Mock, OnStringScalar("sandello")).InSequence(s2);
            EXPECT_CALL(Mock, OnEndList()).InSequence(s2);
        EXPECT_CALL(Mock, OnEndMap()).InSequence(s1, s2);

        EXPECT_CALL(Mock, OnKeyedItem("lock_scope")).InSequence(s3);
        EXPECT_CALL(Mock, OnStringScalar("mytables")).InSequence(s3);
    EXPECT_CALL(Mock, OnEndAttributes()).InSequence(s1, s2, s3, s4);

    EXPECT_CALL(Mock, OnBeginMap()).InSequence(s4, s5, s6);
        EXPECT_CALL(Mock, OnKeyedItem("mode")).InSequence(s5);
        EXPECT_CALL(Mock, OnInt64Scalar(755)).InSequence(s5);

        EXPECT_CALL(Mock, OnKeyedItem("path")).InSequence(s6);
        EXPECT_CALL(Mock, OnStringScalar("/home/sandello")).InSequence(s6);
    EXPECT_CALL(Mock, OnEndMap()).InSequence(s4, s5, s6);

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();

    builder->OnBeginAttributes();
        builder->OnKeyedItem("acl");
        builder->OnBeginMap();
            builder->OnKeyedItem("read");
            builder->OnBeginList();
                builder->OnListItem();
                builder->OnStringScalar("*");
            builder->OnEndList();

            builder->OnKeyedItem("write");
            builder->OnBeginList();
                builder->OnListItem();
                builder->OnStringScalar("sandello");
            builder->OnEndList();
        builder->OnEndMap();

        builder->OnKeyedItem("lock_scope");
        builder->OnStringScalar("mytables");
    builder->OnEndAttributes();

    builder->OnBeginMap();
        builder->OnKeyedItem("path");
        builder->OnStringScalar("/home/sandello");

        builder->OnKeyedItem("mode");
        builder->OnInt64Scalar(755);
    builder->OnEndMap();

    auto root = builder->EndTree();

    VisitTree(root, &Mock, false);
}

TEST_F(TTreeBuilderTest, SkipEntityMapChildren)
{
    InSequence dummy;
    EXPECT_CALL(Mock, OnBeginMap());
    EXPECT_CALL(Mock, OnKeyedItem("b"));
    EXPECT_CALL(Mock, OnInt64Scalar(42));
    EXPECT_CALL(Mock, OnKeyedItem("c"));
        EXPECT_CALL(Mock, OnBeginAttributes());
            EXPECT_CALL(Mock, OnKeyedItem("d"));
            EXPECT_CALL(Mock, OnEntity());
        EXPECT_CALL(Mock, OnEndAttributes());
    EXPECT_CALL(Mock, OnInt64Scalar(42));
    EXPECT_CALL(Mock, OnEndMap());

    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());

    builder->BeginTree();
        builder->OnBeginMap();

        builder->OnKeyedItem("a");
        builder->OnEntity();

        builder->OnKeyedItem("b");
        builder->OnInt64Scalar(42);

        builder->OnKeyedItem("c");
        builder->OnBeginAttributes();
            builder->OnKeyedItem("d");
            builder->OnEntity();
        builder->OnEndAttributes();
        builder->OnInt64Scalar(42);

        builder->OnKeyedItem("f");
        builder->OnBeginAttributes();
            builder->OnKeyedItem("g");
            builder->OnInt64Scalar(42);
        builder->OnEndAttributes();
        builder->OnEntity();

        builder->OnEndMap();
    auto root = builder->EndTree();

    VisitTree(root, &Mock, false, TAttributeFilter(), true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
