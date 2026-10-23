#include "yql_constraint.h"
#include "yql_expr.h"

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node_io.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TSerializeConstrains) {

Y_UNIT_TEST(SerializeSorted) {
    TExprContext ctx;
    auto c = ctx.MakeConstraint<TSortedConstraintNode>(TSortedConstraintNode::TContainerType{
        std::pair{TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"a", "b"}}, true},
        std::pair{TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"d"}}, false},
        std::pair{TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"e"}, TPartOfConstraintBase::TPathType{"f", "g"}}, false},
    });
    auto yson = c->ToYson();
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), R"([[[["a";"b"]];%true];[["d"];%false];[["e";["f";"g"]];%false]])");
    auto c2 = ctx.MakeConstraint<TSortedConstraintNode>(yson);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), NYT::NodeToCanonicalYsonString(c2->ToYson()));
    UNIT_ASSERT_EQUAL(c, c2);
}

Y_UNIT_TEST(SerializeChopped) {
    TExprContext ctx;
    auto c = ctx.MakeConstraint<TChoppedConstraintNode>(TPartOfConstraintBase::TSetOfSetsType{
        TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"a"}},
        TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"a", "b"}},
        TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"c", "d"}, TPartOfConstraintBase::TPathType{"e"}},
    });
    auto yson = c->ToYson();
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), R"(["a";[["a";"b"]];[["c";"d"];"e"]])");
    auto c2 = ctx.MakeConstraint<TChoppedConstraintNode>(yson);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), NYT::NodeToCanonicalYsonString(c2->ToYson()));
    UNIT_ASSERT_EQUAL(c, c2);
}

template <class TUniqueConstraint>
void CheckSerializeUnique() {
    TExprContext ctx;
    auto c = ctx.MakeConstraint<TUniqueConstraint>(typename TUniqueConstraint::TContentType{
        TConstraintWithFieldsNode::TSetOfSetsType{
            TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"a"}},
            TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"a", "b"}}},
        TConstraintWithFieldsNode::TSetOfSetsType{
            TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"c"}},
            TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"d"}, TPartOfConstraintBase::TPathType{"e"}}},
    });
    auto yson = c->ToYson();
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), R"([["a";[["a";"b"]]];["c";["d";"e"]]])");
    auto c2 = ctx.MakeConstraint<TUniqueConstraint>(yson);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), NYT::NodeToCanonicalYsonString(c2->ToYson()));
    UNIT_ASSERT_EQUAL(c, c2);
}

Y_UNIT_TEST(SerializeUnique) {
    CheckSerializeUnique<TUniqueConstraintNode>();
}

Y_UNIT_TEST(SerializeDistint) {
    CheckSerializeUnique<TDistinctConstraintNode>();
}

Y_UNIT_TEST(SerializeEmpty) {
    TExprContext ctx;
    auto c = ctx.MakeConstraint<TEmptyConstraintNode>();
    auto yson = c->ToYson();
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), R"(#)");
    auto c2 = ctx.MakeConstraint<TEmptyConstraintNode>(yson);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), NYT::NodeToCanonicalYsonString(c2->ToYson()));
    UNIT_ASSERT_EQUAL(c, c2);
}

Y_UNIT_TEST(SerializeVarIndex) {
    TExprContext ctx;
    auto c = ctx.MakeConstraint<TVarIndexConstraintNode>(TVarIndexConstraintNode::TMapType{
        std::pair{1U, 3U},
        std::pair{0U, 1U},
    });
    auto yson = c->ToYson();
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), R"([[0u;1u];[1u;3u]])");
    auto c2 = ctx.MakeConstraint<TVarIndexConstraintNode>(yson);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), NYT::NodeToCanonicalYsonString(c2->ToYson()));
    UNIT_ASSERT_EQUAL(c, c2);
}

Y_UNIT_TEST(SerializeMulti) {
    TExprContext ctx;

    TConstraintSet s1;
    s1.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
    s1.AddConstraint(
        ctx.MakeConstraint<TSortedConstraintNode>(TSortedConstraintNode::TContainerType{
            std::pair{TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"a"}}, true},
            std::pair{TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"b"}}, false},
        }));

    TConstraintSet s2;
    s2.AddConstraint(
        ctx.MakeConstraint<TUniqueConstraintNode>(typename TUniqueConstraintNode::TContentType{
            TConstraintWithFieldsNode::TSetOfSetsType{
                TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"a"}},
                TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"b"}}},
            TConstraintWithFieldsNode::TSetOfSetsType{
                TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"c"}},
                TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"d"}, TPartOfConstraintBase::TPathType{"e"}}},
        }));
    s2.AddConstraint(
        ctx.MakeConstraint<TVarIndexConstraintNode>(TVarIndexConstraintNode::TMapType{
            std::pair{0U, 1U},
            std::pair{1U, 2U},
        }));

    auto c = ctx.MakeConstraint<TMultiConstraintNode>(TMultiConstraintNode::TMapType{
        std::pair{0U, s1},
        std::pair{1U, s2},
    });
    auto yson = c->ToYson();
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), R"([[0u;{"Empty"=#;"Sorted"=[[["a"];%true];[["b"];%false]]}];[1u;{"Unique"=[["a";"b"];["c";["d";"e"]]];"VarIndex"=[[0u;1u];[1u;2u]]}]])");
    auto c2 = ctx.MakeConstraint<TMultiConstraintNode>(yson);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), NYT::NodeToCanonicalYsonString(c2->ToYson()));
    UNIT_ASSERT_EQUAL(c, c2);
}

Y_UNIT_TEST(SerializeConstrainSet) {
    TExprContext ctx;

    TConstraintSet s;
    s.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
    s.AddConstraint(
        ctx.MakeConstraint<TSortedConstraintNode>(TSortedConstraintNode::TContainerType{
            std::pair{TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"a"}}, true},
            std::pair{TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"b"}}, false},
        }));
    s.AddConstraint(
        ctx.MakeConstraint<TUniqueConstraintNode>(typename TUniqueConstraintNode::TContentType{
            TConstraintWithFieldsNode::TSetOfSetsType{
                TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"a"}},
                TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"b"}}},
            TConstraintWithFieldsNode::TSetOfSetsType{
                TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"c"}},
                TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{"d"}, TPartOfConstraintBase::TPathType{"e"}}},
        }));
    s.AddConstraint(
        ctx.MakeConstraint<TVarIndexConstraintNode>(TVarIndexConstraintNode::TMapType{
            std::pair{0U, 1U},
            std::pair{1U, 2U},
        }));
    s.AddConstraint(
        ctx.MakeConstraint<TVarIndexConstraintNode>(TVarIndexConstraintNode::TMapType{
            std::pair{0U, 1U},
            std::pair{1U, 2U},
        }));

    TConstraintSet inner;
    inner.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
    s.AddConstraint(
        ctx.MakeConstraint<TMultiConstraintNode>(TMultiConstraintNode::TMapType{
            std::pair{0U, inner},
        }));

    auto yson = s.ToYson();
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), R"({"Empty"=#;"Multi"=[[0u;{"Empty"=#}]];"Sorted"=[[["a"];%true];[["b"];%false]];"Unique"=[["a";"b"];["c";["d";"e"]]];"VarIndex"=[[0u;1u];[1u;2u]]})");
    auto s2 = ctx.MakeConstraintSet(yson);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), NYT::NodeToCanonicalYsonString(s2.ToYson()));
    UNIT_ASSERT_EQUAL(s, s2);

    s.Clear();
    yson = s.ToYson();
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), R"({})");
    auto s3 = ctx.MakeConstraintSet(yson);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(yson), NYT::NodeToCanonicalYsonString(s3.ToYson()));
    UNIT_ASSERT_EQUAL(s, s3);
}

Y_UNIT_TEST(StreamingAliasesSerialization) {
    using TPath = TPartOfConstraintBase::TPathType;
    using TPaths = TPartOfConstraintBase::TSetType;
    TExprContext ctx;
    TConstraintSet constraints;
    const auto streaming = ctx.MakeConstraint<TStreamingConstraintNode>(TPath{"t"}, TPaths{TPath{"t"}, TPath{"event_time"}});
    constraints.AddConstraint(streaming);
    const auto serialized = constraints.ToYson();
    UNIT_ASSERT_EQUAL(ctx.MakeConstraintSet(serialized), constraints);

    TExprContext otherCtx;
    const auto restored = otherCtx.MakeConstraintSet(serialized).GetConstraint<TStreamingConstraintNode>();
    UNIT_ASSERT(restored);
    UNIT_ASSERT(restored->Equals(*streaming));
    UNIT_ASSERT(restored->IsEventTimePath(TPath{"t"}));
    UNIT_ASSERT(restored->IsEventTimePath(TPath{"event_time"}));
    UNIT_ASSERT(!restored->IsEventTimePath(TPath{"other"}));
    UNIT_ASSERT(restored->GetEventTime() == streaming->GetEventTime());

    for (const TStringBuf yson : {"#", "\"t\"", R"(["nested";"t"])", "[]"}) {
        const auto node = NYT::NodeFromYsonString(yson);
        const auto legacy = ctx.MakeConstraint<TStreamingConstraintNode>(node);
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(legacy->ToYson()), NYT::NodeToCanonicalYsonString(node));
    }
}

Y_UNIT_TEST(StreamingAliasesFilterRenameCommon) {
    using TPath = TPartOfConstraintBase::TPathType;
    using TPaths = TPartOfConstraintBase::TSetType;
    TExprContext ctx;
    const auto streaming = ctx.MakeConstraint<TStreamingConstraintNode>(TPath{"t"}, TPaths{TPath{"t"}, TPath{"event_time"}});
    const auto single = ctx.MakeConstraint<TStreamingConstraintNode>(TPath{"event_time"});
    UNIT_ASSERT(streaming->Includes(*single));
    UNIT_ASSERT(!single->Includes(*streaming));
    UNIT_ASSERT(streaming != ctx.MakeConstraint<TStreamingConstraintNode>(TPath{"t"}));
    UNIT_ASSERT_EQUAL(streaming->MakeCommon(single, ctx), single);
    UNIT_ASSERT_EQUAL(single->MakeCommon(streaming, ctx), single);
    UNIT_ASSERT_EQUAL(streaming->FilterFields(ctx, [](const TPath& path) { return path == TPath{"event_time"}; }), single);
    const auto renamed = streaming->RenameFields(ctx, [](const TPath& path) {
        return std::vector<TPath>{path == TPath{"t"} ? TPath{"renamed"} : path};
    });
    UNIT_ASSERT(renamed->GetEventTime() == TMaybe<TPath>(TPath{"renamed"}));
    UNIT_ASSERT(renamed->IsEventTimePath(TPath{"event_time"}));
    UNIT_ASSERT(!renamed->IsEventTimePath(TPath{"t"}));

    TPaths references{TPath{"event_time"}, TPath{"other"}};
    streaming->FilterUncompleteReferences(references);
    UNIT_ASSERT_EQUAL(references, TPaths{TPath{"event_time"}});
    const auto unrelated = ctx.MakeConstraint<TStreamingConstraintNode>(TPath{"other"});
    UNIT_ASSERT_EQUAL(streaming->MakeCommon(unrelated, ctx), ctx.MakeConstraint<TStreamingConstraintNode>());
}

Y_UNIT_TEST(StreamingAliasesRejectBadSerialization) {
    TExprContext ctx;
    for (const TStringBuf yson : {
             R"({"event_time"="t";"aliases"=["other"]})",
             R"({"event_time"="t";"aliases"=#})",
             R"({"event_time"="t"})",
         }) {
        UNIT_ASSERT_EXCEPTION(ctx.MakeConstraint<TStreamingConstraintNode>(NYT::NodeFromYsonString(yson)), NYql::TYqlPanic);
    }
}

Y_UNIT_TEST(DeserializeBadConstrainSet) {
    TExprContext ctx;
    UNIT_ASSERT_EXCEPTION(
        ctx.MakeConstraintSet(NYT::NodeFromYsonString(R"(#)")),
        NYql::TYqlPanic);
    UNIT_ASSERT_EXCEPTION(
        ctx.MakeConstraintSet(NYT::NodeFromYsonString(R"({"Unknown"=[]})")),
        NYql::TYqlPanic);
    UNIT_ASSERT_EXCEPTION(
        ctx.MakeConstraintSet(NYT::NodeFromYsonString(R"({"Empty"=1u})")),
        NYql::TYqlPanic);
}
} // Y_UNIT_TEST_SUITE(TSerializeConstrains)

} // namespace NYql
