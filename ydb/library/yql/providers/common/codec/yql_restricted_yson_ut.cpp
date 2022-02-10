#include "yql_restricted_yson.h"

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

namespace {
TString FormatNode(const NYT::TNode& node) {
    TStringStream stream;
    NYson::TYsonWriter writer(&stream, NYson::EYsonFormat::Text);
    const auto sortMapKeys = true;
    NYT::TNodeVisitor visitor(&writer, sortMapKeys);
    visitor.Visit(node);
    return stream.Str();
}

// reformat yson and sort keys
TString Normalize(const TString& yson) {
    return FormatNode(NYT::NodeFromYsonString(yson));
}

}

Y_UNIT_TEST_SUITE(TRestrictedYson) {
    void RunTest(const NYT::TNode& node, const TString& expectedNodeStr, const TString& expectedEncodedStr) {
        UNIT_ASSERT_VALUES_EQUAL(FormatNode(node), expectedNodeStr);
        TString encoded = NCommon::EncodeRestrictedYson(node, NYson::EYsonFormat::Text);
        UNIT_ASSERT_VALUES_EQUAL(Normalize(encoded), expectedEncodedStr);
        TString decoded = NCommon::DecodeRestrictedYson(TStringBuf(encoded), NYson::EYsonFormat::Text);
        UNIT_ASSERT_VALUES_EQUAL(FormatNode(node), Normalize(decoded));
    }

    Y_UNIT_TEST(MapScalars) {
        NYT::TNode node = NYT::TNode::CreateMap();
        node["a"] = NYT::TNode("abc");
        node["b"] = NYT::TNode::CreateEntity();
        node["c"] = NYT::TNode(true);
        node["d"] = NYT::TNode(1);
        node["e"] = NYT::TNode(1u);
        node["f"] = NYT::TNode(1.25);

        RunTest(node,
            R"({"a"="abc";"b"=#;"c"=%true;"d"=1;"e"=1u;"f"=1.25})",
            R"({"a"={"$type"="string";"$value"="abc"};"b"=#;"c"={"$type"="boolean";"$value"="true"};"d"={"$type"="int64";"$value"="1"};"e"={"$type"="uint64";"$value"="1"};"f"={"$type"="double";"$value"="1.25"}})"
        );
    }

    Y_UNIT_TEST(ScalarWithAttributes) {
        NYT::TNode node("abc");
        node.Attributes()["d"] = NYT::TNode(true);

        RunTest(node,
            R"(<"d"=%true>"abc")",
            R"({"$attributes"={"d"={"$type"="boolean";"$value"="true"}};"$type"="string";"$value"="abc"})"
        );
    }

    Y_UNIT_TEST(MapWithAttributes) {
        NYT::TNode node = NYT::TNode::CreateMap();
        node["b"] = NYT::TNode::CreateEntity();
        node["c"] = NYT::TNode(false);
        node.Attributes()["d"] = NYT::TNode(true);

        RunTest(node,
            R"(<"d"=%true>{"b"=#;"c"=%false})",
            R"({"$attributes"={"d"={"$type"="boolean";"$value"="true"}};"$value"={"b"=#;"c"={"$type"="boolean";"$value"="false"}}})"
        );
    }

    Y_UNIT_TEST(ListWithAttributes) {
        NYT::TNode node = NYT::TNode::CreateList();
        node.Add(NYT::TNode::CreateEntity());
        node.Add(NYT::TNode(false));
        node.Attributes()["d"] = NYT::TNode(true);

        RunTest(node,
            R"(<"d"=%true>[#;%false])",
            R"({"$attributes"={"d"={"$type"="boolean";"$value"="true"}};"$value"=[#;{"$type"="boolean";"$value"="false"}]})"
        );
    }

    Y_UNIT_TEST(EntityWithAttributes) {
        NYT::TNode node = NYT::TNode::CreateEntity();
        node.Attributes()["d"] = NYT::TNode(true);

        RunTest(node,
            R"(<"d"=%true>#)",
            R"({"$attributes"={"d"={"$type"="boolean";"$value"="true"}};"$value"=#})"
        );
    }
}
}
