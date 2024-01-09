#include "fyamlcpp.h"

#include <contrib/libs/libfyaml/include/libfyaml.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(FYamlCpp) {
    Y_UNIT_TEST(EnumEquals) {
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeType::Scalar, FYNT_SCALAR);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeType::Sequence, FYNT_SEQUENCE);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeType::Mapping, FYNT_MAPPING);

        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeStyle::Any, FYNS_ANY);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeStyle::Flow, FYNS_FLOW);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeStyle::Block, FYNS_BLOCK);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeStyle::Plain, FYNS_PLAIN);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeStyle::SingleQuoted, FYNS_SINGLE_QUOTED);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeStyle::DoubleQuoted, FYNS_DOUBLE_QUOTED);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeStyle::Literal, FYNS_LITERAL);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeStyle::Folded, FYNS_FOLDED);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeStyle::Alias, FYNS_ALIAS);
    }

    Y_UNIT_TEST(ErrorHandling) {
        {
            const char *yaml = R"(
config: a
config: b
)";
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                NFyaml::TDocument::Parse(yaml),
                NFyaml::TFyamlEx,
                "3:1 duplicate key");
        }

        {
            const char *yaml = R"(
anchor: *does_not_exists
)";
            auto doc = NFyaml::TDocument::Parse(yaml);
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                doc.Resolve(),
                NFyaml::TFyamlEx,
                "2:10 invalid alias");
        }
        {
            const char *yaml = R"(
a: 1
a: 2
a: 3
)";
            try {
                NFyaml::TDocument::Parse(yaml);
                UNIT_FAIL("exception must've happend");
            } catch (NFyaml::TFyamlEx e) {
                UNIT_ASSERT(TString(e.what()).Contains("3:1 duplicate key"));
                UNIT_ASSERT(e.Errors().ysize() == 1);
            }
        }
    }

    Y_UNIT_TEST(Out) {
        const char *yaml = R"(
test: output
)";

        auto doc = NFyaml::TDocument::Parse(yaml);

        TStringStream ss;

        ss << doc;

        UNIT_ASSERT_VALUES_EQUAL(ss.Str(), "test: output\n");
    }

    Y_UNIT_TEST(Parser) {
        const char *yaml = R"(
test: a
---
test: b
)";
        auto parser = NFyaml::TParser::Create(yaml);

        TStringStream ss;

        auto docOpt = parser.NextDocument();
        UNIT_ASSERT(docOpt);
        ss << *docOpt;
        UNIT_ASSERT_VALUES_EQUAL(ss.Str(), "test: a\n");
        auto beginMark = docOpt->BeginMark();
        UNIT_ASSERT_VALUES_EQUAL(beginMark.InputPos, 1);
        auto endMark = docOpt->EndMark();
        UNIT_ASSERT_VALUES_EQUAL(endMark.InputPos, 12);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(yaml).SubStr(beginMark.InputPos, endMark.InputPos - 4), ss.Str());

        ss.clear();

        auto docOpt2 = parser.NextDocument();
        UNIT_ASSERT(docOpt2);
        ss << *docOpt2;
        UNIT_ASSERT_VALUES_EQUAL(ss.Str(), "---\ntest: b\n");
        beginMark = docOpt2->BeginMark();
        UNIT_ASSERT_VALUES_EQUAL(beginMark.InputPos, 9);
        endMark = docOpt2->EndMark();
        UNIT_ASSERT_VALUES_EQUAL(endMark.InputPos, 21);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(yaml).SubStr(beginMark.InputPos, endMark.InputPos), ss.Str());

        auto docOpt3 = parser.NextDocument();
        UNIT_ASSERT(!docOpt3);
    }

    Y_UNIT_TEST(Leak) {
        std::optional<NFyaml::TDocument> doc;

        {
            std::optional<NFyaml::TDocument> item1;
            std::optional<NFyaml::TDocument> item2;
            {
                const TString items = R"(
item:
  x: 1
  y: 2
---
test:
  a: noop
  b:
  - 1
  - 2
  - 3
---
x: b
)";
                auto parser = NFyaml::TParser::Create(items);

                item1.emplace(*parser.NextDocument());
                item2.emplace(*parser.NextDocument());
                parser.NextDocument();
                parser.NextDocument();
            }

            {
                const TString config = R"(
test: a
---
test: []
---
x: b
)";
                auto parser = NFyaml::TParser::Create(config);

                parser.NextDocument();
                doc.emplace(*parser.NextDocument());
                parser.NextDocument();
                parser.NextDocument();
            }

            {
                auto item1NodeRef = item1->Root().Map().at("item");
                auto item2NodeRef = item2->Root().Map().at("test");
                auto docNodeRef = doc->Root().Map().at("test");
                auto node1 = item1NodeRef.Copy(*doc);
                auto node2 = item2NodeRef.Copy(*doc);
                docNodeRef.Sequence().Append(node1);
                docNodeRef.Sequence().Append(node2);
                item1.reset();
                item2.reset();
            }
        }

        auto seq = doc->Root().Map().at("test").Sequence();
        UNIT_ASSERT(seq[0].Map().Has("x"));
        UNIT_ASSERT(!seq[0].Map().Has("xx"));
        UNIT_ASSERT_VALUES_EQUAL(seq[0].Map().at("x").Scalar(), "1");
        UNIT_ASSERT_VALUES_EQUAL(seq[0].Map().at("y").Scalar(), "2");
        UNIT_ASSERT_VALUES_EQUAL(seq[1].Map().at("a").Scalar(), "noop");
        UNIT_ASSERT_VALUES_EQUAL(seq[1].Map().at("b").Sequence().at(0).Scalar(), "1");
        UNIT_ASSERT_VALUES_EQUAL(seq[1].Map().at("b").Sequence().at(1).Scalar(), "2");
        UNIT_ASSERT_VALUES_EQUAL(seq[1].Map().at("b").Sequence().at(2).Scalar(), "3");
    }

    Y_UNIT_TEST(SimpleScalarMark) {
        auto check = [](const TString& str, const NFyaml::TNodeRef& node) {
            auto pos = str.find("123");
            auto endPos = pos + strlen("123");
            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;
            UNIT_ASSERT_VALUES_EQUAL(begin, pos);
            UNIT_ASSERT_VALUES_EQUAL(end, endPos);
        };

        {
            TString str = R"(obj: 123)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(obj: 123 # test)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
# test
obj: 123 # test
# test
)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
---
obj: 123
...
)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
a: foo
test: [{obj: 123}]
b: bar
            )";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("test").Sequence().at(0).Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(obj: '123')";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(obj: '123' # test)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
# test
obj: '123' # test
# test
)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
---
obj: '123'
...
)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
a: foo
test: [{obj: "123"}]
b: bar
            )";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("test").Sequence().at(0).Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(obj: "123")";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(obj: "123" # test)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
# test
obj: "123" # test
# test
)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
---
obj: "123"
...
)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
a: foo
test: [{obj: "123"}]
b: bar
            )";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("test").Sequence().at(0).Map().at("obj");
            check(str, node);
        }

        {
            TString str = R"(
a: foo
test: [{obj: !!int "123"}]
b: bar
            )";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("test").Sequence().at(0).Map().at("obj");
            check(str, node);
        }

    }

    Y_UNIT_TEST(MultilineScalarMark) {
        {
            TString str = R"(obj: >+2
  some
  multiline

  scalar with couple words)";
            auto doc = NFyaml::TDocument::Parse(str);
            auto node = doc.Root().Map().at("obj");
            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;
            UNIT_ASSERT_VALUES_EQUAL(begin, 9);
            UNIT_ASSERT_VALUES_EQUAL(end, 55);
        }
    }

    Y_UNIT_TEST(MapMark) {
        {
            TString str = R"(
a: foo
map: !!map
  internal_map1: {}
  internal_map2:
    internal_map3:
      internal_map4:
        value: 1
      internal_map5: {
        internal_map6: {test1: 1, test2: 2},
        internal_map7: {
          value: 1
          }
        }
# comment
c: bar
            )";

            auto doc = NFyaml::TDocument::Parse(str);

            auto node = doc.Root().Map().at("map");

            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;

            UNIT_ASSERT_VALUES_EQUAL(begin, 21);
            UNIT_ASSERT_VALUES_EQUAL(end, 246);
        }

        {
            TString str = R"(
a: foo
map: !!map # comment
# comment
c: bar
            )";

            auto doc = NFyaml::TDocument::Parse(str);

            auto node = doc.Root().Map().at("map");

            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;

            UNIT_ASSERT_VALUES_EQUAL(begin, 11);
            UNIT_ASSERT_VALUES_EQUAL(end, 11);
        }

        {
            TString str = R"(
a: foo
map: {} # comment
# comment
c: bar
            )";

            auto doc = NFyaml::TDocument::Parse(str);

            auto node = doc.Root().Map().at("map");

            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;

            UNIT_ASSERT_VALUES_EQUAL(begin, 13);
            UNIT_ASSERT_VALUES_EQUAL(end, 15);
        }

        {
            TString str = R"(
a: foo
map:
  value: 1
# comment
c: bar
            )";

            auto doc = NFyaml::TDocument::Parse(str);

            auto node = doc.Root().Map().at("map");

            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;

            UNIT_ASSERT_VALUES_EQUAL(begin, 15);
            UNIT_ASSERT_VALUES_EQUAL(end, 23);
        }
    }

    Y_UNIT_TEST(SequenceMark) {
        {
            TString str = R"(
a: foo
seq: !!seq
- internal_map1: {}
- internal_seq2:
  - internal_seq3:
    - internal_seq4:
        value: 1
    - internal_seq5: [
        internal_seq6: [{test1: 1}, {test2: 2}],
        internal_seq7: [
          {value: 1}
          ]
        ]
# comment
c: bar
            )";

            auto doc = NFyaml::TDocument::Parse(str);

            auto node = doc.Root().Map().at("seq");

            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;

            UNIT_ASSERT_VALUES_EQUAL(begin, 19);
            UNIT_ASSERT_VALUES_EQUAL(end, 252);
        }

        {
            TString str = R"(
a: foo
seq: !!seq # comment
# comment
c: bar
            )";

            auto doc = NFyaml::TDocument::Parse(str);

            auto node = doc.Root().Map().at("seq");

            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;

            UNIT_ASSERT_VALUES_EQUAL(begin, 11);
            UNIT_ASSERT_VALUES_EQUAL(end, 11);
        }

        {
            TString str = R"(
a: foo
seq: [] # comment
# comment
c: bar
            )";

            auto doc = NFyaml::TDocument::Parse(str);

            auto node = doc.Root().Map().at("seq");

            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;

            UNIT_ASSERT_VALUES_EQUAL(begin, 13);
            UNIT_ASSERT_VALUES_EQUAL(end, 15);
        }

        {
            TString str = R"(
a: foo
seq:
- value: 1
# comment
c: bar
            )";

            auto doc = NFyaml::TDocument::Parse(str);

            auto node = doc.Root().Map().at("seq");

            auto begin = node.BeginMark().InputPos;
            auto end = node.EndMark().InputPos;

            UNIT_ASSERT_VALUES_EQUAL(begin, 13);
            UNIT_ASSERT_VALUES_EQUAL(end, 23);
        }
    }

}
