#include "fyamlcpp.h"

#include <contrib/libs/libfyaml/include/libfyaml.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

Y_UNIT_TEST_SUITE(FYamlCpp) {
    Y_UNIT_TEST(EnumEquals) {
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeType::Scalar, FYNT_SCALAR);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeType::Sequence, FYNT_SEQUENCE);
        UNIT_ASSERT_EQUAL((int)NFyaml::ENodeType::Mapping, FYNT_MAPPING);
    }

    Y_UNIT_TEST(ErrorHandling) {
        {
            const char *yaml = R"(
config: a
config: b
)";
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                NFyaml::TDocument::Parse(yaml),
                yexception,
                "3:1 duplicate key");
        }

        {
            const char *yaml = R"(
anchor: *does_not_exists
)";
            auto doc = NFyaml::TDocument::Parse(yaml);
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                doc.Resolve(),
                yexception,
                "2:10 invalid alias");
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
                docNodeRef.Sequence().Append(node1.Ref());
                docNodeRef.Sequence().Append(node2.Ref());
                item1.reset();
                item2.reset();
            }
        }

        auto seq = doc->Root().Map().at("test").Sequence();
        UNIT_ASSERT_VALUES_EQUAL(seq[0].Map().at("x").Scalar(), "1");
        UNIT_ASSERT_VALUES_EQUAL(seq[0].Map().at("y").Scalar(), "2");
        UNIT_ASSERT_VALUES_EQUAL(seq[1].Map().at("a").Scalar(), "noop");
        UNIT_ASSERT_VALUES_EQUAL(seq[1].Map().at("b").Sequence().at(0).Scalar(), "1");
        UNIT_ASSERT_VALUES_EQUAL(seq[1].Map().at("b").Sequence().at(1).Scalar(), "2");
        UNIT_ASSERT_VALUES_EQUAL(seq[1].Map().at("b").Sequence().at(2).Scalar(), "3");
    }
}
