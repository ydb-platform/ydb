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
}
