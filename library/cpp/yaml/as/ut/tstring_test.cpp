#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yaml/as/tstring.h>

#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>

#include <array>

Y_UNIT_TEST_SUITE(YamlTstringTest) {
    Y_UNIT_TEST(As) {
        const auto* key1 = "key1";
        const auto* key2 = "key2";
        const TString string = "string";
        const TString anotherString = "another string";

        YAML::Node map;
        map[key1] = std::string(string);

        UNIT_ASSERT_VALUES_EQUAL(map[key1].as<TString>(), string);
        UNIT_ASSERT_VALUES_EQUAL(map[key1].as<TString>(anotherString), string);

        UNIT_ASSERT_VALUES_EQUAL(map[key2].as<TString>(anotherString), anotherString);

        UNIT_ASSERT_EXCEPTION(map[key1].as<int>(), YAML::BadConversion);

        UNIT_ASSERT_EXCEPTION(map[key2].as<int>(), YAML::BadConversion);
        UNIT_ASSERT_EXCEPTION(map[key2].as<TString>(), YAML::BadConversion);
    }

    Y_UNIT_TEST(Convert) {
        const TString string("string");
        auto node = YAML::Node(std::string(string));

        UNIT_ASSERT_VALUES_EQUAL(node.Scalar(), YAML::convert<TString>::encode(string).Scalar());

        TString buffer;
        YAML::convert<TString>::decode(node, buffer);
        UNIT_ASSERT_VALUES_EQUAL(string, buffer);
    }

    Y_UNIT_TEST(NullCharactersInString) {
        const std::array<char, 3> characters = {{'a', '\0', 'b'}};
        const std::string testString(characters.data(), characters.size());
        const TString refString(characters.data(), characters.size());
        const std::string key = "key";

        YAML::Node node;
        node[key] = testString;

        UNIT_ASSERT_VALUES_EQUAL(node[key].as<TString>(), refString);
        UNIT_ASSERT_VALUES_EQUAL(node[key].as<TString>(TString("fallback")), refString);

        auto stringNode = YAML::Node(testString);
        UNIT_ASSERT_VALUES_EQUAL(stringNode.Scalar(), YAML::convert<TString>::encode(refString).Scalar());

        TString buffer;
        YAML::convert<TString>::decode(stringNode, buffer);
        UNIT_ASSERT_VALUES_EQUAL(refString, buffer);
    }
}
