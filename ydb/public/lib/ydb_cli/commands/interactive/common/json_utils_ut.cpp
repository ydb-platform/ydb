#include "json_utils.h"

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

NJson::TJsonValue MakeString(const TString& s) {
    return NJson::TJsonValue(s);
}

NJson::TJsonValue MakeMap(const std::unordered_map<TString, NJson::TJsonValue>& pairs) {
    NJson::TJsonValue v(NJson::JSON_MAP);
    for (const auto& [key, val] : pairs) {
        v[key] = val;
    }
    return v;
}

NJson::TJsonValue MakeArray(const std::vector<NJson::TJsonValue>& elements) {
    NJson::TJsonValue v(NJson::JSON_ARRAY);
    for (const auto& el : elements) {
        v.AppendValue(el);
    }
    return v;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TJsonParserTests) {
    Y_UNIT_TEST(DefaultConstructorCreatesUndefined) {
        TJsonParser p;
        UNIT_ASSERT_EQUAL(p.GetValue().GetType(), NJson::JSON_UNDEFINED);
    }

    Y_UNIT_TEST(ConstructFromStringValue) {
        TJsonParser p(MakeString("hello"));
        UNIT_ASSERT_VALUES_EQUAL(p.GetString(), "hello");
        UNIT_ASSERT(!p.IsNull());
    }

    Y_UNIT_TEST(ParseValidJson) {
        TJsonParser p;
        UNIT_ASSERT(p.Parse(R"({"key": "value"})"));
        UNIT_ASSERT_VALUES_EQUAL(p.GetKey("key").GetString(), "value");
    }

    Y_UNIT_TEST(ParseInvalidJsonReturnsFalse) {
        TJsonParser p;
        UNIT_ASSERT(!p.Parse("not valid json {{{"));
    }

    Y_UNIT_TEST(ToStringOnStringValueReturnsRawString) {
        TJsonParser p(MakeString("hello"));
        UNIT_ASSERT_VALUES_EQUAL(p.ToString(), "hello");
    }

    Y_UNIT_TEST(ToStringOnNonStringFormatsJson) {
        TJsonParser p;
        p.Parse(R"({"key": "value"})");
        const TString output = p.ToString();
        UNIT_ASSERT(output.Contains("key"));
        UNIT_ASSERT(output.Contains("value"));
    }

    Y_UNIT_TEST(GetValueReturnsCurrentState) {
        NJson::TJsonValue v(42.0);
        TJsonParser p(v);
        UNIT_ASSERT_EQUAL(p.GetValue().GetType(), NJson::JSON_DOUBLE);
    }

    Y_UNIT_TEST(GetFieldNameRootReturnsDoller) {
        TJsonParser p;
        UNIT_ASSERT_VALUES_EQUAL(p.GetFieldName(), "$");
    }

    Y_UNIT_TEST(GetFieldNameChildReturnsPath) {
        TJsonParser p(MakeMap({{"key", MakeString("v")}}));
        UNIT_ASSERT_VALUES_EQUAL(p.GetKey("key").GetFieldName(), "$.key");
    }

    Y_UNIT_TEST(GetKeySuccess) {
        TJsonParser p(MakeMap({{"name", MakeString("alice")}}));
        UNIT_ASSERT_VALUES_EQUAL(p.GetKey("name").GetString(), "alice");
    }

    Y_UNIT_TEST(GetKeyMissingThrows) {
        TJsonParser p(MakeMap({{"x", MakeString("v")}}));
        UNIT_ASSERT_EXCEPTION(p.GetKey("missing"), yexception);
    }

    Y_UNIT_TEST(GetKeyOnNonMapThrows) {
        TJsonParser p(MakeString("hello"));
        UNIT_ASSERT_EXCEPTION(p.GetKey("key"), yexception);
    }

    Y_UNIT_TEST(MaybeKeyFound) {
        TJsonParser p(MakeMap({{"k", MakeString("v")}}));
        auto maybe = p.MaybeKey("k");
        UNIT_ASSERT(maybe.has_value());
        UNIT_ASSERT_VALUES_EQUAL(maybe->GetString(), "v");
    }

    Y_UNIT_TEST(MaybeKeyNotFound) {
        TJsonParser p(MakeMap({{"x", MakeString("v")}}));
        UNIT_ASSERT(!p.MaybeKey("missing").has_value());
    }

    Y_UNIT_TEST(MaybeKeyOnNonMapReturnsNullopt) {
        TJsonParser p(MakeString("hello"));
        UNIT_ASSERT(!p.MaybeKey("key").has_value());
    }

    Y_UNIT_TEST(GetElementSuccess) {
        TJsonParser p(MakeArray({MakeString("a"), MakeString("b")}));
        UNIT_ASSERT_VALUES_EQUAL(p.GetElement(0).GetString(), "a");
        UNIT_ASSERT_VALUES_EQUAL(p.GetElement(1).GetString(), "b");
    }

    Y_UNIT_TEST(GetElementOutOfBoundsThrows) {
        TJsonParser p(MakeArray({MakeString("a")}));
        UNIT_ASSERT_EXCEPTION(p.GetElement(1), yexception);
    }

    Y_UNIT_TEST(GetElementOnNonArrayThrows) {
        TJsonParser p(MakeString("hello"));
        UNIT_ASSERT_EXCEPTION(p.GetElement(0), yexception);
    }

    Y_UNIT_TEST(GetElementPathIncludesIndex) {
        TJsonParser p(MakeArray({MakeString("a"), MakeString("b")}));
        UNIT_ASSERT_VALUES_EQUAL(p.GetElement(1).GetFieldName(), "$[1]");
    }

    Y_UNIT_TEST(IterateVisitsAllElements) {
        TJsonParser p(MakeArray({MakeString("x"), MakeString("y"), MakeString("z")}));
        TVector<TString> collected;
        p.Iterate([&](TJsonParser elem) {
            collected.push_back(elem.GetString());
        });
        UNIT_ASSERT_VALUES_EQUAL(collected.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(collected[0], "x");
        UNIT_ASSERT_VALUES_EQUAL(collected[1], "y");
        UNIT_ASSERT_VALUES_EQUAL(collected[2], "z");
    }

    Y_UNIT_TEST(IterateEmptyArrayVisitsNothing) {
        TJsonParser p(MakeArray({}));
        int count = 0;
        p.Iterate([&](TJsonParser) { ++count; });
        UNIT_ASSERT_VALUES_EQUAL(count, 0);
    }

    Y_UNIT_TEST(IterateOnNonArrayThrows) {
        TJsonParser p(MakeString("hello"));
        UNIT_ASSERT_EXCEPTION(p.Iterate([](TJsonParser) {}), yexception);
    }

    Y_UNIT_TEST(GetStringSuccess) {
        TJsonParser p(MakeString("world"));
        UNIT_ASSERT_VALUES_EQUAL(p.GetString(), "world");
    }

    Y_UNIT_TEST(GetStringOnNonStringThrows) {
        TJsonParser p;
        p.Parse("42");
        UNIT_ASSERT_EXCEPTION(p.GetString(), yexception);
    }

    Y_UNIT_TEST(GetBooleanSafeTrue) {
        NJson::TJsonValue v(true);
        TJsonParser p(v);
        UNIT_ASSERT(p.GetBooleanSafe(false));
    }

    Y_UNIT_TEST(GetBooleanSafeFalse) {
        NJson::TJsonValue v(false);
        TJsonParser p(v);
        UNIT_ASSERT(!p.GetBooleanSafe(true));
    }

    Y_UNIT_TEST(GetBooleanSafeNonBooleanReturnsDefault) {
        TJsonParser p(MakeString("hello"));
        UNIT_ASSERT(p.GetBooleanSafe(true));
        UNIT_ASSERT(!p.GetBooleanSafe(false));
    }

    Y_UNIT_TEST(IsNullOnNullValue) {
        NJson::TJsonValue nullVal(NJson::JSON_NULL);
        TJsonParser p(nullVal);
        UNIT_ASSERT(p.IsNull());
    }

    Y_UNIT_TEST(IsNullOnNonNullValue) {
        TJsonParser p(MakeString("x"));
        UNIT_ASSERT(!p.IsNull());
    }

    Y_UNIT_TEST(ValidateTypeSucceedsOnCorrectType) {
        TJsonParser p(MakeString("s"));
        p.ValidateType(NJson::JSON_STRING); // must not throw
    }

    Y_UNIT_TEST(ValidateTypeThrowsOnWrongType) {
        TJsonParser p(MakeString("s"));
        UNIT_ASSERT_EXCEPTION(p.ValidateType(NJson::JSON_MAP), yexception);
    }

    Y_UNIT_TEST(NestedPathIsCorrect) {
        TJsonParser p(MakeMap({
            {"data", MakeArray({MakeMap({{"name", MakeString("bob")}})})}
        }));
        auto leaf = p.GetKey("data").GetElement(0).GetKey("name");
        UNIT_ASSERT_VALUES_EQUAL(leaf.GetFieldName(), "$.data[0].name");
        UNIT_ASSERT_VALUES_EQUAL(leaf.GetString(), "bob");
    }

    Y_UNIT_TEST(ExceptionMessageContainsFieldName) {
        TJsonParser p(MakeMap({{"val", MakeString("s")}}));
        try {
            p.GetKey("val").GetKey("missing");
            UNIT_FAIL("Expected exception");
        } catch (const yexception& e) {
            UNIT_ASSERT_C(TString(e.what()).Contains("val"), e.what());
        }
    }

}

Y_UNIT_TEST_SUITE(TJsonSchemaBuilderTests) {
    Y_UNIT_TEST(BuildStringSchema) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::String);
        builder.Type(TJsonSchemaBuilder::EType::String); // Same type second time should not throw
        const auto schema = builder.Build();
        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "string");
    }

    Y_UNIT_TEST(BuildBooleanSchema) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Boolean);
        const auto schema = builder.Build();
        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "boolean");
    }

    Y_UNIT_TEST(BuildObjectSchemaWithNoProperties) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Object);
        const auto schema = builder.Build();
        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "object");
    }

    Y_UNIT_TEST(BuildObjectSchemaWithDescription) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::String)
               .Description("A text field");
        const auto schema = builder.Build();
        UNIT_ASSERT_VALUES_EQUAL(schema["description"].GetString(), "A text field");
    }

    Y_UNIT_TEST(BuildObjectSchemaWithRequiredProperty) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Object)
               .Property("name")
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT(schema["properties"].GetMapSafe().contains("name"));
        const auto& required = schema["required"].GetArray();
        UNIT_ASSERT_VALUES_EQUAL(required.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(required[0].GetString(), "name");
    }

    Y_UNIT_TEST(BuildObjectSchemaWithOptionalProperty) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Object)
               .Property("notes", /* required = */ false)
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT(schema["properties"].GetMapSafe().contains("notes"));
        UNIT_ASSERT(!schema.Has("required") || schema["required"].GetArray().empty());
    }

    Y_UNIT_TEST(BuildObjectSchemaWithMultipleProperties) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Object)
               .Property("a").Type(TJsonSchemaBuilder::EType::String).Done()
               .Property("b").Type(TJsonSchemaBuilder::EType::Boolean).Done();
        const auto schema = builder.Build();

        const auto& props = schema["properties"].GetMapSafe();
        UNIT_ASSERT(props.contains("a"));
        UNIT_ASSERT(props.contains("b"));
        UNIT_ASSERT_VALUES_EQUAL(props.at("a")["type"].GetString(), "string");
        UNIT_ASSERT_VALUES_EQUAL(props.at("b")["type"].GetString(), "boolean");
    }

    Y_UNIT_TEST(RequiredAddsToRequiredList) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Object)
               .Property("x", /* required = */ false)
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done()
               .Required("x");
        const auto schema = builder.Build();

        const auto& required = schema["required"].GetArray();
        UNIT_ASSERT_VALUES_EQUAL(required.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(required[0].GetString(), "x");
    }

    Y_UNIT_TEST(SetObjectTypeAfterProperty) {
        TJsonSchemaBuilder builder;
        builder.Property("name")
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done()
                .Type(TJsonSchemaBuilder::EType::Object);
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "object");
        UNIT_ASSERT(schema["properties"].GetMapSafe().contains("name"));
    }

    Y_UNIT_TEST(PropertyAutoSetsTypeToObjectWhenUndefined) {
        TJsonSchemaBuilder builder;
        builder.Property("name")
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "object");
        UNIT_ASSERT(schema["properties"].GetMapSafe().contains("name"));
    }

    Y_UNIT_TEST(RequiredAutoSetsTypeToObjectWhenUndefined) {
        TJsonSchemaBuilder builder;
        builder.Property("x", /* required = */ false)
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done()
               .Required("x");
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "object");
        const auto& required = schema["required"].GetArray();
        UNIT_ASSERT_VALUES_EQUAL(required.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(required[0].GetString(), "x");
    }

    Y_UNIT_TEST(PropertyKeepsExplicitObjectType) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Object)
               .Property("a")
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "object");
        UNIT_ASSERT(schema["properties"].GetMapSafe().contains("a"));
    }

    Y_UNIT_TEST(RequiredKeepsExplicitObjectType) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Object)
               .Required("a");
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "object");
    }

    Y_UNIT_TEST(MultiplePropertiesWithAutoType) {
        TJsonSchemaBuilder builder;
        builder.Property("a")
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done()
               .Property("b", /* required = */ false)
                   .Type(TJsonSchemaBuilder::EType::Boolean)
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "object");
        const auto& props = schema["properties"].GetMapSafe();
        UNIT_ASSERT(props.contains("a"));
        UNIT_ASSERT(props.contains("b"));
        UNIT_ASSERT_VALUES_EQUAL(props.at("a")["type"].GetString(), "string");
        UNIT_ASSERT_VALUES_EQUAL(props.at("b")["type"].GetString(), "boolean");

        const auto& required = schema["required"].GetArray();
        UNIT_ASSERT_VALUES_EQUAL(required.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(required[0].GetString(), "a");
    }

    Y_UNIT_TEST(BuildArraySchemaWithExplicitType) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Array);
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "array");
        UNIT_ASSERT(!schema.Has("items"));
    }

    Y_UNIT_TEST(BuildArraySchemaWithStringItems) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Array)
               .Items()
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "array");
        UNIT_ASSERT(schema.Has("items"));
        UNIT_ASSERT_VALUES_EQUAL(schema["items"]["type"].GetString(), "string");
    }

    Y_UNIT_TEST(BuildArraySchemaWithObjectItems) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Array)
               .Items()
                   .Type(TJsonSchemaBuilder::EType::Object)
                   .Property("name")
                       .Type(TJsonSchemaBuilder::EType::String)
                       .Done()
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "array");
        UNIT_ASSERT_VALUES_EQUAL(schema["items"]["type"].GetString(), "object");
        UNIT_ASSERT(schema["items"]["properties"].GetMapSafe().contains("name"));
    }

    Y_UNIT_TEST(ItemsAutoSetsTypeToArrayWhenUndefined) {
        TJsonSchemaBuilder builder;
        builder.Items()
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "array");
        UNIT_ASSERT_VALUES_EQUAL(schema["items"]["type"].GetString(), "string");
    }

    Y_UNIT_TEST(ItemsKeepsExplicitArrayType) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Array)
               .Items()
                   .Type(TJsonSchemaBuilder::EType::Boolean)
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "array");
        UNIT_ASSERT_VALUES_EQUAL(schema["items"]["type"].GetString(), "boolean");
    }

    Y_UNIT_TEST(SetArrayTypeAfterItems) {
        TJsonSchemaBuilder builder;
        builder.Items()
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Done()
               .Type(TJsonSchemaBuilder::EType::Array);
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "array");
        UNIT_ASSERT_VALUES_EQUAL(schema["items"]["type"].GetString(), "string");
    }

    Y_UNIT_TEST(BuildArraySchemaWithDescription) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Array)
               .Description("List of tags")
               .Items()
                   .Type(TJsonSchemaBuilder::EType::String)
                   .Description("A tag")
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "array");
        UNIT_ASSERT_VALUES_EQUAL(schema["description"].GetString(), "List of tags");
        UNIT_ASSERT_VALUES_EQUAL(schema["items"]["type"].GetString(), "string");
        UNIT_ASSERT_VALUES_EQUAL(schema["items"]["description"].GetString(), "A tag");
    }

    Y_UNIT_TEST(BuildArrayOfArrays) {
        TJsonSchemaBuilder builder;
        builder.Type(TJsonSchemaBuilder::EType::Array)
               .Items()
                   .Type(TJsonSchemaBuilder::EType::Array)
                   .Items()
                       .Type(TJsonSchemaBuilder::EType::String)
                       .Done()
                   .Done();
        const auto schema = builder.Build();

        UNIT_ASSERT_VALUES_EQUAL(schema["type"].GetString(), "array");
        UNIT_ASSERT_VALUES_EQUAL(schema["items"]["type"].GetString(), "array");
        UNIT_ASSERT_VALUES_EQUAL(schema["items"]["items"]["type"].GetString(), "string");
    }
}

Y_UNIT_TEST_SUITE(FormatJsonValueTests) {
    Y_UNIT_TEST(FormatJsonValueStringReturnsRaw) {
        NJson::TJsonValue v("hello");
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonValue(v), "hello");
    }

    Y_UNIT_TEST(FormatJsonValueObjectContainsKeys) {
        const auto v = MakeMap({{"k", MakeString("v")}});
        const TString output = FormatJsonValue(v);
        UNIT_ASSERT(output.Contains("k"));
        UNIT_ASSERT(output.Contains("v"));
    }

    Y_UNIT_TEST(FormatJsonValueStringFromValidJson) {
        const TString json = R"({"x": "y"})";
        const TString output = FormatJsonValue(json);
        UNIT_ASSERT(output.Contains("x"));
        UNIT_ASSERT(output.Contains("y"));
    }

    Y_UNIT_TEST(FormatJsonValueStringFromInvalidJsonReturnsAsIs) {
        const TString bad = "not json at all";
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonValue(bad), bad);
    }
}

} // namespace NYdb::NConsoleClient::NAi
