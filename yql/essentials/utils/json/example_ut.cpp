#include "from.h"
#include "to.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

using namespace NYql::NJson;

namespace NExample {

enum class EPriority {
    Low,
    Medium,
    High,
};

struct TTag {
    i64 Id;
    TString Label;
};

struct TDocument {
    ui64 DocumentId;
    TString Title;
    EPriority Priority;
    TVector<TTag> Tags;
    TMaybe<TString> Description;
    TMaybe<i64> Rating;
};

struct TLibrary {
    TString LibraryName;
    TVector<TDocument> Documents;
};

} // namespace NExample

namespace NYql::NJson {

JSON_DECLARE_FROM(NExample::EPriority, json);
JSON_DECLARE_TO(NExample::EPriority, value);
JSON_DECLARE_FROM(NExample::TTag, json);
JSON_DECLARE_TO(NExample::TTag, value);
JSON_DECLARE_FROM(NExample::TDocument, json);
JSON_DECLARE_TO(NExample::TDocument, value);
JSON_DECLARE_FROM(NExample::TLibrary, json);
JSON_DECLARE_TO(NExample::TLibrary, value);

} // namespace NYql::NJson

namespace NYql::NJson {

JSON_DEFINE_FROM(NExample::EPriority, json) {
    if (!json.IsString()) {
        return std::unexpected(TString("must be a string"));
    }

    const TString& value = json.GetStringSafe();
    if (value == "Low") {
        return NExample::EPriority::Low;
    }
    if (value == "Medium") {
        return NExample::EPriority::Medium;
    }
    if (value == "High") {
        return NExample::EPriority::High;
    }

    return std::unexpected(TString::Join("unknown priority: ", value));
}

JSON_DEFINE_TO(NExample::EPriority, value) {
    switch (value) {
        case NExample::EPriority::Low:
            return "Low";
        case NExample::EPriority::Medium:
            return "Medium";
        case NExample::EPriority::High:
            return "High";
    }
}

JSON_DEFINE_FROM(NExample::TTag, json) {
    NExample::TTag x;
    JSON_MOVE_FROM(json, "id", x.Id);
    JSON_MOVE_FROM(json, "label", x.Label);
    return x;
}

JSON_DEFINE_TO(NExample::TTag, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "id", value.Id);
    SaveTo(json, "label", std::move(value.Label));
    return json;
}

JSON_DEFINE_FROM(NExample::TDocument, json) {
    NExample::TDocument x;
    JSON_MOVE_FROM(json, "document_id", x.DocumentId);
    JSON_MOVE_FROM(json, "title", x.Title);
    JSON_MOVE_FROM(json, "priority", x.Priority);
    JSON_MOVE_FROM(json, "tags", x.Tags);
    JSON_MOVE_FROM(json, "description", x.Description);
    JSON_MOVE_FROM(json, "rating", x.Rating);
    return x;
}

JSON_DEFINE_TO(NExample::TDocument, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "document_id", value.DocumentId);
    SaveTo(json, "title", std::move(value.Title));
    SaveTo(json, "priority", value.Priority);
    SaveTo(json, "tags", std::move(value.Tags));
    SaveTo(json, "description", std::move(value.Description));
    SaveTo(json, "rating", value.Rating);
    return json;
}

JSON_DEFINE_FROM(NExample::TLibrary, json) {
    NExample::TLibrary x;
    JSON_MOVE_FROM(json, "library_name", x.LibraryName);
    JSON_MOVE_FROM(json, "documents", x.Documents);
    return x;
}

JSON_DEFINE_TO(NExample::TLibrary, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "library_name", std::move(value.LibraryName));
    SaveTo(json, "documents", std::move(value.Documents));
    return json;
}

} // namespace NYql::NJson

Y_UNIT_TEST_SUITE(JsonExample) {

Y_UNIT_TEST(Happy) {
    const TStringBuf input = R"json({
        "library_name": "Corporate Document Archive",
        "documents": [
            {
                "document_id": 10203040506070809,
                "title": "System Architecture Overview",
                "priority": "High",
                "tags": [
                    {"id": 1001, "label": "architecture"},
                    {"id": 1002, "label": "infrastructure"}
                ],
                "description": "A high-level overview of the distributed system architecture.",
                "rating": 5
            },
            {
                "document_id": 10203040506070810,
                "title": "Weekly Team Notes",
                "priority": "Low",
                "tags": []
            },
            {
                "document_id": 9988776655443322,
                "title": "Q3 Financial Report",
                "priority": "Medium",
                "tags": [
                    {"id": -500, "label": "confidential"}
                ],
                "description": "",
                "rating": -1
            }
        ]
    })json";

    auto library = FromJsonString<NExample::TLibrary>(input);
    UNIT_ASSERT(library);

    const auto& docs = library->Documents;
    UNIT_ASSERT_VALUES_EQUAL(library->LibraryName, "Corporate Document Archive");
    UNIT_ASSERT_VALUES_EQUAL(docs.size(), 3);

    UNIT_ASSERT_VALUES_EQUAL(docs[0].DocumentId, static_cast<ui64>(10203040506070809));
    UNIT_ASSERT_VALUES_EQUAL(docs[0].Title, "System Architecture Overview");
    UNIT_ASSERT(docs[0].Priority == NExample::EPriority::High);
    UNIT_ASSERT_VALUES_EQUAL(docs[0].Tags.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(docs[0].Tags[0].Id, 1001);
    UNIT_ASSERT_VALUES_EQUAL(docs[0].Tags[0].Label, "architecture");
    UNIT_ASSERT_VALUES_EQUAL(docs[0].Tags[1].Label, "infrastructure");
    UNIT_ASSERT(docs[0].Description.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*docs[0].Description, "A high-level overview of the distributed system architecture.");
    UNIT_ASSERT(docs[0].Rating.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*docs[0].Rating, 5);

    UNIT_ASSERT_VALUES_EQUAL(docs[1].DocumentId, static_cast<ui64>(10203040506070810));
    UNIT_ASSERT(docs[1].Priority == NExample::EPriority::Low);
    UNIT_ASSERT(docs[1].Tags.empty());
    UNIT_ASSERT(!docs[1].Description.Defined());
    UNIT_ASSERT(!docs[1].Rating.Defined());

    UNIT_ASSERT_VALUES_EQUAL(docs[2].DocumentId, static_cast<ui64>(9988776655443322));
    UNIT_ASSERT(docs[2].Priority == NExample::EPriority::Medium);
    UNIT_ASSERT_VALUES_EQUAL(docs[2].Tags.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(docs[2].Tags[0].Id, -500);
    UNIT_ASSERT_VALUES_EQUAL(docs[2].Tags[0].Label, "confidential");
    UNIT_ASSERT(docs[2].Description.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*docs[2].Description, "");
    UNIT_ASSERT(docs[2].Rating.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*docs[2].Rating, -1);

    const TString input1 = ToJsonString(std::move(*library));
    const TString input2 = ToJsonString(FromJsonString<NExample::TLibrary>(input1).value());
    UNIT_ASSERT_NO_DIFF(input1, input2);
}

Y_UNIT_TEST(BadEnum) {
    const auto result = FromJsonString<NExample::TDocument>(R"json({
        "document_id": 1,
        "title": "Doc",
        "priority": "Urgent",
        "tags": []
    })json");

    UNIT_ASSERT(!result);
    UNIT_ASSERT_STRING_CONTAINS(result.error(), "priority");
    UNIT_ASSERT_STRING_CONTAINS(result.error(), "Urgent");
}

Y_UNIT_TEST(MissingRequiredField) {
    auto result = FromJsonString<NExample::TDocument>(R"json({
        "title": "Doc",
        "priority": "Low",
        "tags": []
    })json");

    UNIT_ASSERT(!result);
    UNIT_ASSERT_STRING_CONTAINS(result.error(), "document_id");
    UNIT_ASSERT_STRING_CONTAINS(result.error(), "required");
}

Y_UNIT_TEST(TypeMismatchObjectInsteadOfList) {
    auto result = FromJsonString<NExample::TDocument>(R"json({
        "document_id": 10203040506070810,
        "title": "Weekly Team Notes",
        "priority": "Low",
        "tags": { "a": 1 }
    })json");

    UNIT_ASSERT(!result);
    UNIT_ASSERT_STRING_CONTAINS(result.error(), "\"tags\" must be an array");
}

Y_UNIT_TEST(TypeMismatchListInsteadOfObject) {
    auto result = FromJsonString<NExample::TDocument>(R"json([
    ])json");

    UNIT_ASSERT(!result);
    UNIT_ASSERT_STRING_CONTAINS(result.error(), "expected an object with key \"document_id\", but got Array");
}

} // Y_UNIT_TEST_SUITE(JsonExample)
