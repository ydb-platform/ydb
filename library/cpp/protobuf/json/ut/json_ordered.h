#pragma once

#include <library/cpp/protobuf/json/ut/test.pb.h>

#include <library/cpp/json/ordered_maps/json_value_ordered.h>

#include <cstdarg>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

#include <util/system/defaults.h>

namespace NProtobufJsonTest {
    inline NJson::NOrderedJson::TJsonValue
    CreateFlatOrderedJson(const THashSet<TString>& skippedKeys = THashSet<TString>()) {
        NJson::NOrderedJson::TJsonValue json;

#define DEFINE_FIELD(name, value)                     \
    if (skippedKeys.find(#name) == skippedKeys.end()) \
        json.InsertValue(#name, value);
#include <library/cpp/protobuf/json/ut/fields.incl>
#undef DEFINE_FIELD

        return json;
    }

    inline NJson::NOrderedJson::TJsonValue
    CreateRepeatedFlatOrderedJson(const THashSet<TString>& skippedKeys = THashSet<TString>()) {
        NJson::NOrderedJson::TJsonValue json;

#define DEFINE_REPEATED_FIELD(name, type, ...)                         \
    if (skippedKeys.find(#name) == skippedKeys.end()) {                \
        type values[] = {__VA_ARGS__};                                 \
        NJson::NOrderedJson::TJsonValue array(NJson::NOrderedJson::JSON_ARRAY);                    \
        for (size_t i = 0, end = Y_ARRAY_SIZE(values); i < end; ++i) { \
            array.AppendValue(values[i]);                              \
        }                                                              \
        json.InsertValue(#name, array);                                \
    }
#include <library/cpp/protobuf/json/ut/repeated_fields.incl>
#undef DEFINE_REPEATED_FIELD

        return json;
    }

    inline NJson::NOrderedJson::TJsonValue
    CreateCompositeOrderedJson(const THashSet<TString>& skippedKeys = THashSet<TString>()) {
        const NJson::NOrderedJson::TJsonValue& part = CreateFlatOrderedJson(skippedKeys);
        NJson::NOrderedJson::TJsonValue json;
        json.InsertValue("Part", part);

        return json;
    }

#define UNIT_ASSERT_JSONS_EQUAL(lhs, rhs)                                        \
    if (lhs != rhs) {                                                            \
        UNIT_ASSERT_STRINGS_EQUAL(lhs.GetStringRobust(), rhs.GetStringRobust()); \
    }

#define UNIT_ASSERT_JSON_STRINGS_EQUAL(lhs, rhs)           \
    if (lhs != rhs) {                                      \
        NJson::NOrderedJson::TJsonValue _lhs_json, _rhs_json;            \
        UNIT_ASSERT(NJson::NOrderedJson::ReadJsonTree(lhs, &_lhs_json)); \
        UNIT_ASSERT(NJson::NOrderedJson::ReadJsonTree(rhs, &_rhs_json)); \
        UNIT_ASSERT_JSONS_EQUAL(_lhs_json, _rhs_json);     \
    }

}
