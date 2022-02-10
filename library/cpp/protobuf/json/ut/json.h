#pragma once

#include <library/cpp/protobuf/json/ut/test.pb.h>

#include <library/cpp/json/json_value.h>

#include <cstdarg>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

#include <util/system/defaults.h>

namespace NProtobufJsonTest {
    inline NJson::TJsonValue
    CreateFlatJson(const THashSet<TString>& skippedKeys = THashSet<TString>()) {
        NJson::TJsonValue json;

#define DEFINE_FIELD(name, value)                     \
    if (skippedKeys.find(#name) == skippedKeys.end()) \
        json.InsertValue(#name, value);
#include "fields.incl"
#undef DEFINE_FIELD

        return json;
    }

    inline NJson::TJsonValue
    CreateRepeatedFlatJson(const THashSet<TString>& skippedKeys = THashSet<TString>()) {
        NJson::TJsonValue json;

#define DEFINE_REPEATED_FIELD(name, type, ...)                         \
    if (skippedKeys.find(#name) == skippedKeys.end()) {                \
        type values[] = {__VA_ARGS__};                                 \
        NJson::TJsonValue array(NJson::JSON_ARRAY);                    \
        for (size_t i = 0, end = Y_ARRAY_SIZE(values); i < end; ++i) { \
            array.AppendValue(values[i]);                              \
        }                                                              \
        json.InsertValue(#name, array);                                \
    }
#include "repeated_fields.incl"
#undef DEFINE_REPEATED_FIELD

        return json;
    }

    inline NJson::TJsonValue
    CreateCompositeJson(const THashSet<TString>& skippedKeys = THashSet<TString>()) {
        const NJson::TJsonValue& part = CreateFlatJson(skippedKeys);
        NJson::TJsonValue json;
        json.InsertValue("Part", part);

        return json;
    }

#define UNIT_ASSERT_JSONS_EQUAL(lhs, rhs)                                        \
    if (lhs != rhs) {                                                            \
        UNIT_ASSERT_STRINGS_EQUAL(lhs.GetStringRobust(), rhs.GetStringRobust()); \
    }

#define UNIT_ASSERT_JSON_STRINGS_EQUAL(lhs, rhs)           \
    if (lhs != rhs) {                                      \
        NJson::TJsonValue _lhs_json, _rhs_json;            \
        UNIT_ASSERT(NJson::ReadJsonTree(lhs, &_lhs_json)); \
        UNIT_ASSERT(NJson::ReadJsonTree(rhs, &_rhs_json)); \
        UNIT_ASSERT_JSONS_EQUAL(_lhs_json, _rhs_json);     \
    }

}
