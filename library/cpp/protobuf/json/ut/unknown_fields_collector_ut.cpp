#include "json.h"
#include "proto.h"
#include "proto2json.h"

#include <library/cpp/protobuf/json/json2proto.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/set.h>
#include <util/generic/string.h>

using namespace NProtobufJson;
using namespace NProtobufJsonTest;

struct TTestUnknownFieldsCollector : public IUnknownFieldsCollector {
    void OnEnterMapItem(const TString& key) override {
        CurrentPath.push_back(key);
    }

    void OnEnterArrayItem(ui64 id) override {
        CurrentPath.push_back(ToString(id));
    }

    void OnLeaveMapItem() override {
        CurrentPath.pop_back();
    }

    void OnLeaveArrayItem() override {
        CurrentPath.pop_back();
    }

    void OnUnknownField(const TString& key, const google::protobuf::Descriptor& value) override {
        TString path;
        for (auto& piece : CurrentPath) {
            path.append("/");
            path.append(piece);
        }
        path.append("/");
        path.append(key);
        UnknownKeys.insert(std::move(path));
        Y_UNUSED(value);
    }

    TVector<TString> CurrentPath;
    TSet<TString> UnknownKeys;
};

Y_UNIT_TEST_SUITE(TUnknownFieldsCollectorTest) {
    Y_UNIT_TEST(TestFlatOptional) {
        TFlatOptional proto;
        TSimpleSharedPtr<TTestUnknownFieldsCollector> collector = new TTestUnknownFieldsCollector;
        TJson2ProtoConfig cfg;
        cfg.SetUnknownFieldsCollector(collector).SetAllowUnknownFields(true);

        Json2Proto(TStringBuf(R"({"42":42,"I32":11,"test":2,"string":"str","String":"string","obj":{"inner":{}},"arr":[1,2,3]})"), proto, cfg);
        TSet<TString> expectedKeys = {
            {"/42"},
            {"/arr"},
            {"/obj"},
            {"/string"},
            {"/test"},
        };
        UNIT_ASSERT(collector->CurrentPath.empty());
        UNIT_ASSERT_VALUES_EQUAL(collector->UnknownKeys, expectedKeys);
    }

    Y_UNIT_TEST(TestFlatRepeated) {
        TFlatRepeated proto;
        TSimpleSharedPtr<TTestUnknownFieldsCollector> collector = new TTestUnknownFieldsCollector;
        TJson2ProtoConfig cfg;
        cfg.SetUnknownFieldsCollector(collector).SetAllowUnknownFields(true);

        Json2Proto(TStringBuf(R"({"42":42,"I32":[11,12],"test":12,"string":"str","String":["string1","string2"],"obj":{"inner":{}},"arr":[1,2,3]})"), proto, cfg);
        TSet<TString> expectedKeys = {
            {"/42"},
            {"/arr"},
            {"/obj"},
            {"/string"},
            {"/test"},
        };
        UNIT_ASSERT(collector->CurrentPath.empty());
        UNIT_ASSERT_VALUES_EQUAL(collector->UnknownKeys, expectedKeys);
    }

    Y_UNIT_TEST(TestCompositeOptional) {
        TCompositeOptional proto;
        TSimpleSharedPtr<TTestUnknownFieldsCollector> collector = new TTestUnknownFieldsCollector;
        TJson2ProtoConfig cfg;
        cfg.SetUnknownFieldsCollector(collector).SetAllowUnknownFields(true);

        Json2Proto(TStringBuf(R"({"Part":{"42":42,"I32":11,"test":12,"string":"str","String":"string"},"string2":"str"})"), proto, cfg);
        TSet<TString> expectedKeys = {
            {"/Part/42"},
            {"/Part/string"},
            {"/Part/test"},
            {"/string2"},
        };
        UNIT_ASSERT(collector->CurrentPath.empty());
        UNIT_ASSERT_VALUES_EQUAL(collector->UnknownKeys, expectedKeys);
    }

    Y_UNIT_TEST(TestCompositeRepeated) {
        TCompositeRepeated proto;
        TSimpleSharedPtr<TTestUnknownFieldsCollector> collector = new TTestUnknownFieldsCollector;
        TJson2ProtoConfig cfg;
        cfg.SetUnknownFieldsCollector(collector).SetAllowUnknownFields(true);

        Json2Proto(TStringBuf(R"({"Part":[)"
                              R"(         {"42":42,"I32":11,"test":12,"string":"str","String":"string"},)"
                              R"(         {"abc":"d"})"
                              R"(],)"
                              R"("string2":"str"})"), proto, cfg);
        TSet<TString> expectedKeys = {
            {"/Part/0/42"},
            {"/Part/0/string"},
            {"/Part/0/test"},
            {"/Part/1/abc"},
            {"/string2"},
        };
        UNIT_ASSERT(collector->CurrentPath.empty());
        UNIT_ASSERT_VALUES_EQUAL(collector->UnknownKeys, expectedKeys);
    }

    Y_UNIT_TEST(TestCompleMapType) {
        TComplexMapType proto;
        TSimpleSharedPtr<TTestUnknownFieldsCollector> collector = new TTestUnknownFieldsCollector;
        TJson2ProtoConfig cfg;
        cfg.SetUnknownFieldsCollector(collector).SetAllowUnknownFields(true);

        Json2Proto(TStringBuf(R"({"42":42,)"
                              R"("Nested":[)"
                              R"(         {"key":"abc","value":{"string":"string","Nested":[{"key":"def","value":{"string2":"string2"}}]}},)"
                              R"(         {"key":"car","value":{"string3":"string3"}})"
                              R"(]})"), proto, cfg);
        TSet<TString> expectedKeys = {
            {"/42"},
            {"/Nested/0/value/Nested/0/value/string2"},
            {"/Nested/0/value/string"},
            {"/Nested/1/value/string3"},
        };
        UNIT_ASSERT(collector->CurrentPath.empty());
        UNIT_ASSERT_VALUES_EQUAL(collector->UnknownKeys, expectedKeys);
    }

    Y_UNIT_TEST(TestCompleMapTypeMapAsObject) {
        TComplexMapType proto;
        TSimpleSharedPtr<TTestUnknownFieldsCollector> collector = new TTestUnknownFieldsCollector;
        TJson2ProtoConfig cfg;
        cfg.SetUnknownFieldsCollector(collector).SetAllowUnknownFields(true).SetMapAsObject(true);

        Json2Proto(TStringBuf(R"({"42":42,)"
                              R"("Nested":{)"
                              R"(    "abc":{"string":"string","Nested":{"def":{"string2":"string2"}}},)"
                              R"(    "car":{"string3":"string3"})"
                              R"(}})"), proto, cfg);
        TSet<TString> expectedKeys = {
            {"/42"},
            {"/Nested/abc/Nested/def/string2"},
            {"/Nested/abc/string"},
            {"/Nested/car/string3"},
        };
        UNIT_ASSERT(collector->CurrentPath.empty());
        UNIT_ASSERT_VALUES_EQUAL(collector->UnknownKeys, expectedKeys);
    }
} // TJson2ProtoTest
