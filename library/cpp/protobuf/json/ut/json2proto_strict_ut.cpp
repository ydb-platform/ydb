#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/ut/test.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/struct.pb.h>

#include <util/generic/set.h>
#include <util/stream/str.h>

using namespace NProtobufJson;
using namespace NProtobufJsonTest;

namespace {
    TJson2ProtoConfig ValidationConfig(unsigned flags) {
        return TJson2ProtoConfig()
            .SetAllowDuplicateJsonKeys(flags & 1)
            .SetAllowMultipleFieldAliases(flags & 2)
            .SetAllowOneofConflicts(flags & 4);
    }

    class TUnknownFieldsCollector: public IUnknownFieldsCollector {
    public:
        void OnEnterMapItem(const TString&) override {
        }

        void OnLeaveMapItem() override {
        }

        void OnEnterArrayItem(ui64) override {
        }

        void OnLeaveArrayItem() override {
        }

        void OnUnknownField(const TString& key, const google::protobuf::Descriptor&) override {
            UnknownFields.insert(key);
        }

        TSet<TString> UnknownFields;
    };
} // namespace

Y_UNIT_TEST_SUITE(TJson2ProtoStrictValidation) {
    Y_UNIT_TEST(UnknownFields) {
        for (const TStringBuf json : {R"({"Unknown":null})", R"({"Child":{"textValue":"ok","Unknown":1}})"}) {
            auto config = ValidationConfig(0).SetAllowFieldNameAliases(true);
            UNIT_ASSERT_NO_EXCEPTION(Json2Proto<TStrictValidation>(json, config.SetAllowUnknownFields(true)));
            TSimpleSharedPtr<TUnknownFieldsCollector> collector = new TUnknownFieldsCollector;
            Json2Proto<TStrictValidation>(json, config.SetUnknownFieldsCollector(collector));
            const TSet<TString> expected = {"Unknown"};
            UNIT_ASSERT_VALUES_EQUAL(collector->UnknownFields, expected);
            UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(json, config.SetAllowUnknownFields(false)), yexception, "Unknown");
        }
    }

    Y_UNIT_TEST(UnknownEnumValues) {
        auto config = ValidationConfig(0).SetAllowUnknownFields(false);
        for (const TStringBuf json : {R"({"Enum":"Unknown"})", R"({"Enum":100})"}) {
            UNIT_ASSERT_EXCEPTION(Json2Proto<TFlatOptional>(json, config.SetAllowUnknownEnumValues(false)), yexception);
            UNIT_ASSERT(!Json2Proto<TFlatOptional>(json, config.SetAllowUnknownEnumValues(true)).HasEnum());
        }
    }

    Y_UNIT_TEST(AllFlagCombinations) {
        for (unsigned flags = 0; flags < 8; ++flags) {
            const auto config = ValidationConfig(flags).SetAllowFieldNameAliases(true);
            const TStringBuf duplicate = R"({"Number":1,"Number":2})";
            if (!(flags & 1)) {
                UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(duplicate, config), yexception, "duplicate JSON key");
            } else {
                UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(duplicate, config).GetNumber(), 2);
            }
            const TStringBuf aliases = R"({"Text":"first","textValue":"second"})";
            if (!(flags & 2)) {
                UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(aliases, config), yexception, "multiple JSON keys");
            } else {
                UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(aliases, config).GetText(), "first");
            }
            const TStringBuf oneof = R"({"Number":0,"Flag":false})";
            if (!(flags & 4)) {
                UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(oneof, config), yexception, "Choice");
            } else {
                const auto proto = Json2Proto<TStrictValidation>(oneof, config);
                UNIT_ASSERT(proto.HasFlag());
                UNIT_ASSERT(!proto.HasNumber());
                UNIT_ASSERT(!proto.GetFlag());
            }
            UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(R"({"Text":"ok","OtherNumber":1})", config).GetText(), "ok");
        }
    }

    Y_UNIT_TEST(OneofConflicts) {
        const auto config = TJson2ProtoConfig().SetAllowOneofConflicts(false).SetMapAsObject(true);
        for (const TStringBuf json : {R"({"Text":"","Object":{}})", R"({"Child":{"Number":1,"Flag":true}})",
                                      R"({"Children":[{"Number":1,"Flag":true}]})",
                                      R"({"Items":{"key":{"Number":0,"Flag":false}}})",
                                      R"({"Items":[{"key":"k","value":{"Number":0,"Flag":false}}]})"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(json, config), yexception, "Choice");
        }
        UNIT_ASSERT_NO_EXCEPTION(Json2Proto<TStrictValidation>(
            R"({"Items":{"first":{"Number":1},"second":{"Number":2}}})", config));
    }

    Y_UNIT_TEST(OneofPresence) {
        for (const TStringBuf json : {R"({"Number":null,"Flag":null})", R"({"Number":1,"OtherText":"ok"})"}) {
            UNIT_ASSERT_NO_EXCEPTION(Json2Proto<TStrictValidation>(json, TJson2ProtoConfig().SetAllowOneofConflicts(false)));
        }
        const auto proto = Json2Proto<TStrictValidation>(R"({"Number":0,"Flag":null})", TJson2ProtoConfig().SetAllowOneofConflicts(false));
        UNIT_ASSERT(proto.HasNumber());
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["Number"] = 0;
        json["Flag"] = NJson::TJsonValue(NJson::JSON_UNDEFINED);
        TStrictValidation fromTree;
        Json2Proto(json, fromTree, TJson2ProtoConfig().SetAllowOneofConflicts(false));
        UNIT_ASSERT(fromTree.HasNumber());
    }

    Y_UNIT_TEST(DuplicateKeys) {
        const auto config = TJson2ProtoConfig().SetAllowDuplicateJsonKeys(false);
        for (const TStringBuf json : {R"({"Number":null,"Number":2})", R"({"Number":1,"\u004eumber":2})",
                                      R"({"Child":{},"Child":{}})",
                                      R"({"Children":[{}],"Children":[]})",
                                      R"({"Children":[{"Number":1,"Number":2}]})",
                                      R"({"Items":{"key":{"Number":1},"key":{"Number":2}}})",
                                      R"({"Unknown":null,"Unknown":1})"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(json, config), yexception, "duplicate JSON key");
        }
        const TStringBuf duplicate = R"({"Number":1,"Number":2})";
        TMemoryInput stream{duplicate};
        UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(stream, config), yexception, "duplicate JSON key");
        TMemoryInput streamWithConfig{duplicate};
        UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(streamWithConfig, NJson::TJsonReaderConfig(), config), yexception, "duplicate JSON key");
        TMemoryInput streamWithDestination{duplicate};
        TStrictValidation destination;
        UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto(streamWithDestination, destination, config), yexception, "duplicate JSON key");
        const auto proto = Json2Proto<TStrictValidation>(
            R"({"Number":1,"Child":{"Number":2},"Children":[{"Number":3},{"Number":4}]})", config);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetChildren(1).GetNumber(), 4);
    }

    Y_UNIT_TEST(FieldNamesAndAliases) {
        auto config = TJson2ProtoConfig().SetAllowMultipleFieldAliases(false).SetAllowFieldNameAliases(true).SetMapAsObject(true);
        UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(R"({"textValue":"ok"})", config).GetText(), "ok");
        for (const TStringBuf json : {R"({"Text":null,"textValue":"b"})", R"({"Text":null,"textValue":null})",
                                      R"({"Child":{"Text":"a","textValue":"b"}})",
                                      R"({"Children":[{"Text":"a","textValue":"b"}]})",
                                      R"({"Items":[{"key":"k","value":{"Text":"a","textValue":"b"}}]})",
                                      R"({"Items":{"k":{"Text":"a","textValue":"b"}}})"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(json, config), yexception, "multiple JSON keys");
        }
        UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(R"({"Text":"a","textValue":"b"})", config.SetAllowFieldNameAliases(false)).GetText(), "a");
        config.SetAllowFieldNameAliases(true).SetNameGenerator([](const google::protobuf::FieldDescriptor& field) {
            return TString("prefix_") + field.name();
        });
        UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(R"({"prefix_Text":"a","textValue":"b"})", config), yexception, "multiple JSON keys");
    }

    Y_UNIT_TEST(AliasSelectionWithOneofValidation) {
        auto config = TJson2ProtoConfig().SetAllowFieldNameAliases(true).SetAllowOneofConflicts(false);
        const TStringBuf aliases = R"({"Text":"original","textValue":"json","prefix_Text":"generated"})";
        UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(aliases, config).GetText(), "original");
        UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(aliases, config.SetUseJsonName(true)).GetText(), "json");
        config.SetNameGenerator([](const google::protobuf::FieldDescriptor& field) {
            return TString("prefix_") + field.name();
        });
        UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(aliases, config).GetText(), "generated");
        UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(R"({"prefix_Text":null,"Text":"original","textValue":"json"})", config).GetText(), "json");
        UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(R"({"prefix_Text":null,"Text":"original","textValue":null})", config).GetText(), "original");
        UNIT_ASSERT_NO_EXCEPTION(Json2Proto<TStrictValidation>(R"({"Number":1,"Text":null,"textValue":null})", config));
        for (const TStringBuf json : {R"({"Number":1,"Text":null,"textValue":"json"})",
                                      R"({"Number":1,"Text":"original","textValue":null})"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<TStrictValidation>(json, config), yexception, "Choice");
        }
    }

    Y_UNIT_TEST(MergeAndTree) {
        const auto config = ValidationConfig(0).SetAllowFieldNameAliases(true);
        auto proto = Json2Proto<TStrictValidation>(R"({"Number":1})", config);
        MergeJson2Proto(TStringBuf(R"({"Flag":false})"), proto, config);
        UNIT_ASSERT(proto.HasFlag());
        UNIT_ASSERT(!proto.HasNumber());
        UNIT_ASSERT_EXCEPTION(MergeJson2Proto(TStringBuf(R"({"Flag":true,"Flag":false})"), proto, config), yexception);
        NJson::TJsonValue json;
        for (const TStringBuf conflict : {R"({"Number":1,"Text":"x"})", R"({"Text":null,"textValue":"x"})"}) {
            NJson::ReadJsonTree(conflict, &json, true);
            UNIT_ASSERT_EXCEPTION(Json2Proto(json, proto, config), yexception);
            UNIT_ASSERT_EXCEPTION(MergeJson2Proto(json, proto, config), yexception);
        }
        NJson::ReadJsonTree(R"({"Number":1,"Number":2})", &json, true);
        Json2Proto(json, proto, config);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetNumber(), 2);
    }

    Y_UNIT_TEST(FreeFormJson) {
        const auto config = ValidationConfig(0).SetAllowUnknownFields(false);
        const TStringBuf json = R"({"arbitrary":{"keys":[1,{"nested":true}]}})";
        UNIT_ASSERT_NO_EXCEPTION(Json2Proto<google::protobuf::Struct>(json, config));
        UNIT_ASSERT_NO_EXCEPTION(Json2Proto<google::protobuf::Value>(json, config));
        UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<google::protobuf::Struct>(R"({"a":1,"a":2})", config), yexception, "duplicate JSON key");
        UNIT_ASSERT_EXCEPTION_CONTAINS(Json2Proto<google::protobuf::Value>(R"([{"a":1,"a":2}])", config), yexception, "duplicate JSON key");
    }

    Y_UNIT_TEST(ReaderOptions) {
        UNIT_ASSERT_NO_EXCEPTION(Json2Proto<TStrictValidation>("{/*comment*/\"Number\":1}", TJson2ProtoConfig().SetAllowDuplicateJsonKeys(false).SetAllowComments(true)));
        NJson::TJsonReaderConfig readerConfig;
        readerConfig.AllowComments = true;
        TMemoryInput in("{/*comment*/\"Number\":1}");
        UNIT_ASSERT_VALUES_EQUAL(Json2Proto<TStrictValidation>(in, readerConfig, TJson2ProtoConfig().SetAllowDuplicateJsonKeys(false)).GetNumber(), 1);
        TMemoryInput invalid("{\"Number\":");
        UNIT_ASSERT_EXCEPTION(Json2Proto<TStrictValidation>(invalid, TJson2ProtoConfig().SetAllowDuplicateJsonKeys(false)), yexception);
    }

    Y_UNIT_TEST(MaxDepth) {
        const struct {
            TStringBuf Json;
            ui64 MinDepth;
        } cases[] = {
            {"{}", 1},
            {"[1,null]", 1},
            {R"({"Number":null})", 2},
            {R"({"Children":[{}]})", 3},
            {R"({"Children":[{"Number":1}]})", 4},
        };
        for (const auto& test : cases) {
            for (const bool allowDuplicates : {false, true}) {
                for (const bool iterative : {false, true}) {
                    for (ui64 maxDepth : {ui64(0), test.MinDepth - 1, test.MinDepth}) {
                        NJson::TJsonReaderConfig readerConfig;
                        readerConfig.MaxDepth = maxDepth;
                        readerConfig.UseIterativeParser = iterative;
                        TMemoryInput in(test.Json);
                        const auto config = TJson2ProtoConfig().SetAllowDuplicateJsonKeys(allowDuplicates);
                        if (maxDepth == 0 || maxDepth >= test.MinDepth) {
                            UNIT_ASSERT_NO_EXCEPTION_C(Json2Proto<google::protobuf::Value>(in, readerConfig, config),
                                                       test.Json << ", maxDepth=" << maxDepth << ", allowDuplicates=" << allowDuplicates << ", iterative=" << iterative);
                        } else {
                            UNIT_ASSERT_EXCEPTION_C(Json2Proto<google::protobuf::Value>(in, readerConfig, config), yexception,
                                                    test.Json << ", maxDepth=" << maxDepth << ", allowDuplicates=" << allowDuplicates << ", iterative=" << iterative);
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(DurationConversion) {
        const auto config = ValidationConfig(0).SetAllowString2TimeConversion(true).SetAllowUnknownFields(false);
        const auto proto = Json2Proto<TSingleDuration>(R"({"Duration":{"Seconds":1,"Nanos":2,"Unknown":null}})", config);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetDuration().seconds(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetDuration().nanos(), 2);
    }

    Y_UNIT_TEST(AllowedByDefault) {
        for (const auto& config : {TJson2ProtoConfig(),
                                   ValidationConfig(0).SetAllowDuplicateJsonKeys(true).SetAllowMultipleFieldAliases(true).SetAllowOneofConflicts(true)}) {
            UNIT_ASSERT(config.AllowDuplicateJsonKeys);
            UNIT_ASSERT(config.AllowMultipleFieldAliases);
            UNIT_ASSERT(config.AllowOneofConflicts);
        }
    }

} // Y_UNIT_TEST_SUITE(TJson2ProtoStrictValidation)
