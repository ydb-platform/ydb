#include <library/cpp/protobuf/json/ut/inline_ut.pb.h>

#include <library/cpp/protobuf/json/inline.h>
#include <library/cpp/protobuf/json/field_option.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

using namespace NProtobufJson;

static NProtobufJsonUt::TInlineTest GetTestMsg() {
    NProtobufJsonUt::TInlineTest msg;
    msg.SetOptJson(R"({"a":1,"b":"000"})");
    msg.SetNotJson("12{}34");
    msg.AddRepJson("{}");
    msg.AddRepJson("[1,2]");
    msg.MutableInner()->AddNumber(100);
    msg.MutableInner()->AddNumber(200);
    msg.MutableInner()->SetInnerJson(R"({"xxx":[]})");
    return msg;
}

Y_UNIT_TEST_SUITE(TProto2JsonInlineTest){
    Y_UNIT_TEST(TestNormalPrint){
        NProtobufJsonUt::TInlineTest msg = GetTestMsg();
// normal print should output these fields as just string values
TString expRaw = R"({"OptJson":"{\"a\":1,\"b\":\"000\"}","NotJson":"12{}34","RepJson":["{}","[1,2]"],)"
                 R"("Inner":{"Number":[100,200],"InnerJson":"{\"xxx\":[]}"}})";
TString myRaw;
Proto2Json(msg, myRaw);
UNIT_ASSERT_STRINGS_EQUAL(myRaw, expRaw);

myRaw = PrintInlined(msg, [](const NProtoBuf::Message&, const NProtoBuf::FieldDescriptor*) { return false; });
UNIT_ASSERT_STRINGS_EQUAL(myRaw, expRaw); // result is the same
}

Y_UNIT_TEST(TestInliningPrinter) {
    NProtobufJsonUt::TInlineTest msg = GetTestMsg();
    // inlined print should output these fields as inlined json sub-objects
    TString expInlined = R"({"OptJson":{"a":1,"b":"000"},"NotJson":"12{}34","RepJson":[{},[1,2]],)"
                         R"("Inner":{"Number":[100,200],"InnerJson":{"xxx":[]}}})";

    {
        TString myInlined = PrintInlined(msg, MakeFieldOptionFunctor(NProtobufJsonUt::inline_test));
        UNIT_ASSERT_STRINGS_EQUAL(myInlined, expInlined);
    }
    {
        auto functor = [](const NProtoBuf::Message&, const NProtoBuf::FieldDescriptor* field) {
            return field->name() == "OptJson" || field->name() == "RepJson" || field->name() == "InnerJson";
        };
        TString myInlined = PrintInlined(msg, functor);
        UNIT_ASSERT_STRINGS_EQUAL(myInlined, expInlined);
    }
}

Y_UNIT_TEST(TestNoValues) {
    // no values - no printing
    NProtobufJsonUt::TInlineTest msg;
    msg.MutableInner()->AddNumber(100);
    msg.MutableInner()->AddNumber(200);

    TString expInlined = R"({"Inner":{"Number":[100,200]}})";

    TString myInlined = PrintInlined(msg, MakeFieldOptionFunctor(NProtobufJsonUt::inline_test));
    UNIT_ASSERT_STRINGS_EQUAL(myInlined, expInlined);
}

Y_UNIT_TEST(TestMissingKeyModeNull) {
    NProtobufJsonUt::TInlineTest msg;
    msg.MutableInner()->AddNumber(100);
    msg.MutableInner()->AddNumber(200);

    TString expInlined = R"({"OptJson":null,"NotJson":null,"RepJson":null,"Inner":{"Number":[100,200],"InnerJson":null}})";

    TProto2JsonConfig cfg;
    cfg.SetMissingSingleKeyMode(TProto2JsonConfig::MissingKeyNull).SetMissingRepeatedKeyMode(TProto2JsonConfig::MissingKeyNull);
    TString myInlined = PrintInlined(msg, MakeFieldOptionFunctor(NProtobufJsonUt::inline_test), cfg);
    UNIT_ASSERT_STRINGS_EQUAL(myInlined, expInlined);
}

Y_UNIT_TEST(TestMissingKeyModeDefault) {
    NProtobufJsonUt::TInlineTestDefaultValues msg;

    TString expInlined = R"({"OptJson":{"default":1},"Number":0,"RepJson":[],"Inner":{"OptJson":{"default":2}}})";

    TProto2JsonConfig cfg;
    cfg.SetMissingSingleKeyMode(TProto2JsonConfig::MissingKeyDefault).SetMissingRepeatedKeyMode(TProto2JsonConfig::MissingKeyDefault);
    TString myInlined = PrintInlined(msg, MakeFieldOptionFunctor(NProtobufJsonUt::inline_test), cfg);
    UNIT_ASSERT_STRINGS_EQUAL(myInlined, expInlined);
}

Y_UNIT_TEST(NoUnnecessaryCopyFunctor) {
    size_t CopyCount = 0;
    struct TFunctorMock {
        TFunctorMock(size_t* copyCount)
            : CopyCount(copyCount)
        {
            UNIT_ASSERT(*CopyCount <= 1);
        }

        TFunctorMock(const TFunctorMock& f)
            : CopyCount(f.CopyCount)
        {
            ++*CopyCount;
        }

        TFunctorMock(TFunctorMock&& f) = default;

        bool operator()(const NProtoBuf::Message&, const NProtoBuf::FieldDescriptor*) const {
            return false;
        }

        size_t* CopyCount;
    };

    TProto2JsonConfig cfg;
    TInliningPrinter<> printer(TFunctorMock(&CopyCount), cfg);
    UNIT_ASSERT(CopyCount <= 1);
}
}
;
