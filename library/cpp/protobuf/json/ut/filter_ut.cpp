#include <library/cpp/protobuf/json/ut/filter_ut.pb.h>

#include <library/cpp/protobuf/json/filter.h>
#include <library/cpp/protobuf/json/field_option.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NProtobufJson;

static NProtobufJsonUt::TFilterTest GetTestMsg() {
    NProtobufJsonUt::TFilterTest msg;
    msg.SetOptFiltered("1");
    msg.SetNotFiltered("23");
    msg.AddRepFiltered(45);
    msg.AddRepFiltered(67);
    msg.MutableInner()->AddNumber(100);
    msg.MutableInner()->AddNumber(200);
    msg.MutableInner()->SetInnerFiltered(235);
    return msg;
}

Y_UNIT_TEST_SUITE(TProto2JsonFilterTest){
    Y_UNIT_TEST(TestFilterPrinter){
        NProtobufJsonUt::TFilterTest msg = GetTestMsg();
{
    TString expected = R"({"OptFiltered":"1","NotFiltered":"23","RepFiltered":[45,67],)"
                       R"("Inner":{"Number":[100,200],"InnerFiltered":235}})";
    TString my = Proto2Json(msg);
    UNIT_ASSERT_STRINGS_EQUAL(my, expected);
}

{
    TString expected = R"({"NotFiltered":"23",)"
                       R"("Inner":{"Number":[100,200]}})";
    TString my = PrintWithFilter(msg, MakeFieldOptionFunctor(NProtobufJsonUt::filter_test, false));
    UNIT_ASSERT_STRINGS_EQUAL(my, expected);
}

{
    TString expected = R"({"OptFiltered":"1","RepFiltered":[45,67]})";
    TString my = PrintWithFilter(msg, MakeFieldOptionFunctor(NProtobufJsonUt::filter_test));
    UNIT_ASSERT_STRINGS_EQUAL(my, expected);
}

{
    TString expected = R"({"OptFiltered":"1","NotFiltered":"23",)"
                       R"("Inner":{"Number":[100,200]}})";
    TString my;
    PrintWithFilter(msg, MakeFieldOptionFunctor(NProtobufJsonUt::export_test), *CreateJsonMapOutput(my));
    UNIT_ASSERT_STRINGS_EQUAL(my, expected);
}

{
    TString expected = R"({"NotFiltered":"23",)"
                       R"("Inner":{"Number":[100,200]}})";
    auto functor = [](const NProtoBuf::Message&, const NProtoBuf::FieldDescriptor* field) {
        return field->name() == "NotFiltered" || field->name() == "Number" || field->name() == "Inner";
    };
    TString my = PrintWithFilter(msg, functor);
    UNIT_ASSERT_STRINGS_EQUAL(my, expected);
}
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
    TFilteringPrinter<> printer(TFunctorMock(&CopyCount), cfg);
    UNIT_ASSERT(CopyCount <= 1);
}
}
;
