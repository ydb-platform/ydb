#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/json/json_reader.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/common/helpers.h>

using namespace NYT;

template<>
void Out<NYT::TNode>(IOutputStream& s, const NYT::TNode& node)
{
    s << "TNode:" << NodeToYsonString(node);
}

Y_UNIT_TEST_SUITE(ErrorSuite)
{
    Y_UNIT_TEST(TestParseJson)
    {
        // Scary real world error! Бу!
        const char* jsonText =
            R"""({)"""
                R"""("code":500,)"""
                R"""("message":"Error resolving path //home/user/link",)"""
                R"""("attributes":{)"""
                    R"""("fid":18446484571700269066,)"""
                    R"""("method":"Create",)"""
                    R"""("tid":17558639495721339338,)"""
                    R"""("datetime":"2017-04-07T13:38:56.474819Z",)"""
                    R"""("pid":414529,)"""
                    R"""("host":"build01-01g.yt.yandex.net"},)"""
                R"""("inner_errors":[{)"""
                    R"""("code":1,)"""
                    R"""("message":"Node //tt cannot have children",)"""
                    R"""("attributes":{)"""
                        R"""("fid":18446484571700269066,)"""
                        R"""("tid":17558639495721339338,)"""
                        R"""("datetime":"2017-04-07T13:38:56.474725Z",)"""
                        R"""("pid":414529,)"""
                        R"""("host":"build01-01g.yt.yandex.net"},)"""
                    R"""("inner_errors":[]}]})""";

        NJson::TJsonValue jsonValue;
        ReadJsonFastTree(jsonText, &jsonValue, /*throwOnError=*/ true);

        TYtError error(jsonValue);
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), 500);
        UNIT_ASSERT_VALUES_EQUAL(error.GetMessage(), R"""(Error resolving path //home/user/link)""");
        UNIT_ASSERT_VALUES_EQUAL(error.InnerErrors().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(error.InnerErrors()[0].GetCode(), 1);

        UNIT_ASSERT_VALUES_EQUAL(error.HasAttributes(), true);
        UNIT_ASSERT_VALUES_EQUAL(error.GetAttributes().at("method"), TNode("Create"));

        UNIT_ASSERT_VALUES_EQUAL(error.GetAllErrorCodes(), TSet<int>({500, 1}));
    }

    Y_UNIT_TEST(TestGetYsonText) {
        const char* jsonText =
            R"""({)"""
                R"""("code":500,)"""
                R"""("message":"outer error",)"""
                R"""("attributes":{)"""
                    R"""("method":"Create",)"""
                    R"""("pid":414529},)"""
                R"""("inner_errors":[{)"""
                    R"""("code":1,)"""
                    R"""("message":"inner error",)"""
                    R"""("attributes":{},)"""
                    R"""("inner_errors":[])"""
                R"""(}]})""";
        TYtError error;
        error.ParseFrom(jsonText);
        TString ysonText = error.GetYsonText();
        TYtError error2(NodeFromYsonString(ysonText));
        UNIT_ASSERT_EQUAL(
            ysonText,
            R"""({"code"=500;"message"="outer error";"attributes"={"method"="Create";"pid"=414529};"inner_errors"=[{"code"=1;"message"="inner error"}]})""");
        UNIT_ASSERT_EQUAL(error2.GetYsonText(), ysonText);
    }
}
