#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/json/json_reader.h>

#include <yt/cpp/mapreduce/interface/errors.h>

#include <yt/cpp/mapreduce/common/helpers.h>

#include <util/generic/set.h>

using namespace NYT;

template<>
void Out<NYT::TNode>(IOutputStream& s, const NYT::TNode& node)
{
    s << "TNode:" << NodeToYsonString(node);
}

TEST(TErrorTest, ParseJson)
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
    EXPECT_EQ(error.GetCode(), 500);
    EXPECT_EQ(error.GetMessage(), R"""(Error resolving path //home/user/link)""");
    EXPECT_EQ(error.InnerErrors().size(), 1u);
    EXPECT_EQ(error.InnerErrors()[0].GetCode(), 1);

    EXPECT_EQ(error.HasAttributes(), true);
    EXPECT_EQ(error.GetAttributes().at("method"), TNode("Create"));

    EXPECT_EQ(error.GetAllErrorCodes(), TSet<int>({500, 1}));
}

TEST(TErrorTest, GetYsonText) {
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
    EXPECT_EQ(
        ysonText,
        R"""({"code"=500;"message"="outer error";"attributes"={"method"="Create";"pid"=414529};"inner_errors"=[{"code"=1;"message"="inner error"}]})""");
    EXPECT_EQ(error2.GetYsonText(), ysonText);
}
