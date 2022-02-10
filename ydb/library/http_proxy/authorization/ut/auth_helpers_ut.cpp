#include <ydb/library/http_proxy/authorization/auth_helpers.h>

#include <ydb/library/http_proxy/error/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {

Y_UNIT_TEST_SUITE(AuthorizationParsingTest) {
    void CheckParam(TStringBuf headerValue, const TString& name, const TString& value) {
        TMap<TString, TString> kv;
        UNIT_ASSERT_NO_EXCEPTION_C(kv = ParseAuthorizationParams(headerValue), "Exception while parsing header \"" << headerValue << "\"");
        UNIT_ASSERT(IsIn(kv, name));
        UNIT_ASSERT_STRINGS_EQUAL(kv[name], value);
    }

    Y_UNIT_TEST(ParsesUsualCredential) {
        CheckParam("x credential=a", "credential", "a");
        CheckParam("x ,credential=b", "credential", "b");
        CheckParam("x ,, credential=c,", "credential", "c");
        CheckParam("x credential=d,,,", "credential", "d");
        CheckParam("x   credential=e,, ,", "credential", "e");
    }

    Y_UNIT_TEST(ParsesManyKV) {
        CheckParam("type k=v, x=y,", "k", "v");
        CheckParam("type k=v, x=y,", "x", "y");
    }

    Y_UNIT_TEST(LowersKey) {
        CheckParam("type Key=Value", "key", "Value");
    }

    Y_UNIT_TEST(FailsOnInvalidStrings) {
        const TStringBuf badStrings[] = {
            "",
            "type",
            "type key",
            "type =value",
            "type =",
        };

        for (TStringBuf h : badStrings) {
            UNIT_ASSERT_EXCEPTION_C(ParseAuthorizationParams(h), TSQSException, "Exception is expected while parsing header \"" << h << "\"");
        }
    }
}

} // namespace NKikimr::NSQS
