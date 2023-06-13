#include <library/cpp/scheme/scimpl_private.h>
#include <library/cpp/scheme/ut_utils/scheme_ut_utils.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>
#include <util/string/subst.h>
#include <util/string/util.h>

#include <type_traits>
#include <library/cpp/string_utils/quote/quote.h>

Y_UNIT_TEST_SUITE(TSchemePathTest) {
    void DoTestSelect(TStringBuf path, TStringBuf expected, TStringBuf delexpected) {
        NSc::TValue v;
        UNIT_ASSERT(!v.PathExists(path));
        UNIT_ASSERT(NSc::TValue::PathValid(path));
        UNIT_ASSERT(NSc::TValue::Same(v.TrySelect(path), NSc::Null()));
        *v.TrySelectOrAdd(path) = 1;
        NSc::NUt::AssertSchemeJson(expected, v);
        UNIT_ASSERT(v.PathExists(path));
        UNIT_ASSERT(1 == v.TrySelectOrAdd(path)->GetNumber());
        UNIT_ASSERT(1 == v.TrySelect(path).GetNumber());
        UNIT_ASSERT(1 == v.TrySelectAndDelete(path).GetNumber());
        UNIT_ASSERT(NSc::TValue::Same(v.TrySelectAndDelete(path), NSc::Null()));
        NSc::NUt::AssertSchemeJson(delexpected, v);
        UNIT_ASSERT(!v.PathExists(path));
        UNIT_ASSERT(NSc::TValue::Same(v.TrySelect(path), NSc::Null()));
    }

    Y_UNIT_TEST(TestSelect) {
        NSc::TValue v;
        UNIT_ASSERT(!v.PathValid(" "));
        UNIT_ASSERT(v.PathExists(""));
        UNIT_ASSERT(v.PathExists("//"));

        UNIT_ASSERT(NSc::TValue::Same(v, *v.TrySelectOrAdd("//")));
        NSc::NUt::AssertSchemeJson("null", v);
        UNIT_ASSERT(NSc::TValue::Same(v.TrySelectAndDelete("//"), NSc::Null()));
        NSc::NUt::AssertSchemeJson("null", v);

        v.SetDict();
        UNIT_ASSERT(NSc::TValue::Same(v, *v.TrySelectOrAdd("//")));
        NSc::NUt::AssertSchemeJson("{}", v);
        UNIT_ASSERT(NSc::TValue::Same(v.TrySelectAndDelete("//"), NSc::Null()));
        NSc::NUt::AssertSchemeJson("{}", v);

        v.SetArray();
        UNIT_ASSERT(NSc::TValue::Same(v, *v.TrySelectOrAdd("//")));
        NSc::NUt::AssertSchemeJson("[]", v);
        UNIT_ASSERT(NSc::TValue::Same(v.TrySelectAndDelete("//"), NSc::Null()));
        NSc::NUt::AssertSchemeJson("[]", v);

        DoTestSelect("[]", "{'':1}", "{}");
        DoTestSelect("[ ]", "{' ':1}", "{}");
        DoTestSelect("[0]", "[1]", "[]");
        DoTestSelect("[1]", "[null,1]", "[null]");
        DoTestSelect("foo/[0]/bar", "{foo:[{bar:1}]}", "{foo:[{}]}");
        DoTestSelect("foo/1/bar", "{foo:[null,{bar:1}]}", "{foo:[null,{}]}");
        DoTestSelect("foo[-1]bar", "{foo:{'-1':{bar:1}}}", "{foo:{'-1':{}}}");
        DoTestSelect("'foo'/\"0\"/'bar'", "{foo:{'0':{bar:1}}}", "{foo:{'0':{}}}");
        DoTestSelect("'\\''", "{'\\'':1}", "{}");
    }

    Y_UNIT_TEST(TestSelectAndMerge) {
        NSc::TValue v;
        v.TrySelectOrAdd("blender/enabled")->MergeUpdateJson("1");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::FromJson("1").ToJson(), "1");
        UNIT_ASSERT_VALUES_EQUAL(v.ToJson(), "{\"blender\":{\"enabled\":1}}");
    }

    Y_UNIT_TEST(TestPathEscapes) {
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("a"), "a");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath(""), R"=("")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("[]"), R"=("[]")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("[]ab"), R"=("[]ab")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("a[]b"), R"=("a[]b")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("ab[]"), R"=("ab[]")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("[ab]"), R"=("[ab]")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("[ab"), R"=("[ab")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("a[b"), R"=("a[b")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("ab["), R"=("ab[")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("]ab"), R"=("]ab")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("a]b"), R"=("a]b")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("ab]"), R"=("ab]")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath(R"=(\)="), R"=("\\")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath(R"=(\\)="), R"=("\\\\")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("/"), R"=("/")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("//"), R"=("//")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("///"), R"=("///")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("/ab"), R"=("/ab")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("a/b"), R"=("a/b")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("ab/"), R"=("ab/")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("//ab"), R"=("//ab")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("a//b"), R"=("a//b")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("ab//"), R"=("ab//")=");
        UNIT_ASSERT_VALUES_EQUAL(NSc::TValue::EscapeForPath("6400"), R"=("6400")=");

        {
            NSc::TValue val;
            *val.TrySelectOrAdd("") = 100;
            const TString res = R"=(100)=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
        {
            NSc::TValue val;
            *val.TrySelectOrAdd("a") = 100;
            const TString res = R"=({"a":100})=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
        {
            NSc::TValue val;
            *val.TrySelectOrAdd(R"=(////)=") = 100;
            const TString res = R"=(100)=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
        {
            NSc::TValue val;
            *val.TrySelectOrAdd(R"=()=") = 100;
            const TString res = R"=(100)=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
        {
            NSc::TValue val;
            *val.TrySelectOrAdd(R"=("")=") = 100;
            const TString res = R"=({"":100})=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
        {
            NSc::TValue val;
            *val.TrySelectOrAdd(R"=("[1]")=") = 100;
            const TString res = R"=({"[1]":100})=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
        {
            NSc::TValue val;
            *val.TrySelectOrAdd(R"=("\"\"")=") = 100;
            const TString res = R"=({"\"\"":100})=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
        {
            NSc::TValue val;
            *val.TrySelectOrAdd(R"=("/10/")=") = 100;
            const TString res = R"=({"/10/":100})=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
        {
            NSc::TValue val;
            *val.TrySelectOrAdd(R"=(/"[10]"//""/"\"/10/\""///)=") = 100;
            const TString res = R"=({"[10]":{"":{"\"/10/\"":100}}})=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
        {
            NSc::TValue val;
            *val.TrySelectOrAdd(R"=(/"[10]"//""/"\"/10/\""///)=") = 100;
            const TString res = R"=({"[10]":{"":{"\"/10/\"":100}}})=";
            UNIT_ASSERT_VALUES_EQUAL(val.ToJson(), res);
        }
    }
}
