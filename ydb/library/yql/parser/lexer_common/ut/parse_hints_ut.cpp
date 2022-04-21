#include <ydb/library/yql/parser/lexer_common/parse_hints_impl.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>

using namespace NSQLTranslation;
using namespace NSQLTranslation::NDetail;

void CheckParse(TStringBuf comment, TStringBuf expected) {
    TString parsed = JoinSeq(",", ParseSqlHints({}, comment));
    UNIT_ASSERT_NO_DIFF(parsed, expected);
}

Y_UNIT_TEST_SUITE(TParseTests) {
    Y_UNIT_TEST(NoPlusInComment) {
        CheckParse("/* foo(bar) */", "");
        CheckParse("-- foo(bar)\n", "");
    }

    Y_UNIT_TEST(Basic) {
        TStringBuf comment = "/*+Foo( Bar 0Baz*) test\nFoo1(Bar1    Bar2)\n Foo2()*/";
        TStringBuf expected = R"raw("Foo":{"Bar","0Baz*"},"Foo1":{"Bar1","Bar2"},"Foo2":{})raw";
        CheckParse(comment, expected);
    }

    Y_UNIT_TEST(Quoted) {
        TStringBuf comment = "/*+Foo('Bar' Baz 'Bar'' quote')*/";
        TStringBuf expected = R"raw("Foo":{"Bar","Baz","Bar' quote"})raw";
        CheckParse(comment, expected);
    }

    Y_UNIT_TEST(MissingSpace) {
        TStringBuf comment = "/*+Foo()Bar(x)*/";
        TStringBuf expected = R"raw("Foo":{})raw";
        CheckParse(comment, expected);
    }
}
