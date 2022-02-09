#include <ydb/library/yql/parser/pg_query_wrapper/wrapper.h>

#include <util/stream/str.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

class TEvents : public IPGParseEvents {
public:
    void OnResult(const PgQuery__ParseResult* result, const List* raw) override {
        Y_UNUSED(raw);
        Result.ConstructInPlace();
        TStringOutput str(*Result);
        PrintCProto((const ProtobufCMessage*)result, str);
    }

    void OnError(const TIssue& issue) override {
        Issue = issue;
    }

    TMaybe<TIssue> Issue;
    TMaybe<TString> Result;
};

Y_UNIT_TEST_SUITE(TWrapperTests) {
    Y_UNIT_TEST(TestOk) {
        TEvents events;
        PGParse(TString("SELECT 1"), events);
        UNIT_ASSERT(events.Result);
        UNIT_ASSERT(!events.Issue);
        const auto expected = R"(version: 130003
stmts: > #1
stmt: > #2
select_stmt: > #3
target_list: > #4
res_target: > #5
val: > #6
a_const: > #7
val: > #8
integer: > #9
ival: 1
< #9
< #8
location: 7
< #7
< #6
location: 7
< #5
< #4
limit_option: LIMIT_OPTION_DEFAULT
op: SETOP_NONE
< #3
< #2
< #1
)";
        UNIT_ASSERT_NO_DIFF(*events.Result, expected);
    }

    Y_UNIT_TEST(TestFail) {
        TEvents events;
        PGParse(TString(" \n  SELECT1"), events);
        UNIT_ASSERT(!events.Result);
        UNIT_ASSERT(events.Issue);
        auto msg = events.Issue->Message;
        UNIT_ASSERT_NO_DIFF(msg, "syntax error at or near \"SELECT1\"");
        UNIT_ASSERT_VALUES_EQUAL(events.Issue->Position.Row, 2);
        UNIT_ASSERT_VALUES_EQUAL(events.Issue->Position.Column, 3);
    }
}
