#include <ydb/library/yql/parser/pg_wrapper/interface/raw_parser.h>

#include <util/stream/str.h>
#include <util/system/thread.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

class TEvents : public IPGParseEvents {
public:
    void OnResult(const List* raw) override {
        Result = PrintPGTree(raw);
    }

    void OnError(const TIssue& issue) override {
        Issue = issue;
    }

    TMaybe<TIssue> Issue;
    TMaybe<TString> Result;
};

const TStringBuf ExpectedSelect1 = "({RAWSTMT :stmt {SELECTSTMT :distinctClause <> :intoClause <> :targetList "
    "({RESTARGET :name <> :indirection <> :val {A_CONST :val 1 :location 7} :location 7}) :fromClause <> :whereClause "
    "<> :groupClause <> :groupDistinct false :havingClause <> :windowClause <> :valuesLists <> :sortClause <>"
    " :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>}"
    " :stmt_location 0 :stmt_len 0})";

const TString Error1 = "ERROR:  syntax error at or near \"SELECT1\"\n";

Y_UNIT_TEST_SUITE(TWrapperTests) {
    Y_UNIT_TEST(TestOk) {
        TEvents events;
        PGParse(TString("SELECT 1"), events);
        UNIT_ASSERT(events.Result);
        UNIT_ASSERT(!events.Issue);
        UNIT_ASSERT_NO_DIFF(*events.Result, ExpectedSelect1);
    }

    Y_UNIT_TEST(TestFail) {
        TEvents events;
        PGParse(TString(" \n  SELECT1"), events);
        UNIT_ASSERT(!events.Result);
        UNIT_ASSERT(events.Issue);
        auto msg = events.Issue->GetMessage();
        UNIT_ASSERT_NO_DIFF(msg, Error1);
        UNIT_ASSERT_VALUES_EQUAL(events.Issue->Position.Row, 2);
        UNIT_ASSERT_VALUES_EQUAL(events.Issue->Position.Column, 3);
    }
}

const ui32 threadsCount = 10;

Y_UNIT_TEST_SUITE(TMTWrapperTests) {
    Y_UNIT_TEST(TestOk) {
        TVector<THolder<TThread>> threads;
        for (ui32 i = 0; i < threadsCount; ++i) {
            threads.emplace_back(MakeHolder<TThread>([]() {
                ui32 iters = 10000;
#if defined(_san_enabled_)
                iters /= 100;
#endif
                for (ui32 i = 0; i < iters; ++i) {
                    TEvents events;
                    PGParse(TString("SELECT 1"), events);
                    Y_ENSURE(events.Result);
                    Y_ENSURE(!events.Issue);
                    Y_ENSURE(*events.Result == ExpectedSelect1);
                }
            }));
        }

        for (ui32 i = 0; i < threadsCount; ++i) {
            threads[i]->Start();
        }

        for (ui32 i = 0; i < threadsCount; ++i) {
            threads[i]->Join();
        }
    }

    Y_UNIT_TEST(TestFail) {
        TVector<THolder<TThread>> threads;
        for (ui32 i = 0; i < threadsCount; ++i) {
            threads.emplace_back(MakeHolder<TThread>([]() {
                ui32 iters = 10000;
#if defined(_san_enabled_)
                iters /= 100;
#endif
                for (ui32 i = 0; i < iters; ++i) {
                    TEvents events;
                    PGParse(TString(" \n  SELECT1"), events);
                    Y_ENSURE(!events.Result);
                    Y_ENSURE(events.Issue);
                    auto msg = events.Issue->GetMessage();
                    Y_ENSURE(msg == Error1);
                    Y_ENSURE(events.Issue->Position.Row == 2);
                    Y_ENSURE(events.Issue->Position.Column == 3);
                }
            }));
        }

        for (ui32 i = 0; i < threadsCount; ++i) {
            threads[i]->Start();
        }

        for (ui32 i = 0; i < threadsCount; ++i) {
            threads[i]->Join();
        }
    }
}
