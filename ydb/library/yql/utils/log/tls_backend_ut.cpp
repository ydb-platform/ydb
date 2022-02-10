#include "tls_backend.h"
#include "log.h"
#include <ydb/library/yql/utils/log/ut/log_parser.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/logger/stream.h>
#include <library/cpp/logger/null.h>

#include <util/system/thread.h>
#include <util/string/split.h>

#include <thread>
#include <chrono>


using namespace NYql;
using namespace NLog;

class TRunnable {
public:
    TRunnable(TStringBuf name, int count)
        : Name_(name)
        , Count_(count)
    {
    }

    void operator()() {
        using namespace std::chrono_literals;

        YQL_LOG(INFO) << "this message will be missed";
        {
            TScopedBackend<TStreamLogBackend> logBackend(&Logs_);
            for (int i = 0; i < Count_; i++) {
                YQL_LOG(INFO) << Name_;
                std::this_thread::sleep_for(20ms);
            }
        }
        YQL_LOG(INFO) << "this message will be missed";
    }

    const TString& GetLogs() const {
        return Logs_.Str();
    }

private:
    TString Name_;
    int Count_;
    TStringStream Logs_;
};

Y_UNIT_TEST_SUITE(TTlsLogBackendTest)
{
    Y_UNIT_TEST(CaptureOutputs) {
        YqlLoggerScope logger(new TTlsLogBackend(new TNullLogBackend));

        YQL_LOG(INFO) << "this message will be missed";

        TRunnable r1("t1", 3);
        std::thread t1(std::ref(r1));

        TRunnable r2("t2", 2);
        std::thread t2(std::ref(r2));

        t1.join();
        t2.join();

//        Cout << "--[t1 logs]-----------------\n" << r1.GetLogs() << Endl;
//        Cout << "--[t2 logs]-----------------\n" << r2.GetLogs() << Endl;

        { // t1
            TString row1Str, row2Str, row3Str, _;
            Split(r1.GetLogs(), '\n', row1Str, row2Str, row3Str, _);

            ui64 threadId = 0;
            {
                TLogRow logRow = ParseLogRow(row1Str);
                UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
                UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
                UNIT_ASSERT(logRow.ThreadId > 0);
                UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "t1");
                threadId = logRow.ThreadId;
            }
            {
                TLogRow logRow = ParseLogRow(row2Str);
                UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
                UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
                UNIT_ASSERT_EQUAL(logRow.ThreadId, threadId);
                UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "t1");
            }
            {
                TLogRow logRow = ParseLogRow(row3Str);
                UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
                UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
                UNIT_ASSERT_EQUAL(logRow.ThreadId, threadId);
                UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "t1");
            }
        }

        { // t2
            TString row1Str, row2Str, _;
            Split(r2.GetLogs(), '\n', row1Str, row2Str, _);

            ui64 threadId = 0;
            {
                TLogRow logRow = ParseLogRow(row1Str);
                UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
                UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
                UNIT_ASSERT(logRow.ThreadId > 0);
                UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "t2");
                threadId = logRow.ThreadId;
            }
            {
                TLogRow logRow = ParseLogRow(row2Str);
                UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
                UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
                UNIT_ASSERT_EQUAL(logRow.ThreadId, threadId);
                UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "t2");
            }
        }
    }
}
