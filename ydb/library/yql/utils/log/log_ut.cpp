#include "log.h"
#include "context.h"
#include "profile.h"
#include <ydb/library/yql/utils/log/ut/log_parser.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/logger/stream.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/system/getpid.h>
#include <util/string/split.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

#include <regex>


using namespace NYql;
using namespace NLog;

Y_UNIT_TEST_SUITE(TLogTest)
{
    Y_UNIT_TEST(Format) {
        TStringStream out;
        YqlLoggerScope logger(&out);
        YqlLogger().UpdateProcInfo("my_proc");

        TString message = "some performance info";
        YQL_LOG(INFO) << message;

        TLogRow logRow = ParseLogRow(out.Str());

        TDuration elapsed(logRow.Time - TInstant::Now());
        UNIT_ASSERT(elapsed < TDuration::MilliSeconds(5));
        UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
        UNIT_ASSERT_STRINGS_EQUAL(logRow.ProcName, "my_proc");
        UNIT_ASSERT_EQUAL(logRow.ProcId, GetPID());
        UNIT_ASSERT(logRow.ThreadId > 0);
        UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
        UNIT_ASSERT_STRINGS_EQUAL(
                    logRow.FileName,
                    TStringBuf(__FILE__).RNextTok(LOCSLASH_C));
        UNIT_ASSERT(logRow.LineNumber != 0);
        UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, message);
    }

    Y_UNIT_TEST(Levels) {
        TStringStream out;
        YqlLoggerScope logger(&out); // default log level INFO

        YQL_LOG(FATAL) << "fatal message";
        YQL_LOG(ERROR) << "error message";
        YQL_LOG(WARN) << "warning message";
        YQL_LOG(INFO) << "info message";
        YQL_LOG(DEBUG) << "debug message";
        YQL_LOG(TRACE) << "trace message";

        TString fatalStr, errorStr, warnStr, infoStr, _;
        Split(out.Str(), '\n', fatalStr, errorStr, warnStr, infoStr, _);

        {
            TLogRow logRow = ParseLogRow(fatalStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::FATAL);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "fatal message");
        }
        {
            TLogRow logRow = ParseLogRow(errorStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::ERROR);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "error message");
        }
        {
            TLogRow logRow = ParseLogRow(warnStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::WARN);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "warning message");
        }
        {
            TLogRow logRow = ParseLogRow(infoStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "info message");
        }
    }

    Y_UNIT_TEST(Components) {
        TStringStream out;
        YqlLoggerScope logger(&out);

        YQL_CLOG(INFO, Default) << "default message";
        YQL_CLOG(INFO, Core) << "core message";
        YQL_CLOG(INFO, Sql) << "sql message";
        YQL_CLOG(INFO, ProviderCommon) << "common message";
        YQL_CLOG(INFO, ProviderYt) << "yt message";
        YQL_CLOG(INFO, ProviderKikimr) << "kikimr message";
        YQL_CLOG(INFO, ProviderRtmr) << "rtmr message";
        YQL_CLOG(INFO, Performance) << "performance message";
        YQL_CLOG(INFO, Perf) << "perf message";

        TString defaultStr, coreStr, sqlStr, commonStr, ytStr,
                kikimrStr, rtmrStr, performanceStr, perfStr, _;
        Split(out.Str(), '\n', defaultStr, coreStr, sqlStr,
              commonStr, ytStr,
              kikimrStr, rtmrStr,
              performanceStr, perfStr, _);

        {
            TLogRow logRow = ParseLogRow(defaultStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "default message");
        }
        {
            TLogRow logRow = ParseLogRow(coreStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Core);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "core message");
        }
        {
            TLogRow logRow = ParseLogRow(sqlStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Sql);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "sql message");
        }
        {
            TLogRow logRow = ParseLogRow(commonStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::ProviderCommon);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "common message");
        }
        {
            TLogRow logRow = ParseLogRow(ytStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::ProviderYt);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "yt message");
        }
        {
            TLogRow logRow = ParseLogRow(kikimrStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::ProviderKikimr);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "kikimr message");
        }
        {
            TLogRow logRow = ParseLogRow(rtmrStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::ProviderRtmr);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "rtmr message");
        }
        {
            TLogRow logRow = ParseLogRow(performanceStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Performance);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Perf);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "performance message");
        }
        {
            TLogRow logRow = ParseLogRow(perfStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Perf);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Performance);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "perf message");
        }
    }

    Y_UNIT_TEST(Conditional) {
        TStringStream out;
        YqlLoggerScope logger(&out);

        YQL_LOG_IF(INFO, true) << "default info message";
        YQL_LOG_IF(INFO, false) << "must not be logged";

        YQL_CLOG_IF(INFO, Perf, true) << "perf info message";
        YQL_CLOG_IF(INFO, Perf, false) << "perf info message";

        TString defaultStr, perfStr, _;
        Split(out.Str(), '\n', defaultStr, perfStr, _);

        {
            TLogRow logRow = ParseLogRow(defaultStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "default info message");
        }
        {
            TLogRow logRow = ParseLogRow(perfStr);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Perf);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "perf info message");
        }
    }

    Y_UNIT_TEST(Contexts) {
        TStringStream out;
        YqlLoggerScope logger(&out);

        UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "");
        YQL_LOG(INFO) << "level0 - begin";
        {
            YQL_LOG_CTX_SCOPE("ctx1");
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx1");
            YQL_LOG(INFO) << "level1 - begin";

            YQL_LOG_CTX_BLOCK(TStringBuf("ctx2")) {
                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx1/ctx2");
                YQL_LOG(WARN) << "level2";
            }

            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx1");
            YQL_LOG(INFO) << "level1 - end";
        }
        UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "");
        YQL_LOG(INFO) << "level0 - end";

        TString row1Str, row2Str, row3Str, row4Str, row5Str, _;
        Split(out.Str(), '\n', row1Str, row2Str, row3Str, row4Str, row5Str, _);

        {
            TLogRow logRow = ParseLogRow(row1Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "level0 - begin");
        }
        {
            TLogRow logRow = ParseLogRow(row2Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx1} level1 - begin");
        }
        {
            TLogRow logRow = ParseLogRow(row3Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::WARN);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx1/ctx2} level2");
        }
        {
            TLogRow logRow = ParseLogRow(row4Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx1} level1 - end");
        }
        {
            TLogRow logRow = ParseLogRow(row5Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "level0 - end");
        }
    }

    Y_UNIT_TEST(UnknownSessionContexts) {
        TStringStream out;
        YqlLoggerScope logger(&out);

        {
            YQL_LOG_CTX_ROOT_SCOPE("ctx");
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "");
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx");
            YQL_LOG(INFO) << "level0 - begin";

            {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(CurrentLogContextPath());
                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "");
                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx");

                YQL_LOG(INFO) << "level1 - begin";
                YQL_LOG_CTX_BLOCK("ctx1") {
                    UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "");
                    UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx/ctx1");

                    YQL_LOG(WARN) << "level2";
                }

                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "");
                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx");
                YQL_LOG(INFO) << "level1 - end";
            }
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "");
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx");
            YQL_LOG(INFO) << "level0 - end";
        }

        TString row1Str, row2Str, row3Str, row4Str, row5Str, _;
        Split(out.Str(), '\n', row1Str, row2Str, row3Str, row4Str, row5Str, _);
        {
            TLogRow logRow = ParseLogRow(row1Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx} level0 - begin");
        }
        {
            TLogRow logRow = ParseLogRow(row2Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx} level1 - begin");
        }
        {
            TLogRow logRow = ParseLogRow(row3Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::WARN);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx/ctx1} level2");
        }
        {
            TLogRow logRow = ParseLogRow(row4Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx} level1 - end");
        }
        {
            TLogRow logRow = ParseLogRow(row5Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx} level0 - end");
        }
    }

    Y_UNIT_TEST(SessionContexts) {
        TStringStream out;
        YqlLoggerScope logger(&out);

        {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE("sessionId", "ctx");
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "sessionId");
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx");
            YQL_LOG(INFO) << "level0 - begin";

            {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(CurrentLogContextPath());
                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "sessionId");
                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx");

                YQL_LOG(INFO) << "level1 - begin";
                YQL_LOG_CTX_BLOCK("ctx1") {
                    UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "sessionId");
                    UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx/ctx1");

                    YQL_LOG(WARN) << "level2";
                }

                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "sessionId");
                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx");
                YQL_LOG(INFO) << "level1 - end";
            }
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().first, "sessionId");
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx");
            YQL_LOG(INFO) << "level0 - end";
        }

        TString row1Str, row2Str, row3Str, row4Str, row5Str, _;
        Split(out.Str(), '\n', row1Str, row2Str, row3Str, row4Str, row5Str, _);
        {
            TLogRow logRow = ParseLogRow(row1Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{sessionId/ctx} level0 - begin");
        }
        {
            TLogRow logRow = ParseLogRow(row2Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{sessionId/ctx} level1 - begin");
        }
        {
            TLogRow logRow = ParseLogRow(row3Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::WARN);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{sessionId/ctx/ctx1} level2");
        }
        {
            TLogRow logRow = ParseLogRow(row4Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{sessionId/ctx} level1 - end");
        }
        {
            TLogRow logRow = ParseLogRow(row5Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{sessionId/ctx} level0 - end");
        }
    }

    Y_UNIT_TEST(ThrowWithContext) {
        bool isThrown = false;
        YQL_LOG_CTX_SCOPE("first");
        try {
            YQL_LOG_CTX_SCOPE("second");
            YQL_LOG_CTX_THROW yexception() << "some message";
        } catch (const yexception& e) {
            isThrown = true;

            UNIT_ASSERT_STRINGS_EQUAL(e.AsStrBuf(), "some message");

            TString throwedLogCtx = ThrowedLogContextPath();
            TStringBuf file, line, context;
            TStringBuf(throwedLogCtx).Split(".cpp:", file, line);
            line.Split(':', line, context);

            TString expectedFile(__LOCATION__.File);
            SubstGlobal(expectedFile, LOCSLASH_C, '/');
            UNIT_ASSERT_STRINGS_EQUAL(TString(file)+".cpp", expectedFile);
            int lineNumber;
            UNIT_ASSERT(TryFromString<int>(line, lineNumber));
            UNIT_ASSERT(lineNumber > 0);
            UNIT_ASSERT_STRINGS_EQUAL(context, " {first/second} ");

            // second call without throw returns empty string
            throwedLogCtx = ThrowedLogContextPath();
            UNIT_ASSERT(throwedLogCtx.empty());
        }

        UNIT_ASSERT_C(isThrown, "exception was not thrown");
    }

    Y_UNIT_TEST(ContextOverride) {
        TStringStream out;
        YqlLoggerScope logger(&out);

        UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "");
        {
            YQL_LOG_CTX_SCOPE("ctx1");
            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx1");
            YQL_LOG(INFO) << "level1 - begin";

            YQL_LOG_CTX_BLOCK(TStringBuf("ctx2")) {
                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx1/ctx2");
                YQL_LOG(WARN) << "level2 - begin";

                {
                    YQL_LOG_CTX_ROOT_SCOPE("ctx3");
                    UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx3");
                    YQL_LOG(ERROR) << "level3";
                }

                UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx1/ctx2");
                YQL_LOG(WARN) << "level2 - end";
            }

            UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "ctx1");
            YQL_LOG(INFO) << "level1 - end";
        }
        UNIT_ASSERT_STRINGS_EQUAL(CurrentLogContextPath().second, "");

        TString row1Str, row2Str, row3Str, row4Str, row5Str, _;
        Split(out.Str(), '\n', row1Str, row2Str, row3Str, row4Str, row5Str, _);

        {
            TLogRow logRow = ParseLogRow(row1Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx1} level1 - begin");
        }
        {
            TLogRow logRow = ParseLogRow(row2Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::WARN);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx1/ctx2} level2 - begin");
        }
        {
            TLogRow logRow = ParseLogRow(row3Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::ERROR);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx3} level3");
        }
        {
            TLogRow logRow = ParseLogRow(row4Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::WARN);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx1/ctx2} level2 - end");
        }
        {
            TLogRow logRow = ParseLogRow(row5Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "{ctx1} level1 - end");
        }
    }

    Y_UNIT_TEST(Profiling) {
        TStringStream out;
        YqlLoggerScope logger(&out);

        {
            YQL_PROFILE_SCOPE(INFO, "scope1");
        }

        YQL_PROFILE_BLOCK(WARN, "block1") {
            Sleep(TDuration::MilliSeconds(2));
        }

        YQL_PROFILE_BLOCK(ERROR, "block2") {
            Sleep(TDuration::MilliSeconds(1200));
        }

        bool isExecuted = false;
        YQL_PROFILE_BLOCK(TRACE, "block3") { // log will be filtered out
            isExecuted = true;
        }
        UNIT_ASSERT(isExecuted);

        TString row1Str, row2Str, row3Str, _;
        Split(out.Str(), '\n', row1Str, row2Str, row3Str, _);

        {
            TLogRow logRow = ParseLogRow(row1Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Perf);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Performance);
            std::regex re("Execution of \\[scope1\\] took [0-9\\.]+us");
            bool isMatch = std::regex_match(logRow.Message.c_str(), re);
            UNIT_ASSERT_C(isMatch, "Unexpected message: " << logRow.Message);
        }
        {
            TLogRow logRow = ParseLogRow(row2Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::WARN);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Perf);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Performance);
            std::regex re("Execution of \\[block1\\] took [0-9\\.]+ms");
            bool isMatch = std::regex_match(logRow.Message.c_str(), re);
            UNIT_ASSERT_C(isMatch, "Unexpected message: " << logRow.Message);
        }
        {
            TLogRow logRow = ParseLogRow(row3Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::ERROR);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Perf);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Performance);
            std::regex re("Execution of \\[block2\\] took [0-9\\.]+s");
            bool isMatch = std::regex_match(logRow.Message.c_str(), re);
            UNIT_ASSERT_C(isMatch, "Unexpected message: " << logRow.Message);
        }
    }


    int Func1(int a, char b) {
        YQL_PROFILE_FUNC(INFO);
        return a + b;
    }

    int Func2(int a, char b) {
        YQL_PROFILE_FUNCSIG(WARN);
        return a + b;
    }

    Y_UNIT_TEST(ProfilingFuncs) {
        TStringStream out;
        YqlLoggerScope logger(&out);

        Func1(1, 2);
        Func2(1, 2);

        TString row1Str, row2Str, _;
        Split(out.Str(), '\n', row1Str, row2Str, _);

        {
            TLogRow logRow = ParseLogRow(row1Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Perf);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Performance);
            std::regex re("Execution of \\[Func1\\] took [0-9\\.]+us");
            bool isMatch = std::regex_match(logRow.Message.c_str(), re);
            UNIT_ASSERT_C(isMatch, "Unexpected message: " << logRow.Message);
        }
        {
            TLogRow logRow = ParseLogRow(row2Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::WARN);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Perf);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Performance);
#ifdef _win_
            std::regex re("Execution of \\[int __cdecl NTestSuiteTLogTest::Func2\\(int, char\\)\\] took [0-9\\.]+us");
#else
            std::regex re("Execution of \\[int NTestSuiteTLogTest::Func2\\(int, char\\)\\] took [0-9\\.]+us");
#endif
            bool isMatch = std::regex_match(logRow.Message.c_str(), re);
            UNIT_ASSERT_C(isMatch, "Unexpected message: " << logRow.Message);
        }
    }

    Y_UNIT_TEST(Limit1) {
        size_t limit = 0;
        {
            TStringStream out;
            YqlLoggerScope logger(&out);
            YqlLogger().UpdateProcInfo("proc");
            YQL_CLOG(INFO, Core) << "message1";
            limit = out.Str().length() * 2 - 7; // Not more than 2 log lines
        }

        TStringStream out;
        YqlLoggerScope logger(&out);
        YqlLogger().UpdateProcInfo("proc");
        YqlLogger().SetMaxLogLimit(limit);

        YQL_CLOG(INFO, Core) << "message1";
        YQL_CLOG(INFO, Core) << "message2";
        YQL_CLOG(INFO, Core) << "message3";

        TString row1Str, row2Str, row3Str, _;
        Split(out.Str(), '\n', row1Str, row2Str, row3Str, _);

        {
            TLogRow logRow = ParseLogRow(row1Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Core);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "message1");
        }
        {
            TLogRow logRow = ParseLogRow(row2Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Core);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "message2");
        }
        {
            TLogRow logRow = ParseLogRow(row3Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::FATAL);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "Log is truncated by limit");
        }
    }

    Y_UNIT_TEST(Limit2) {
        size_t limit = 0;
        {
            TStringStream out;
            YqlLoggerScope logger(&out);
            YqlLogger().UpdateProcInfo("proc");
            YQL_CLOG(INFO, Core) << "message1";
            limit = out.Str().length() * 2 - 7; // Not more than 2 log lines
        }

        TStringStream out;
        YqlLoggerScope logger(&out);
        YqlLogger().UpdateProcInfo("proc");
        YqlLogger().SetMaxLogLimit(limit);

        YQL_CLOG(INFO, Core) << "message1";
        YQL_CLOG(INFO, Core) << "message2";
        YQL_CLOG(WARN, Core) << "message3";

        TString row1Str, row2Str, row3Str, row4Str, _;
        Split(out.Str(), '\n', row1Str, row2Str, row3Str, row4Str, _);

        {
            TLogRow logRow = ParseLogRow(row1Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Core);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "message1");
        }
        {
            TLogRow logRow = ParseLogRow(row2Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::INFO);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Core);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "message2");
        }
        {
            TLogRow logRow = ParseLogRow(row3Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::FATAL);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Default);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "Log is truncated by limit");
        }
        {
            TLogRow logRow = ParseLogRow(row4Str);
            UNIT_ASSERT_EQUAL(logRow.Level, ELevel::WARN);
            UNIT_ASSERT_EQUAL(logRow.Component, EComponent::Core);
            UNIT_ASSERT_STRINGS_EQUAL(logRow.Message, "message3");
        }
    }
}
