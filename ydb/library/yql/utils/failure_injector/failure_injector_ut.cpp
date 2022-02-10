#include "failure_injector.h"

#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <chrono>

using namespace NYql;
using namespace NYql::NLog;
using namespace std::chrono;

// do nothing
void OnReach(std::atomic<bool>& called) {
    called.store(true);
}

void SetUpLogger() {
    TString logType = "cout";
    NLog::InitLogger(logType, false);
    NLog::EComponentHelpers::ForEach([](NLog::EComponent component) {
        NLog::YqlLogger().SetComponentLevel(component, ELevel::DEBUG);
    });
}

Y_UNIT_TEST_SUITE(TFailureInjectorTests) {
    Y_UNIT_TEST(BasicFailureTest) {
        SetUpLogger();
        std::atomic<bool> called;
        called.store(false);
        auto behavior = [&called] { OnReach(called); };
        TFailureInjector::Reach("misc_failure", behavior);
        UNIT_ASSERT_EQUAL(false, called.load());
        TFailureInjector::Activate();
        TFailureInjector::Set("misc_failure", 0, 1);
        TFailureInjector::Reach("misc_failure", behavior);
        UNIT_ASSERT_EQUAL(true, called.load());
    }

    Y_UNIT_TEST(CheckSkipTest) {
        SetUpLogger();
        std::atomic<bool> called;
        called.store(false);
        auto behavior = [&called] { OnReach(called); };
        TFailureInjector::Activate();
        TFailureInjector::Set("misc_failure", 1, 1);

        TFailureInjector::Reach("misc_failure", behavior);
        UNIT_ASSERT_EQUAL(false, called.load());
        TFailureInjector::Reach("misc_failure", behavior);
        UNIT_ASSERT_EQUAL(true, called.load());
    }

    Y_UNIT_TEST(CheckFailCountTest) {
        SetUpLogger();
        int called = 0;
        auto behavior = [&called] { ++called; };
        TFailureInjector::Activate();
        TFailureInjector::Set("misc_failure", 1, 2);

        TFailureInjector::Reach("misc_failure", behavior);
        UNIT_ASSERT_EQUAL(0, called);
        TFailureInjector::Reach("misc_failure", behavior);
        UNIT_ASSERT_EQUAL(1, called);
        TFailureInjector::Reach("misc_failure", behavior);
        UNIT_ASSERT_EQUAL(2, called);
        TFailureInjector::Reach("misc_failure", behavior);
        UNIT_ASSERT_EQUAL(2, called);
        TFailureInjector::Reach("misc_failure", behavior);
        UNIT_ASSERT_EQUAL(2, called);
    }

    Y_UNIT_TEST(SlowDownTest) {
        SetUpLogger();
        TFailureInjector::Activate();
        TFailureInjector::Set("misc_failure", 0, 1);

        auto start = system_clock::now();
        TFailureInjector::Reach("misc_failure", [] { ::Sleep(TDuration::Seconds(5)); });
        auto finish = system_clock::now();
        auto duration = duration_cast<std::chrono::seconds>(finish - start);
        YQL_LOG(DEBUG) << "Duration :" << duration.count();
        UNIT_ASSERT_GE(duration.count(), 5);
    }
}
