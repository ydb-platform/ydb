#include "init.h"

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/support/log.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <latch>
#include <optional>
#include <thread>

namespace NYdb::NBS::NGrpc {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestLogCounters
{
    std::atomic_int AliveLoggers;
    std::atomic_int Writes;
};

struct TTestLogBackend: public TLogBackend
{
    TTestLogCounters& Counters;

    explicit TTestLogBackend(TTestLogCounters& counters)
        : Counters(counters)
    {
        ++Counters.AliveLoggers;
    }

    ~TTestLogBackend() override
    {
        --Counters.AliveLoggers;
    }

    [[nodiscard]] ELogPriority FiltrationLevel() const override
    {
        return TLOG_DEBUG;
    }

    void WriteData(const TLogRecord& rec) override
    {
        Y_UNUSED(rec);
        ++Counters.Writes;
    }

    void ReopenLog() override
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : NUnitTest::TBaseFixture
    , TTestLogCounters
{
    void SetTestGrpcLogger()
    {
        GrpcLoggerInit(TLog{MakeHolder<TTestLogBackend>(*this)}, true);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInitTest)
{
    Y_UNIT_TEST_F(ShouldIgnoreSubsequentInitializations, TFixture)
    {
        std::optional<TGrpcInitializer> init{std::in_place};

        UNIT_ASSERT_VALUES_EQUAL(0, AliveLoggers.load());
        SetTestGrpcLogger();
        UNIT_ASSERT_VALUES_EQUAL(1, AliveLoggers.load());

        constexpr int n = 10;
        for (int i = 0; i != n; ++i) {
            SetTestGrpcLogger();
        }
        UNIT_ASSERT_VALUES_EQUAL(1, AliveLoggers.load());

        init.reset();
        UNIT_ASSERT_VALUES_EQUAL(0, AliveLoggers.load());
    }

    Y_UNIT_TEST_F(ShouldDestroyLogger, TFixture)
    {
        std::optional<TGrpcInitializer> init1;
        std::optional<TGrpcInitializer> init2;

        UNIT_ASSERT_VALUES_EQUAL(0, AliveLoggers.load());
        UNIT_ASSERT_VALUES_EQUAL(0, Writes.load());

        gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);

        // Initialize GRPC
        init1.emplace();

        gpr_log("ut", 1, GPR_LOG_SEVERITY_INFO, "default logger");

        UNIT_ASSERT_VALUES_EQUAL(0, Writes.load());

        // Setup logger
        SetTestGrpcLogger();

        gpr_log("ut", 2, GPR_LOG_SEVERITY_INFO, "custom logger");

        UNIT_ASSERT_VALUES_EQUAL(1, AliveLoggers.load());
        UNIT_ASSERT_LT(0, Writes.load());

        // GRPC has already been initialized - just increment internal refcount.
        init2.emplace();

        UNIT_ASSERT_VALUES_EQUAL(1, AliveLoggers.load());

        // Decrement internal refcount, GRPC still running.
        init1.reset();

        UNIT_ASSERT_VALUES_EQUAL(1, AliveLoggers.load());

        gpr_log("ut", 3, GPR_LOG_SEVERITY_INFO, "custom logger");

        // Shutdown GRPC
        init2.reset();

        // Custom GRPC logger shoud be destroyed.
        UNIT_ASSERT_VALUES_EQUAL(0, AliveLoggers.load());

        // GRPC should use the default logger (even after deinitialization).

        const int expectedWrites = Writes.load();

        gpr_log("ut", 4, GPR_LOG_SEVERITY_INFO, "default logger again");

        UNIT_ASSERT_VALUES_EQUAL(expectedWrites, Writes.load());
    }

    Y_UNIT_TEST_F(ShouldDestroyLoggerInMultiThreadEnvironment, TFixture)
    {
        constexpr int threadCount = 10;
        std::latch start{threadCount + 1};
        std::latch execute{threadCount + 1};
        std::latch stop{threadCount};

        TVector<std::thread> threads;
        threads.reserve(threadCount);

        for (int i = 0; i != threadCount; ++i) {
            threads.emplace_back(
                [&execute, &start, &stop, i, this]
                {
                    TGrpcInitializer grpcInitializer;

                    SetTestGrpcLogger();

                    start.arrive_and_wait();
                    execute.arrive_and_wait();

                    gpr_log("ut", i, GPR_LOG_SEVERITY_INFO, "custom logger");

                    stop.arrive_and_wait();
                });
        }

        start.arrive_and_wait();

        UNIT_ASSERT_VALUES_EQUAL(1, AliveLoggers.load());

        execute.arrive_and_wait();

        for (auto& t: threads) {
            t.join();
        }

        UNIT_ASSERT_VALUES_EQUAL(0, AliveLoggers.load());
        UNIT_ASSERT_LE(threadCount, Writes.load());
    }
}

}   // namespace NYdb::NBS::NGrpc
