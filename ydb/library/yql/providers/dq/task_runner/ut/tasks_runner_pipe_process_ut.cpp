#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe_process.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>
#include <util/system/thread.h>

#include <atomic>
#include <cerrno>
#include <csignal>

#include <sys/wait.h>
#include <unistd.h>

namespace NYql::NTaskRunnerProxy::NPrivate {

Y_UNIT_TEST_SUITE(TPipeChildProcessTest) {
    Y_UNIT_TEST(InvalidPidDoesNotReachSyscalls)
    {
        for (int pid : {-1, 0}) {
            int calls = 0;
            TChildProcessState process(
                [&] (int, int) { ++calls; return 0; },
                [&] (int, int* status, int) { ++calls; *status = 0; return -1; });
            process.SetPid(pid);
            process.Kill();
            UNIT_ASSERT_VALUES_EQUAL(calls, 0);
            UNIT_ASSERT(!process.IsAlive());
            UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), -1);
            UNIT_ASSERT_VALUES_EQUAL(calls, 0);
        }
    }

    Y_UNIT_TEST(PollPreservesExitStatusAndRetiresPid)
    {
        int kills = 0;
        int waits = 0;
        TChildProcessState process(
            [&] (int, int) { ++kills; return 0; },
            [&] (int pid, int* status, int options) {
                ++waits;
                UNIT_ASSERT_VALUES_EQUAL(options, WNOHANG);
                *status = waits == 1 ? 7 << 8 : 0;
                return pid;
            });
        process.SetPid(42);
        UNIT_ASSERT(!process.IsAlive());
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), 7 << 8);
        process.Kill();
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), 7 << 8);
        UNIT_ASSERT(!process.IsAlive());
        UNIT_ASSERT_VALUES_EQUAL(kills, 0);
        UNIT_ASSERT_VALUES_EQUAL(waits, 1);
    }

    Y_UNIT_TEST(WaitRetriesInterruptionsAndCachesExitStatus)
    {
        int kills = 0;
        int waits = 0;
        TChildProcessState process(
            [&] (int, int) { ++kills; return 0; },
            [&] (int pid, int* status, int) {
                *status = 0;
                if (++waits == 1) {
                    errno = EINTR;
                    return -1;
                }
                *status = 9 << 8;
                return pid;
            });
        process.SetPid(42);
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Seconds(1)), 9 << 8);
        UNIT_ASSERT_VALUES_EQUAL(kills, 0);
        UNIT_ASSERT_VALUES_EQUAL(waits, 2);
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), 9 << 8);
        UNIT_ASSERT_VALUES_EQUAL(waits, 2);
    }

    Y_UNIT_TEST(MissingChildRetiresPidWithoutSignaling)
    {
        int kills = 0;
        int waits = 0;
        TChildProcessState process(
            [&] (int, int) { ++kills; return 0; },
            [&] (int, int* status, int) {
                ++waits;
                *status = 0;
                errno = ECHILD;
                return -1;
            });
        process.SetPid(42);
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), -1);
        process.Kill();
        UNIT_ASSERT(!process.IsAlive());
        UNIT_ASSERT_VALUES_EQUAL(kills, 0);
        UNIT_ASSERT_VALUES_EQUAL(waits, 1);
    }

    Y_UNIT_TEST(TimeoutKillsAndRetriesInterruptedBlockingWait)
    {
        int kills = 0;
        int waits = 0;
        TChildProcessState process(
            [&] (int pid, int signal) {
                UNIT_ASSERT_VALUES_EQUAL(pid, 42);
                UNIT_ASSERT_VALUES_EQUAL(signal, SIGKILL);
                ++kills;
                return 0;
            },
            [&] (int pid, int* status, int options) {
                *status = 0;
                ++waits;
                if (options == WNOHANG) {
                    return 0;
                }
                if (waits == 2) {
                    errno = EINTR;
                    return -1;
                }
                *status = SIGKILL;
                return pid;
            });
        process.SetPid(42);
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), SIGKILL);
        UNIT_ASSERT_VALUES_EQUAL(kills, 1);
        UNIT_ASSERT_VALUES_EQUAL(waits, 3);
        process.Kill();
        UNIT_ASSERT_VALUES_EQUAL(kills, 1);
    }

    Y_UNIT_TEST(InterruptedPollingHonorsDeadline)
    {
        int kills = 0;
        int polls = 0;
        TChildProcessState process(
            [&] (int, int) { ++kills; return 0; },
            [&] (int pid, int* status, int options) {
                *status = 11 << 8;
                if (options == WNOHANG && ++polls <= 4) {
                    errno = EINTR;
                    return -1;
                }
                return pid;
            });
        process.SetPid(42);
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::MilliSeconds(25)), 11 << 8);
        UNIT_ASSERT_VALUES_EQUAL(kills, 1);
        UNIT_ASSERT(polls <= 4);
    }

    Y_UNIT_TEST(UnexpectedWaitErrorPreservesOwnershipAndUnknownStatus)
    {
        int kills = 0;
        int waits = 0;
        TChildProcessState process(
            [&] (int, int) { ++kills; return 0; },
            [&] (int pid, int* status, int) {
                *status = 0;
                if (++waits <= 2) {
                    errno = EINVAL;
                    return -1;
                }
                *status = 12 << 8;
                return pid;
            });
        process.SetPid(42);
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), -1);
        UNIT_ASSERT(process.IsAlive());
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), 12 << 8);
        UNIT_ASSERT_VALUES_EQUAL(kills, 0);
        UNIT_ASSERT_VALUES_EQUAL(waits, 3);
    }

    Y_UNIT_TEST(KillFailureDoesNotEnterBlockingWait)
    {
        int waits = 0;
        TChildProcessState process(
            [] (int, int) { errno = EPERM; return -1; },
            [&] (int, int* status, int options) {
                ++waits;
                *status = 0;
                UNIT_ASSERT_VALUES_EQUAL(options, WNOHANG);
                return 0;
            });
        process.SetPid(42);
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), -1);
        UNIT_ASSERT(process.IsAlive());
        UNIT_ASSERT_VALUES_EQUAL(waits, 2);
    }

    Y_UNIT_TEST(PollingWaitAllowsConcurrentKill)
    {
        TManualEvent polled;
        std::atomic<bool> killed = false;
        int kills = 0;
        int status = -1;
        TChildProcessState process(
            [&] (int, int) { ++kills; killed = true; return 0; },
            [&] (int pid, int* childStatus, int) {
                if (killed) {
                    *childStatus = SIGKILL;
                    return pid;
                }
                polled.Signal();
                return 0;
            });
        process.SetPid(42);
        TThread waiter([&] () { status = process.Wait(TDuration::Seconds(3)); });
        waiter.Start();
        const bool entered = polled.WaitT(TDuration::Seconds(1));
        const auto started = TInstant::Now();
        process.Kill();
        const auto killDuration = TInstant::Now() - started;
        waiter.Join();
        UNIT_ASSERT(entered);
        UNIT_ASSERT(killDuration < TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(status, SIGKILL);
        process.Kill();
        UNIT_ASSERT_VALUES_EQUAL(kills, 1);
    }

    Y_UNIT_TEST(NativeWaitPreservesExitStatus)
    {
        const int pid = fork();
        if (pid == 0) {
            _exit(23);
        }
        UNIT_ASSERT(pid > 0);
        TChildProcessState process;
        process.SetPid(pid);
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(), 23 << 8);
        UNIT_ASSERT_VALUES_EQUAL(process.Wait(TDuration::Zero()), 23 << 8);
        UNIT_ASSERT(!process.IsAlive());
        process.Kill();
    }
}

} // namespace NYql::NTaskRunnerProxy::NPrivate
