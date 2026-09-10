#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe_process.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cerrno>

namespace NYql::NTaskRunnerProxy::NPrivate {
namespace {

Y_UNIT_TEST_SUITE(TPipeChildProcessTest) {

#ifndef _win_
    Y_UNIT_TEST(InvalidPidDoesNotReachSyscalls)
    {
        int pid = -1;
        int killCallCount = 0;
        int waitPidCallCount = 0;

        UNIT_ASSERT(CleanupChildProcess(
            &pid,
            TDuration::Zero(),
            [&] (int, int) {
                ++killCallCount;
                return 0;
            },
            [&] (int, int*, int) {
                ++waitPidCallCount;
                return 0;
            }));
        UNIT_ASSERT_VALUES_EQUAL(killCallCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(waitPidCallCount, 0);
    }

    Y_UNIT_TEST(InterruptedWaitIsRetried)
    {
        int pid = 42;
        int waitPidCallCount = 0;

        UNIT_ASSERT(CleanupChildProcess(
            &pid,
            TDuration::Seconds(1),
            [] (int, int) {
                return 0;
            },
            [&] (int childPid, int*, int) {
                if (++waitPidCallCount == 1) {
                    errno = EINTR;
                    return -1;
                }
                return childPid;
            }));
        UNIT_ASSERT_VALUES_EQUAL(waitPidCallCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(pid, -1);
    }

    Y_UNIT_TEST(InterruptedWaitRespectsDeadline)
    {
        int pid = 42;
        int waitPidCallCount = 0;
        UNIT_ASSERT(!CleanupChildProcess(
            &pid,
            TDuration::Zero(),
            [] (int, int) { return 0; },
            [&] (int childPid, int*, int) {
                if (++waitPidCallCount <= 3) {
                    errno = EINTR;
                    return -1;
                }
                return childPid;
            }));
        UNIT_ASSERT_VALUES_EQUAL(waitPidCallCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(pid, 42);
    }

    Y_UNIT_TEST(TimeoutDoesNotConfirmCleanup)
    {
        int pid = 42;

        UNIT_ASSERT(!CleanupChildProcess(
            &pid,
            TDuration::Zero(),
            [] (int, int) {
                return 0;
            },
            [] (int, int*, int) {
                return 0;
            }));
        UNIT_ASSERT_VALUES_EQUAL(pid, 42);
    }
#endif

    Y_UNIT_TEST(ShellCommandRequiresSuccessfulExit)
    {
        UNIT_ASSERT(IsShellCommandSuccessful(TShellCommand::SHELL_FINISHED, 0));
        UNIT_ASSERT(!IsShellCommandSuccessful(TShellCommand::SHELL_ERROR, 1));
        UNIT_ASSERT(!IsShellCommandSuccessful(TShellCommand::SHELL_FINISHED, Nothing()));
    }
}

} // namespace
} // namespace NYql::NTaskRunnerProxy::NPrivate
