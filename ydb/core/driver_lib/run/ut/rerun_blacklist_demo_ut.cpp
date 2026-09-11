#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>

#include <cstdlib>

// DEMO ONLY, must not be merged (see ydb-platform/ydb#52773).
//
// ya marks a whole suite in its last-failed cache (and -X then restarts it without per-test
// filters) only when a chunk-event carries a crashed/fail status. A crash inside a test body
// does not do that: run_ut attributes it to the test and relaunches the rest of the chunk.
// The chunk itself is marked CRASHED when the binary dies before the first test, so crash on
// startup, but only in the process that was asked to run this suite, i.e. only in this
// suite's chunk. The muted test on this path lives in another chunk and is unaffected, so the
// path stays in the try_2 blacklist and, before the fix, the whole suite disappears from try_2.
namespace {

struct TCrashChunkOnStartup {
    TCrashChunkOnStartup() {
        try {
            if (TFileInput("/proc/self/cmdline").ReadAll().Contains("+RerunBlacklistDemo::")) {
                abort();
            }
        } catch (...) {
        }
    }
} CrashChunkOnStartup;

} // namespace

Y_UNIT_TEST_SUITE(RerunBlacklistDemo) {
    Y_UNIT_TEST(Crashes) {
    }
}
