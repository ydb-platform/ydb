#include "init.h"

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/src/core/lib/iomgr/exec_ctx.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/logger/null.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NGrpc {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TShutdownTest)
{
    Y_UNIT_TEST(ShouldShutdownInAnotherThread)
    {
        grpc_init();
        // Tracing is mandatory: grpc_shutdown_internal runs in a dedicated
        // thread (due to an active ExecCtx) and invokes the custom logger,
        // triggering the shutdown/teardown race covered by this test.
        // If GrpcLoggerInit() does not register
        // grpc_maybe_wait_for_async_shutdown() in AtExit, TSAN reports the
        // race.
        const bool enableTracing = true;
        GrpcLoggerInit(TLog(MakeHolder<TNullLogBackend>()), enableTracing);

        // With an active ExecCtx grpc_shutdown() must not clean up inline.
        // Instead it spawns a dedicated shutdown thread (running
        // grpc_shutdown_internal), and the final wait is performed later
        // from AtExit.
        grpc_core::ExecCtx ctx;
        grpc_shutdown();

        // Intentionally no explicit wait here:
        // this test verifies that async shutdown is properly drained from
        // AtExit.
    }
}

}   // namespace NYdb::NBS::NGrpc
