#include <yt/yql/providers/yt/job/yql_job_registry.h>

#include <yql/essentials/minikql/aligned_page_pool.h>
#include <yql/essentials/utils/backtrace/backtrace.h>

#include <yt/cpp/mapreduce/client/init.h>

#include <util/system/env.h>
#include <util/system/mlock.h>
#include <util/system/yassert.h>

int main(int argc, const char *argv[]) {
    Y_UNUSED(NKikimr::NUdf::GetStaticSymbols());
    try {
        LockAllMemory(LockCurrentMemory | LockFutureMemory);
    } catch (yexception&) {
        Cerr << "mlockall failed, but that's fine" << Endl;
    }

    if (GetEnv("YQL_USE_DEFAULT_ARROW_ALLOCATOR") == "1") {
        NKikimr::UseDefaultArrowAllocator();
    }

    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        NYT::Initialize(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage();
        return -1;
    }

    Y_ABORT("This binary should not be called directly");
}
