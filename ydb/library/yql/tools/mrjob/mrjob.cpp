#include <ydb/library/yql/providers/yt/job/yql_job_registry.h>

#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <yt/cpp/mapreduce/client/init.h>

#include <util/system/yassert.h>
#include <util/system/mlock.h>

int main(int argc, const char *argv[]) {
    Y_UNUSED(NKikimr::NUdf::GetStaticSymbols());
    try {
        LockAllMemory(LockCurrentMemory | LockFutureMemory);
    } catch (yexception&) {
        Cerr << "mlockall failed, but that's fine" << Endl;
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
