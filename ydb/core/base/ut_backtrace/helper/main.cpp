#include <ydb/core/base/backtrace.h>

#include <util/system/yassert.h>

int main() {
    NKikimr::EnableYDBBacktraceFormat();
    Y_ABORT("intentional abort for backtrace build-info test");
    return 0;
}
