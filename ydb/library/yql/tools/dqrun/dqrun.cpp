#include <ydb/library/yql/tools/dqrun/lib/dqrun_lib.h>
#include <util/generic/yexception.h>

int main(int argc, const char *argv[]) {
    try {
        return NYql::TDqRunTool().Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
