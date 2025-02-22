#include <yt/yql/tools/ytrun/lib/ytrun_lib.h>

int main(int argc, const char *argv[]) {
    try {
        return NYql::TYtRunTool().Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
