#include <yql/essentials/tools/minirun/lib/minirun_lib.h>

int main(int argc, const char** argv) {
    try {
        return NYql::TMiniRunTool().Main(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
