#include <ydb/mvp/meta/mvp.h>

int main(int argc, char **argv) {
    try {
        return NMVP::TMVP(argc, argv).Run();
    } catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    }
}
