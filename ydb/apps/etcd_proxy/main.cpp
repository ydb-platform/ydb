#include "proxy.h"

int main(int argc, char **argv) try {
    return NEtcd::TProxy(argc, argv).Run();
} catch (const yexception& e) {
    Cerr << "Caught exception: " << e.what() << Endl;
    return 1;
}
