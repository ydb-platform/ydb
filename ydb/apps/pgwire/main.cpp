#include "pgwire.h"

void foo_bar();

int main(int argc, char **argv) {
    foo_bar();
    try {
        return NPGW::TPgWire(argc, argv).Run();
    } catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    }
}
