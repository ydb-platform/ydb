#include "pgwire.h"

void unknown_foo();

int main(int argc, char **argv) {
    unknown_foo();
    try {
        return NPGW::TPgWire(argc, argv).Run();
    } catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    }
}
