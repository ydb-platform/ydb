#include "pgwire.h"

int some_foo();

int main(int argc, char **argv) {
    some_foo()
    try {
        return NPGW::TPgWire(argc, argv).Run();
    } catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    }
}
