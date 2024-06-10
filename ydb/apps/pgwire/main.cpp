#include "pgwire.h"

int foobar();

int main(int argc, char **argv) {
    int val = foobar();
    (void)val;
    try {
        return NPGW::TPgWire(argc, argv).Run();
    } catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    }
}
