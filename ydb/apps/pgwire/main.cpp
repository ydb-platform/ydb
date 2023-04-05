#include "pgwire.h"

int main(int argc, char **argv) {
    try {
        return NPGW::TPgWire(argc, argv).Run();
    } catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    }
}
