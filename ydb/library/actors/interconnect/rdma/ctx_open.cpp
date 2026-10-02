#include <contrib/libs/ibdrv/symbols.h>

// symbols.h and verbs.h can't be used in one cpp file
namespace NInterconnect::NRdma {
    void IbvDlOpen() {
        IBSym();
    }
}
