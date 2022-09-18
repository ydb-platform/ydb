#include "interactive.h"

namespace NYdb {
namespace NConsoleClient {


bool AskYesOrNo() {
    TString input;
    for (;;) {
        Cin >> input;
        if (to_lower(input) == "y" || to_lower(input) == "yes") {
            return true;
        } else if (to_lower(input) == "n" || to_lower(input) == "n") {
            return false;
        } else {
            Cout << "Type \"y\" (yes) or \"n\" (no): ";
        }
    }
    return false;
}

}
}
