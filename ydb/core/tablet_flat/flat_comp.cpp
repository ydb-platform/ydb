#include "flat_comp.h"

namespace NKikimr {
namespace NTable {

void TCompactionParams::Describe(IOutputStream& out) const {
    out << "TCompactionParams{" << Table << ":";

    if (Edge.Head == TEpoch::Max()) {
        out << " epoch +inf";
    } else {
        out << " epoch " << Edge.Head;
    }

    out << ", " << Parts.size() << " parts}";
}

}
}
