#include "defs.h"

Y_DECLARE_OUT_SPEC(, NKikimr::NTable::TTxStamp, stream, value) {
    stream << value.Gen() << ":" << value.Step();
}
