#include "flat_fwd_misc.h"

Y_DECLARE_OUT_SPEC(, NKikimr::NTable::NFwd::TStat, stream, value) {
    value.Describe(stream);
}
