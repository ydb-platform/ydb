#include "value.h"

Y_DECLARE_OUT_SPEC(, NYdb::TType, o, x) {
    return x.Out(o);
}
