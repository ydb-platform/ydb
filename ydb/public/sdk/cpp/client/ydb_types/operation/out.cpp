#include "operation.h"

Y_DECLARE_OUT_SPEC(, NYdb::TOperation, o, x) {
    return x.Out(o);
}
