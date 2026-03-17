

#include "proj_internal.h"

PROJ_HEAD(noop, "No operation");

static void noop(PJ_COORD &, PJ *) {}

PJ *PJ_CONVERSION(noop, 0) {
    P->fwd4d = noop;
    P->inv4d = noop;
    P->left = PJ_IO_UNITS_WHATEVER;
    P->right = PJ_IO_UNITS_WHATEVER;
    return P;
}
