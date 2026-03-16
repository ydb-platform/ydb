#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
typedef struct Point {
    uint32_t X[10];
    uint32_t Z[10];
} Point;

int curve25519_new_point(Point **out,
                                    const uint8_t x[32],
                                    size_t modsize,
                                    const void *context);
int curve25519_clone(Point **P, const Point *Q);
void curve25519_free_point(Point *p);
int curve25519_get_x(uint8_t *xb, size_t modsize, const Point *p);
int curve25519_scalar(Point *P, const uint8_t *scalar, size_t scalar_len, uint64_t seed);
int curve25519_cmp(const Point *ecp1, const Point *ecp2);
}

BEGIN_SYMS("Crypto.PublicKey._curve25519")
SYM(curve25519_new_point)
SYM(curve25519_clone)
SYM(curve25519_free_point)
SYM(curve25519_get_x)
SYM(curve25519_scalar)
SYM(curve25519_cmp)
END_SYMS()
