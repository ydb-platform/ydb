#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
typedef void Point;
int ed25519_new_point(Point **out,
                      const uint8_t x[32],
                      const uint8_t y[32],
                      size_t modsize,
                      const void *context);
int ed25519_clone(Point **P, const Point *Q);
void ed25519_free_point(Point *p);
int ed25519_cmp(const Point *p1, const Point *p2);
int ed25519_neg(Point *p);
int ed25519_get_xy(uint8_t *xb, uint8_t *yb, size_t modsize, Point *p);
int ed25519_double(Point *p);
int ed25519_add(Point *P1, const Point *P2);
int ed25519_scalar(Point *P, uint8_t *scalar, size_t scalar_len, uint64_t seed);
}

BEGIN_SYMS("Crypto.PublicKey._ed25519")
SYM(ed25519_new_point)
SYM(ed25519_clone)
SYM(ed25519_free_point)
SYM(ed25519_cmp)
SYM(ed25519_neg)
SYM(ed25519_get_xy)
SYM(ed25519_double)
SYM(ed25519_add)
SYM(ed25519_scalar)
END_SYMS()
