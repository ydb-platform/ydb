#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
typedef void EcContext;
typedef void PointEd448;
int ed448_new_context(EcContext **pec_ctx);
void ed448_free_context(EcContext *ec_ctx);
int ed448_new_point(PointEd448 **out,
                    const uint8_t x[56],
                    const uint8_t y[56],
                    size_t len,
                    const EcContext *context);
int ed448_clone(PointEd448 **P, const PointEd448 *Q);
void ed448_free_point(PointEd448 *p);
int ed448_cmp(const PointEd448 *p1, const PointEd448 *p2);
int ed448_neg(PointEd448 *p);
int ed448_get_xy(uint8_t *xb, uint8_t *yb, size_t len, const PointEd448 *p);
int ed448_double(PointEd448 *p);
int ed448_add(PointEd448 *P1, const PointEd448 *P2);
int ed448_scalar(PointEd448 *P, const uint8_t *scalar, size_t scalar_len, uint64_t seed);
}

BEGIN_SYMS("Crypto.PublicKey._ed448")
SYM(ed448_new_context)
SYM(ed448_free_context)
SYM(ed448_new_point)
SYM(ed448_clone)
SYM(ed448_free_point)
SYM(ed448_cmp)
SYM(ed448_neg)
SYM(ed448_get_xy)
SYM(ed448_double)
SYM(ed448_add)
SYM(ed448_scalar)
END_SYMS()
