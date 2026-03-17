#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
#include "mont.h"

typedef struct _WorkplaceCurve448 {
    uint64_t *a, *b;
    uint64_t *scratch;
} WorkplaceCurve448;

typedef struct _Curve448Context {
    MontContext *mont_ctx;
    uint64_t *a24;              /* encoded in Montgomery form */
} Curve448Context;

typedef struct Curve448Point {
    Curve448Context *ec_ctx;
    WorkplaceCurve448 *wp;
    uint64_t *x;
    uint64_t *z;
} Curve448Point;

int curve448_new_context(Curve448Context **pec_ctx);
void curve448_free_context(Curve448Context *ec_ctx);
int curve448_new_point(Curve448Point **out,
                                  const uint8_t *x,
                                  size_t len,
                                  const Curve448Context *ec_ctx);
void curve448_free_point(Curve448Point *p);
int curve448_clone(Curve448Point **P, const Curve448Point *Q);
int curve448_get_x(uint8_t *xb, size_t modsize, const Curve448Point *p);
int curve448_scalar(Curve448Point *P, const uint8_t *scalar, size_t scalar_len, uint64_t seed);
int curve448_cmp(const Curve448Point *ecp1, const Curve448Point *ecp2);
}

BEGIN_SYMS("Crypto.PublicKey._curve448")
SYM(curve448_new_context)
SYM(curve448_free_context)
SYM(curve448_new_point)
SYM(curve448_free_point)
SYM(curve448_clone)
SYM(curve448_get_x)
SYM(curve448_scalar)
SYM(curve448_cmp)
END_SYMS()
