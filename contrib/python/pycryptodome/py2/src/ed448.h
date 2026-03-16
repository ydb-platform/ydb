#ifndef __ED448_H
#define __ED448_H

#include "common.h"
#include "mont.h"

typedef struct _WorkplaceEd448 {
    uint64_t *a, *b, *c, *d, *e, *f;
    uint64_t *scratch;
} WorkplaceEd448;

typedef struct _EcContext {
    MontContext *mont_ctx;
    uint64_t *d;                    /* encoded in Montgomery form */
} EcContext;

/*
 * An Ed448 point in Jacobian coordinates
 */
typedef struct _PointEd448 {
    const EcContext *ec_ctx;
    WorkplaceEd448 *wp;
    uint64_t *x;
    uint64_t *y;
    uint64_t *z;
} PointEd448;

EXPORT_SYM int ed448_new_context(EcContext **pec_ctx);
EXPORT_SYM void ed448_free_context(EcContext *ec_ctx);
EXPORT_SYM int ed448_new_point(PointEd448 **pecp,
                               const uint8_t *x,
                               const uint8_t *y,
                               size_t len,
                               const EcContext *ec_ctx);
EXPORT_SYM int ed448_clone(PointEd448 **pecp2, const PointEd448 *ecp);
EXPORT_SYM int ed448_copy(PointEd448 *ecp1, const PointEd448 *ecp2);
EXPORT_SYM void ed448_free_point(PointEd448 *ecp);
EXPORT_SYM int ed448_get_xy(uint8_t *x, uint8_t *y, size_t len, const PointEd448 *ecp);
EXPORT_SYM int ed448_add(PointEd448 *ecpa, const PointEd448 *ecpb);
EXPORT_SYM int ed448_double(PointEd448 *P);
EXPORT_SYM int ed448_scalar(PointEd448 *P, const uint8_t *scalar, size_t scalar_len, uint64_t);
EXPORT_SYM int ed448_cmp(const PointEd448 *ecp1, const PointEd448 *ecp2);
EXPORT_SYM int ed448_neg(PointEd448 *P);
#endif
