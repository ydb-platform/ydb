#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
typedef void EcContext;
typedef void EcPoint;
int ec_ws_new_context(EcContext **pec_ctx,
                      const uint8_t *modulus,
                      const uint8_t *b,
                      const uint8_t *order,
                      size_t len,
                      uint64_t seed);
void ec_ws_free_context(EcContext *ec_ctx);
int ec_ws_new_point(EcPoint **pecp,
                    const uint8_t *x,
                    const uint8_t *y,
                    size_t len,
                    const EcContext *ec_ctx);
void ec_ws_free_point(EcPoint *ecp);
int ec_ws_get_xy(uint8_t *x,
                 uint8_t *y,
                 size_t len,
                 const EcPoint *ecp);
int ec_ws_double(EcPoint *p);
int ec_ws_add(EcPoint *ecpa, EcPoint *ecpb);
int ec_ws_scalar(EcPoint *ecp,
                 const uint8_t *k,
                 size_t len,
                 uint64_t seed);
int ec_ws_clone(EcPoint **pecp2, const EcPoint *ecp);
int ec_ws_copy(EcPoint *ecp1, const EcPoint *ecp2);
int ec_ws_cmp(const EcPoint *ecp1, const EcPoint *ecp2);
int ec_ws_neg(EcPoint *p);
}

BEGIN_SYMS("Crypto.PublicKey._ec_ws")
SYM(ec_ws_new_context)
SYM(ec_ws_free_context)
SYM(ec_ws_new_point)
SYM(ec_ws_free_point)
SYM(ec_ws_get_xy)
SYM(ec_ws_double)
SYM(ec_ws_add)
SYM(ec_ws_scalar)
SYM(ec_ws_clone)
SYM(ec_ws_copy)
SYM(ec_ws_cmp)
SYM(ec_ws_neg)
END_SYMS()
