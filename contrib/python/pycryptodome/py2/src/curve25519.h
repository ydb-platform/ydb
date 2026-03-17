#ifndef _CURVE25519_H
#define _CURVE25519_H

typedef struct Point {
    uint32_t X[10];
    uint32_t Z[10];
} Point;

EXPORT_SYM int curve25519_new_point(Point **out,
                                    const uint8_t x[32],
                                    size_t modsize,
                                    const void *context);
EXPORT_SYM int curve25519_clone(Point **P, const Point *Q);
EXPORT_SYM void curve25519_free_point(Point *p);
EXPORT_SYM int curve25519_get_x(uint8_t *xb, size_t modsize, const Point *p);
EXPORT_SYM int curve25519_scalar(Point *P, const uint8_t *scalar, size_t scalar_len, uint64_t seed);
EXPORT_SYM int curve25519_cmp(const Point *ecp1, const Point *ecp2);

#endif
