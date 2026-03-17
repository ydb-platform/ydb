#ifndef _ED25519_H
#define _ED25519_H

typedef struct Point {
    uint32_t X[10];
    uint32_t Y[10];
    uint32_t Z[10];
    uint32_t T[10];
} Point;

EXPORT_SYM int ed25519_new_point(Point **out,
                      const uint8_t x[32], const uint8_t y[32],
                      size_t modsize, void *context);
EXPORT_SYM int ed25519_clone(Point **P, const Point *Q);
EXPORT_SYM void ed25519_free_point(Point *p);
EXPORT_SYM int ed25519_cmp(const Point *p1, const Point *p2);
EXPORT_SYM int ed25519_neg(Point *p);
EXPORT_SYM int ed25519_get_xy(uint8_t *xb, uint8_t *yb, size_t modsize, Point *p);
EXPORT_SYM int ed25519_double(Point *p);
EXPORT_SYM int ed25519_add(Point *P1, const Point *P2);
EXPORT_SYM int ed25519_scalar(Point *P, const uint8_t *scalar, size_t scalar_len, uint64_t seed);

#endif
