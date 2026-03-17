#ifndef ___EDITDISTANCE__H__
#define ___EDITDISTANCE__H__

#include "./def.h"

#ifdef __cplusplus
extern "C" {
#endif

unsigned int edit_distance(const int64_t *a, const unsigned int asize, const int64_t *b, const unsigned int bsize);
bool edit_distance_criterion(const int64_t *a, const unsigned int asize, const int64_t *b, const unsigned int bsize, const unsigned int thr);
unsigned int edit_distance_dp(int64_t const *str1, size_t const size1, int64_t const *str2, size_t const size2);

#ifdef __cplusplus
}
#endif

#endif
