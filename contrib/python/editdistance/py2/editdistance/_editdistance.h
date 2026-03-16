#ifndef ___EDITDISTANCE__H__
#define ___EDITDISTANCE__H__

#include "./def.h"

#ifdef __cplusplus
extern "C" {
#endif

unsigned int edit_distance(const int64_t *a, const unsigned int asize, const int64_t *b, const unsigned int bsize);

#ifdef __cplusplus
}
#endif

#endif
