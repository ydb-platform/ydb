/*
*    An Adaptive Hash Table
*    Sumer Cip 2012
*/

#ifndef YHASHTAB_H
#define YHASHTAB_H

#include "config.h"

#define HSIZE(n) (1<<n)
#define HMASK(n) (HSIZE(n)-1)
#define HLOADFACTOR 0.75

struct _hitem {
    uintptr_t key;
    uintptr_t val;
    int free; // for recycling.
    struct _hitem *next;
};
typedef struct _hitem _hitem;

typedef struct {
    int realsize;
    int logsize;
    int count;
    int mask;
    int freecount;
    _hitem ** _table;
} _htab;

_htab *htcreate(int logsize);
void htdestroy(_htab *ht);
_hitem *hfind(_htab *ht, uintptr_t key);
int hadd(_htab *ht, uintptr_t key, uintptr_t val);
void henum(_htab *ht, int (*fn) (_hitem *item, void *arg), void *arg);
int hcount(_htab *ht);
void hfree(_htab *ht, _hitem *item);

#endif
