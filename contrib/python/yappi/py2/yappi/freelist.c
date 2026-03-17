#include "freelist.h"
#include "mem.h"

static int
_flgrow(_freelist *flp)
{
    int i, newsize;
    void **old;

    old = flp->items;
    newsize = flp->size * 2;
    flp->items = ymalloc(newsize * sizeof(void *));
    if (!flp->items)
        return 0;

    // init new list
    for(i=0; i<flp->size; i++) {
        flp->items[i] = ymalloc(flp->chunksize);
        if (!flp->items[i]) {
            yfree(flp->items);
            return 0;
        }
    }
    // copy old list
    for(i=flp->size; i<newsize; i++)
        flp->items[i] = old[i-flp->size];

    yfree(old);
    flp->head = flp->size-1;
    flp->size = newsize;
    return 1;
}

_freelist *
flcreate(int chunksize, int size)
{
    int i;
    _freelist *flp;

    flp = (_freelist *)ymalloc(sizeof(_freelist));
    if (!flp)
        return NULL;
    flp->items = ymalloc(size * sizeof(void *));
    if (!flp->items) {
        yfree(flp);
        return NULL;
    }

    for (i=0; i<size; i++) {
        flp->items[i] = ymalloc(chunksize);
        if (!flp->items[i]) {
            yfree(flp->items);
            yfree(flp);
            return NULL;
        }
    }
    flp->size = size;
    flp->chunksize = chunksize;
    flp->head = size-1;
    return flp;
}

void
fldestroy(_freelist *flp)
{
    int i;

    for (i=0; i<flp->size; i++) {
        yfree(flp->items[i]);
    }
    yfree(flp->items);
    yfree(flp);
}

void *
flget(_freelist *flp)
{
    if (flp->head < 0) {
        if (!_flgrow(flp))
            return NULL;
    }
    return flp->items[flp->head--];
}

int
flput(_freelist *flp, void *p)
{
    if (flp->head > flp->size-2)
        return 0;

    flp->items[++flp->head] = p;
    return 1;
}

unsigned int
flcount(_freelist *flp)
{
    return flp->size - flp->head + 1;
}
