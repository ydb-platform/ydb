#ifndef YFREELIST_H
#define YFREELIST_H

typedef struct {
    int head;
    int size;
    int chunksize;
    void **items;
} _freelist;

_freelist * flcreate(int chunksize, int size);
void fldestroy(_freelist *flp);
void *flget(_freelist *flp);
int flput(_freelist *flp, void *p);
unsigned int flcount(_freelist *flp);
void fldisp(_freelist *flp);

#endif



