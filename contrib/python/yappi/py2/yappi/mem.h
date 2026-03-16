#ifndef YMEM_H
#define YMEM_H

#include "config.h"
#include "debug.h"

struct dnode {
    void *ptr;
    unsigned int size;
    struct dnode *next;
};
typedef struct dnode dnode_t;

void *ymalloc(size_t size);
void yfree(void *p);
size_t ymemusage(void);
void YMEMLEAKCHECK(void);

#endif

