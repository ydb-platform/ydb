#include "mem.h"

static size_t memused = 0;

#ifdef DEBUG_MEM

static dnode_t *dhead;
static unsigned int dsize;

void YMEMLEAKCHECK(void)
{
    dnode_t *v;
    unsigned int tleak;

    v = dhead;
    tleak = 0;
    while(v) {
        fprintf(stderr, "[YMEM]    Leaked block: (addr:%p) (size:%d)\n", v->ptr, v->size);
        tleak += v->size;
        v = v->next;
    }
    if (tleak == 0)
        fprintf(stderr, "[YMEM]    Application currently has no leakage.[%d]\n", dsize);
    else
        fprintf(stderr, "[YMEM]    Application currently leaking %d bytes.[%d]\n", tleak, dsize);
}
#else
void YMEMLEAKCHECK(void)
{
    ;
}
#endif


size_t
ymemusage(void)
{
    return memused;
}

void *
ymalloc(size_t size)
{
    void *p;
#ifdef DEBUG_MEM
    dnode_t *v;
#endif

    p = PyMem_Malloc(size+sizeof(size_t));
    if (!p) {
        yerr("malloc(%u) failed. No memory?", (unsigned int)size);
        return NULL;
    }
    memused += size;
    *(size_t *)p = size;
#ifdef DEBUG_MEM
    if (dhead)
        yinfo("_ymalloc(%u) called[%p].[old_head:%p]", (unsigned int)size, p, dhead->ptr);
    else
        yinfo("_ymalloc(%u) called[%p].[old_head:nil]", (unsigned int)size, p);
    v = PyMem_Malloc(sizeof(dnode_t));
    v->ptr = p;
    v->size = size;
    v->next = dhead;
    dhead = v;
    dsize++;
#endif
    return (char *)p+sizeof(size_t);
}

void
yfree(void *p)
{
#ifdef DEBUG_MEM
    dnode_t *v;
    dnode_t *prev;
#endif
    p = (char *)p - sizeof(size_t);
    memused -= *(size_t *)p;
#ifdef DEBUG_MEM
    v = dhead;
    prev = NULL;
    while(v) {
        if (v->ptr == p) {
            if (prev)
                prev->next = v->next;
            else
                dhead = v->next;

            yinfo("_yfree(%p) called.", p);
            PyMem_Free(v);
            dsize--;
            break;
        }
        prev = v;
        v = v->next;
    }
#endif
    PyMem_Free(p);
}
