#include "callstack.h"
#include "debug.h"
#include "mem.h"
#include "timing.h"
#include "config.h"

int
slen(_cstack *cs)
{
    return 1 + cs->head;
}

_cstack *
screate(int size)
{
    int i;
    _cstack *cs;

    cs = (_cstack *)ymalloc(sizeof(_cstack));
    if (!cs)
        return NULL;
    cs->_items = ymalloc(size * sizeof(_cstackitem));
    if (cs->_items == NULL) {
        yfree(cs);
        return NULL;
    }

    for(i=0; i<size; i++) {
        cs->_items[i].ckey = 0;
        cs->_items[i].t0 = 0;
    }

    cs->size = size;
    cs->head = -1;
    return cs;
}

static int
_sgrow(_cstack * cs)
{
    int i;
    _cstack *dummy;

    dummy = screate(cs->size*2);
    if(!dummy)
        return 0;

    for(i=0; i<cs->size; i++) {
        dummy->_items[i].ckey = cs->_items[i].ckey;
        dummy->_items[i].t0 = cs->_items[i].t0;
    }
    yfree(cs->_items);
    cs->_items = dummy->_items;
    cs->size = dummy->size;
    yfree(dummy);
    return 1;
}

void
sdestroy(_cstack * cs)
{
    yfree(cs->_items);
    yfree(cs);
}


_cstackitem *
spush(_cstack *cs, void *ckey)
{
    _cstackitem *ci;

    if (cs->head >= cs->size-1) {
        if (!_sgrow(cs))
            return NULL;
    }

    ci = &cs->_items[++cs->head];
    ci->ckey = ckey;

    return ci;
}

_cstackitem *
spop(_cstack * cs)
{
    _cstackitem *ci;

    if (cs->head < 0)
        return NULL;

    ci = &cs->_items[cs->head--];
    return ci;
}

_cstackitem *
shead(_cstack * cs)
{
    if (cs->head < 0)
        return NULL;

    return &cs->_items[cs->head];
}
