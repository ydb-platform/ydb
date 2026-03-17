
/* Grammar subroutines needed by parser */

#include "Python.h"
#include "../Include/pgenheaders.h"
#include "../Include/grammar.h"
#include "../Include/token.h"

/* Return the DFA for the given type */

dfa *
Ta27Grammar_FindDFA(grammar *g, register int type)
{
    register dfa *d;
#if 1
    /* Massive speed-up */
    d = &g->g_dfa[type - NT_OFFSET];
    assert(d->d_type == type);
    return d;
#else
    /* Old, slow version */
    register int i;

    for (i = g->g_ndfas, d = g->g_dfa; --i >= 0; d++) {
        if (d->d_type == type)
            return d;
    }
    assert(0);
    /* NOTREACHED */
#endif
}

char *
Ta27Grammar_LabelRepr(label *lb)
{
    static char buf[100];

    if (lb->lb_type == ENDMARKER)
        return "EMPTY";
    else if (ISNONTERMINAL(lb->lb_type)) {
        if (lb->lb_str == NULL) {
            PyOS_snprintf(buf, sizeof(buf), "NT%d", lb->lb_type);
            return buf;
        }
        else
            return lb->lb_str;
    }
    else {
        if (lb->lb_str == NULL)
            return _Ta27Parser_TokenNames[lb->lb_type];
        else {
            PyOS_snprintf(buf, sizeof(buf), "%.32s(%.32s)",
                _Ta27Parser_TokenNames[lb->lb_type], lb->lb_str);
            return buf;
        }
    }
}
